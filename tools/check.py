#!/usr/bin/env python3
"""Hold every output to what the tape says it must contain.

The expected figures come from the tape alone (data/*.jsonl), folded here in
event time, one-minute windows aligned to the epoch as TUMBLE aligns them.
Every window that ends by minute 33 is checked: the tape's five minutes of
ordinary traffic after minute 30 carry the watermark past them. Each output is
read the way its consumer would read it:

  qoe.title_minute   the Iceberg table, through the REST catalog, with PyIceberg
                     (an analyst's view): one row per title and minute, each as
                     the tape says, none twice although a Worker was killed
  qoe.alerts         the alerts topic, read_committed, decoded through the
                     Schema Registry: exactly the failing title-minutes, once each
  gs://cdn-vendor    the CDN vendor's bucket, every object the exactly-once sink
                     published: one row per edge location and minute, none twice
  azure://finance    finance's container, every object the exactly-once sink
                     published: one row per session that started, none twice

Then the cluster itself, once: the API's token and the control plane's mutual
TLS; the ClinkJobs, and the evidence of the upgrade (--upgraded) and of the
failover (--failover); and the traces, metrics and lineage it exported. A job
named by --parks may already have parked for being idle, as it declares.

--scale-to-zero checks the scale-to-zero scene instead (scripts/park.sh): the
edges job parked once the half hour was over, held no slots and stayed parked
with the half hour's records on its topic, the CDN's later batch woke it
restored from the savepoint it parked at, and it parked again; and every
edge-location minute from 33 to 39, the window open at the park among them,
holds what the tape and the batch say, once.

--wait SECONDS retries the outputs until they all pass or the time is up, since
the last windows close and commit a few checkpoints after the feed ends.

Runs under the project's virtualenv (requirements.txt): PyIceberg and pyarrow.
"""
from __future__ import annotations

import argparse
import datetime
import io
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.fs as pafs
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import gen  # noqa: E402

MINUTE = gen.MINUTE
HORIZON = gen.T0 + 33 * MINUTE
COMPOSE = ["docker", "compose", "-f", str(ROOT / "cloud" / "compose.yml"),
           "--env-file", str(ROOT / "state" / "cloud.env")]
TITLE_COLS = ["starts", "plays", "failures", "rebuffers", "stall_ms", "startup_ms", "cellular_plays"]
POP_COLS = ["requests", "bytes", "server_errors", "ttfb_ms"]
LEDGER_COLS = ["title_id", "device", "app_version"]


def window(ts: int) -> int:
    return ts - ts % MINUTE


def checked(ws: int) -> bool:
    return ws + MINUTE <= HORIZON


def unwrap(v):
    return None if v is None else next(iter(v.values()))


def expected():
    title = defaultdict(lambda: dict.fromkeys(TITLE_COLS, 0))
    ledger = {}
    for name in ("playback-v1.jsonl", "playback-v2.jsonl"):
        with open(ROOT / "data" / name) as f:
            for line in f:
                e = json.loads(line)
                if e["event_type"] == "START":
                    # Every start is billed, whenever it happened: no window closes it.
                    ledger[(e["session_id"], e["ts"])] = {c: e[c] for c in LEDGER_COLS}
                ws = window(e["ts"])
                if not checked(ws):
                    continue
                r = title[(e["title_id"], ws)]
                kind = e["event_type"]
                r["starts"] += kind == "START"
                r["plays"] += kind == "FIRST_FRAME"
                r["failures"] += kind == "ERROR"
                r["rebuffers"] += kind == "REBUFFER"
                r["stall_ms"] += unwrap(e["stall_ms"]) or 0
                r["startup_ms"] += unwrap(e["startup_ms"]) or 0
                r["cellular_plays"] += kind == "FIRST_FRAME" and unwrap(e.get("network")) == "CELLULAR"
    alerts = {k: (r["starts"], r["failures"]) for k, r in title.items()
              if r["failures"] >= 5 and r["failures"] * 2 > r["starts"]}
    pops = defaultdict(lambda: dict.fromkeys(POP_COLS, 0))
    with open(ROOT / "data" / "edge.jsonl") as f:
        for line in f:
            e = json.loads(line)
            ws = window(e["ts"])
            if not checked(ws):
                continue
            r = pops[(e["pop"], ws)]
            r["requests"] += 1
            r["bytes"] += e["bytes"]
            r["server_errors"] += e["status"] >= 500
            r["ttfb_ms"] += e["ttfb_ms"]
    return dict(title), alerts, dict(pops), ledger


def read_iceberg() -> list[dict]:
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog("playhead", **{
        "type": "rest", "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:4566", "s3.region": "us-east-1",
        "s3.access-key-id": "test", "s3.secret-access-key": "test",
        "s3.path-style-access": "true"})
    return catalog.load_table("qoe.title_minute").scan().to_arrow().to_pylist()


def read_alerts() -> list[dict]:
    out = subprocess.run(COMPOSE + ["exec", "-T", "redpanda", "rpk", "-X", "brokers=redpanda:9092",
                                    "-X", "registry.hosts=localhost:8081", "topic", "consume",
                                    "qoe.alerts", "--read-committed", "--use-schema-registry=value",
                                    "-o", ":end", "-f", "%v\n"],
                         capture_output=True, text=True, check=True).stdout
    # rpk prints Avro's JSON encoding, which wraps a union's value in its
    # branch: {"long": 42}. clink derives the alerts schema with every field
    # nullable, so every value arrives wrapped.
    def plain(v):
        return next(iter(v.values())) if isinstance(v, dict) and len(v) == 1 else v
    return [{k: plain(v) for k, v in json.loads(line).items()} for line in out.splitlines() if line.strip()]


def read_gcs() -> list[dict]:
    base = "http://127.0.0.1:4443/storage/v1/b/cdn-vendor/o"
    with urllib.request.urlopen(base + "?prefix=" + urllib.parse.quote("pop-minute/"), timeout=10) as r:
        names = [o["name"] for o in json.load(r).get("items", [])]
    rows = []
    for name in sorted(names):
        if "/committed/" not in name or not name.endswith(".parquet"):
            continue
        with urllib.request.urlopen(f"{base}/{urllib.parse.quote(name, safe='')}?alt=media", timeout=30) as r:
            rows += pq.read_table(io.BytesIO(r.read())).to_pylist()
    return rows


def compare(name: str, want: dict, got: list[dict], key, cols,
            counted=lambda r: checked(r["window_start"]), noun: str = "windows") -> tuple[bool, str]:
    seen = Counter(key(r) for r in got if counted(r))
    repeated = sum(1 for n in seen.values() if n > 1)
    by_key = {key(r): r for r in got}
    missing = [k for k in want if k not in by_key]
    wrong = [k for k in want if k in by_key and any(by_key[k][c] != want[k][c] for c in cols)]
    extra = [k for k in seen if k not in want]
    ok = not (missing or wrong or extra or repeated)
    detail = (f"{len(seen)} {noun} for {len(want)} expected, {len(want) - len(missing) - len(wrong)} "
              f"as the tape says, {repeated} repeated, {len(extra)} unexpected")
    if missing[:3] or wrong[:3]:
        detail += f"; first missing {missing[:3]}, first wrong {wrong[:3]}"
    return ok, f"  {name}: {detail}  {'OK' if ok else 'FAIL'}"


def read_azure() -> list[dict]:
    env = dict(line.strip().split("=", 1) for line in open(ROOT / "state" / "cloud.env") if "=" in line)
    azure = pafs.AzureFileSystem(account_name="playhead", account_key=env["PLAYHEAD_AZURE_KEY"],
                                 blob_storage_authority="127.0.0.1:10000", blob_storage_scheme="http",
                                 dfs_storage_authority="127.0.0.1:10000", dfs_storage_scheme="http")
    found = azure.get_file_info(pafs.FileSelector("finance/billable-sessions/committed",
                                                  recursive=True, allow_not_found=True))
    rows = []
    for info in sorted(found, key=lambda i: i.path):
        if info.type == pafs.FileType.File and info.path.endswith(".parquet"):
            rows += pq.read_table(info.path, filesystem=azure).to_pylist()
    return rows


def check_once(want_title, want_alerts, want_pops, want_ledger) -> tuple[bool, list[str]]:
    lines, all_ok = [], True
    for name, read, fn in (("qoe.title_minute (Iceberg on S3)", read_iceberg, "title"),
                           ("qoe.alerts (Kafka, read_committed)", read_alerts, "alerts"),
                           ("gs://cdn-vendor/pop-minute (GCS, exactly once)", read_gcs, "pops"),
                           ("azure://finance/billable-sessions (Azure, exactly once)", read_azure, "ledger")):
        try:
            got = read()
        except Exception as e:  # an output not there yet is a failed check, retried
            lines.append(f"  {name}: not readable yet ({type(e).__name__}: {str(e)[:120]})  FAIL")
            all_ok = False
            continue
        if fn == "title":
            ok, line = compare(name, want_title, got, lambda r: (r["title_id"], r["window_start"]), TITLE_COLS)
        elif fn == "alerts":
            want = {k: {"starts": s, "failures": f} for k, (s, f) in want_alerts.items()}
            ok, line = compare(name, want, got, lambda r: (r["title_id"], r["window_start"]), ["starts", "failures"])
        elif fn == "pops":
            ok, line = compare(name, want_pops, got, lambda r: (r["pop"], r["window_start"]), POP_COLS)
        else:
            ok, line = compare(name, want_ledger, got, lambda r: (r["session_id"], r["started_at"]),
                               LEDGER_COLS, counted=lambda r: True, noun="sessions")
        lines.append(line)
        all_ok &= ok
    return all_ok, lines


KUBECTL = ["kubectl", "--context", "kind-playhead", "-n", "clink"]
API = "http://clink-coordinator:8081"


def in_pod(pod: str, cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(KUBECTL + ["exec", pod, "--", "sh", "-c", cmd],
                          capture_output=True, text=True, timeout=60)


def check_security() -> tuple[bool, list[str]]:
    # Asked from a Worker's pod, as anything else on the cluster's network would.
    code = 'curl -s -o /dev/null -w "%{http_code}" '
    cases = [
        ("the API refuses a request without the token", code + f"{API}/api/v1/jobs", {"401"}),
        ("and answers the cluster's token",
         code + f'-H "Authorization: Bearer $CLINK_AUTH_TOKEN" {API}/api/v1/jobs', {"200"}),
        ("the readiness probe answers without it", f"curl -s {API}/readyz", {"leader", "standby"}),
    ]
    lines, all_ok = [], True
    for what, cmd, want in cases:
        got = in_pod("clink-worker-0", cmd).stdout.strip()
        ok = got in want
        lines.append(f"  security: {what}: {got or '-'}  {'OK' if ok else 'FAIL'}")
        all_ok &= ok
    # The control plane is mutual TLS: the leader's certificate names the host
    # it advertises, and a client without a certificate the CA signed is refused.
    leader = json.loads(in_pod("clink-coordinator-0", "cat /var/lib/clink/ha/active-leader.json").stdout)["host"]
    # -ign_eof reads on after the handshake until the leader closes: under TLS 1.3
    # the refusal of a missing certificate arrives after the client's half of it.
    tls = (f"timeout 30 openssl s_client -connect {leader}:6123 -CAfile /etc/clink/tls/ca.crt "
           f"-verify_hostname {leader} -verify_return_error -ign_eof")
    with_cert = in_pod("clink-worker-0", tls + " -cert /etc/clink/tls/tls.crt -key /etc/clink/tls/tls.key </dev/null")
    without = in_pod("clink-worker-0", tls + " </dev/null")
    # Both sides verify: s_client prints its verdict on the leader's certificate,
    # and the leader refuses a client without one with a TLS alert. The exit
    # status says nothing either way: the leader closes a connection that sends
    # no frame, which OpenSSL reports as an unexpected EOF.
    def said(r: subprocess.CompletedProcess) -> str:
        return (r.stdout + r.stderr).lower()
    accepted = "verify return code: 0 (ok)" in said(with_cert) and "alert" not in said(with_cert)
    refused = "alert" in said(without)
    for what, ok in ((f"a Worker's certificate is accepted by the leader at {leader.split('.')[0]}", accepted),
                     ("a client without a certificate is refused", refused)):
        lines.append(f"  security: {what}  {'OK' if ok else 'FAIL'}")
        all_ok &= ok
    return all_ok, lines


def api_json(path: str):
    # The Coordinator's API with the cluster's token, asked from a Worker's pod.
    got = in_pod("clink-worker-0", f'curl -s -H "Authorization: Bearer $CLINK_AUTH_TOKEN" {API}{path}')
    return json.loads(got.stdout)


def check_platform(deploy: str, upgraded: str, failover: bool, jobs: list[str],
                   parks: list[str]) -> tuple[bool, list[str]]:
    lines, all_ok = [], True

    def line(what: str, ok: bool) -> None:
        nonlocal all_ok
        lines.append(f"  {what}  {'OK' if ok else 'FAIL'}")
        all_ok &= ok

    idle = set()
    if deploy == "operator":
        cjs = {c["metadata"]["name"]: c for c in json.loads(subprocess.run(
            KUBECTL + ["get", "clinkjob", "-o", "json"], capture_output=True, text=True, check=True).stdout)["items"]}
        for name in jobs:
            st = cjs.get(name, {}).get("status", {})
            if name in parks and st.get("phase") == "Suspended" and st.get("suspendReason") == "Idle":
                idle.add(name)
                line(f"operator: ClinkJob {name} is parked for being idle, as it declares", True)
                continue
            line(f"operator: ClinkJob {name} is {st.get('phase', '-')}, job {st.get('jobID', '-')}",
                 st.get("phase") == "Running")
        if upgraded:
            # What scripts/upgrade.sh saw when the new job started: a later
            # failover restores the job again, from its own checkpoints.
            try:
                up = json.loads((ROOT / "state" / f"upgrade-{upgraded}.json").read_text())
            except (OSError, ValueError):
                up = {}
            line(f"operator: the {upgraded} upgrade replaced job {up.get('from', '-')} with job "
                 f"{up.get('to', '-')}, restored from its savepoint {up.get('savepoint', '-')}",
                 bool(up) and up["from"] != up["to"] and up["restored_from"] == up["savepoint"])
    if failover:
        record = json.loads(in_pod("clink-coordinator-0", "cat /var/lib/clink/ha/active-leader.json").stdout)
        running = [j["name"] for j in api_json("/api/v1/jobs")["jobs"] if j["status"] == "RUNNING"]
        line(f"failover: leadership at epoch {record['epoch']} under {record['host'].split('.')[0]}, "
             f"running {sorted(running)}", record["epoch"] >= 2 and set(jobs) - idle <= set(running))
    return all_ok, lines


SCENE_FROM, SCENE_TO = gen.T0 + 33 * MINUTE, gen.T0 + 40 * MINUTE
PARKED_MINUTE = gen.T0 + 34 * MINUTE       # open when the job parked


def expected_scene() -> tuple[dict, dict]:
    """The edge-location minutes 33 to 39 from the tape and the later batch,
    and how many requests of minute 34 came from each side of the park."""
    pops = defaultdict(lambda: dict.fromkeys(POP_COLS, 0))
    sides = defaultdict(lambda: [0, 0])
    for side, name in enumerate(("edge.jsonl", "edge-later.jsonl")):
        with open(ROOT / "data" / name) as f:
            for line in f:
                e = json.loads(line)
                ws = window(e["ts"])
                if not SCENE_FROM <= ws < SCENE_TO:
                    continue
                r = pops[(e["pop"], ws)]
                r["requests"] += 1
                r["bytes"] += e["bytes"]
                r["server_errors"] += e["status"] >= 500
                r["ttfb_ms"] += e["ttfb_ms"]
                if ws == PARKED_MINUTE:
                    sides[e["pop"]][side] += 1
    return dict(pops), dict(sides)


def check_scale_to_zero(upgraded: str, wait: int) -> tuple[bool, list[str]]:
    lines, all_ok = [], True

    def line(what: str, ok: bool) -> None:
        nonlocal all_ok
        lines.append(f"  {what}  {'OK' if ok else 'FAIL'}")
        all_ok &= ok

    try:
        ev = json.loads((ROOT / "state" / "park-edges.json").read_text())
        ended = int((ROOT / "state" / "feed-ended").read_text())
    except (OSError, ValueError) as e:
        return False, [f"  scale to zero: no record of the scene ({e})  FAIL"]
    job_of = lambda sp: sp.split("@")[0].rstrip("/").rsplit("/", 1)[-1]
    parked_at = int(datetime.datetime.fromisoformat(ev["parked_at"].replace("Z", "+00:00")).timestamp())
    up = {}
    if upgraded:
        try:
            up = json.loads((ROOT / "state" / f"upgrade-{upgraded}.json").read_text())
        except (OSError, ValueError):
            up = {}
    started = up.get("to") if upgraded else job_of(ev["parked_savepoint"])
    line(f"scale to zero: edges parked {parked_at - ended} s after the tape ended, the job "
         f"{'the upgrade started, ' if upgraded else ''}job {job_of(ev['parked_savepoint'])}, "
         f"at its savepoint {ev['parked_savepoint']}",
         parked_at >= ended and job_of(ev["parked_savepoint"]) == started)
    line(f"scale to zero: parked, it held no slots: {ev['slots_parked']} of {ev['slot_capacity']} in use, "
         f"by {ev['running_while_parked']}; {ev['slots_running']} once it ran again",
         "edges" not in ev["running_while_parked"].split(",") and ev["slots_running"] > ev["slots_parked"])
    on_topic = sum(1 for _ in open(ROOT / "data" / "edge.jsonl"))
    line(f"scale to zero: the half hour's {on_topic} edge requests on cdn.edge kept it parked: {ev['held']}",
         "lag 0 < threshold" in ev["held"] and len(ev["baseline"]) > 0)
    later = sum(1 for _ in open(ROOT / "data" / "edge-later.jsonl"))
    line(f"scale to zero: the batch of {later} woke it {ev['woke'] - ev['delivered']} s after it was delivered, "
         f"as job {ev['woke_as']}, restored from {ev['restored_from']}",
         ev["restored_from"] == ev["parked_savepoint"] and ev["woke_as"] != job_of(ev["parked_savepoint"]))
    line(f"scale to zero: and it parked again {ev['parked_again'] - ev['delivered']} s after the batch, "
         f"job {job_of(ev['again_savepoint'])} at savepoint {ev['again_savepoint']}",
         job_of(ev["again_savepoint"]) == ev["woke_as"])

    want, sides = expected_scene()
    in_scene = lambda r: SCENE_FROM <= r["window_start"] < SCENE_TO
    name = "gs://cdn-vendor/pop-minute, minutes 33 to 39 (GCS, exactly once)"
    deadline = time.monotonic() + wait
    while True:
        try:
            got = read_gcs()
            ok, out = compare(name, want, got, lambda r: (r["pop"], r["window_start"]), POP_COLS, counted=in_scene)
        except Exception as e:  # not readable yet: retried
            got, ok, out = [], False, f"  {name}: not readable yet ({type(e).__name__})  FAIL"
        if ok or time.monotonic() >= deadline:
            break
        time.sleep(10)
    lines.append(out)
    all_ok &= ok
    rows = {(r["pop"], r["window_start"]): r for r in got}
    straddle = {p: rows.get((p, PARKED_MINUTE), {}).get("requests") for p in sorted(sides)}
    line("scale to zero: minute 34, open when the job parked, holds requests from both sides of the park: "
         + ", ".join(f"{p} {b}+{a}={straddle[p]}" for p, (b, a) in sorted(sides.items())),
         all(straddle[p] == b + a and b > 0 and a > 0 for p, (b, a) in sides.items()))
    seen = Counter((r["pop"], r["window_start"]) for r in got)
    line(f"scale to zero: every one of the bucket's {len(seen)} edge-location minutes once",
         bool(seen) and max(seen.values()) == 1)
    return all_ok, lines


def get_json(url: str):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)


def check_observability(jobs: list[str]) -> tuple[bool, list[str]]:
    lines, all_ok = [], True

    def line(what: str, ok: bool) -> None:
        nonlocal all_ok
        lines.append(f"  {what}  {'OK' if ok else 'FAIL'}")
        all_ok &= ok

    # Lifecycle spans the Coordinators export over OTLP, through the collector to Jaeger.
    jaeger = "http://127.0.0.1:16686/api"
    services = sorted(x for x in (get_json(f"{jaeger}/services").get("data") or []) if x.startswith("clink"))

    def spans(op: str) -> int:
        url = f"{jaeger}/traces?service=clink-coordinator&operation={op}&limit=1000&lookback=2h"
        return sum(len(t["spans"]) for t in (get_json(url).get("data") or []))
    submits, checkpoints = spans("clink.submit"), spans("clink.checkpoint")
    line(f"traces (Jaeger): {services}, {submits} clink.submit and {checkpoints} clink.checkpoint spans",
         "clink-coordinator" in services and submits >= len(jobs) and checkpoints > 0)

    # Both roles' metrics, through the collector's Prometheus exporter.
    query = urllib.parse.quote('count by (job) ({__name__=~"clink_.+"})')
    result = get_json(f"http://127.0.0.1:9090/api/v1/query?query={query}")["data"]["result"]
    roles = sorted(r["metric"].get("job", "?") for r in result)
    line(f"metrics (Prometheus): series from {roles}", {"clink-coordinator", "clink-worker"} <= set(roles))

    # Every job's lineage, as OpenLineage run events, in Marquez.
    found = {j["name"]: j for j in get_json("http://127.0.0.1:5000/api/v1/namespaces/playhead/jobs?limit=100")["jobs"]}
    for name in jobs:
        j = found.get(name)
        state = ((j or {}).get("latestRun") or {}).get("state", "-")
        ins = [f"{d['namespace']}/{d['name']}" for d in (j or {}).get("inputs", [])]
        outs = [f"{d['namespace']}/{d['name']}" for d in (j or {}).get("outputs", [])]
        line(f"lineage (Marquez) {name}: {state}, {ins} -> {outs}", state == "RUNNING" and bool(ins) and bool(outs))
    return all_ok, lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--deploy", choices=["operator", "chart"], default="operator")
    ap.add_argument("--upgraded", default="", help="the job the run upgraded, if any")
    ap.add_argument("--failover", action="store_true", help="the run killed the leading Coordinator")
    ap.add_argument("--parks", action="append", default=[], help="a job that may have parked for being idle")
    ap.add_argument("--scale-to-zero", action="store_true", help="check the scale-to-zero scene instead")
    args = ap.parse_args()
    if args.scale_to_zero:
        ok, lines = check_scale_to_zero(args.upgraded, args.wait)
        print("\n".join(lines))
        print("check: PASS" if ok else "check: FAIL")
        return 0 if ok else 1
    want_title, want_alerts, want_pops, want_ledger = expected()
    print(f"check: the tape says {len(want_title)} title-minutes, {len(want_alerts)} alerts "
          f"({sorted({t for t, _ in want_alerts})}), {len(want_pops)} edge-location minutes up to minute 33, "
          f"{len(want_ledger)} billable sessions")
    deadline = time.monotonic() + args.wait
    while True:
        ok, lines = check_once(want_title, want_alerts, want_pops, want_ledger)
        if ok or time.monotonic() >= deadline:
            break
        time.sleep(10)
    jobs = ["qoe", "alerts", "edges", "ledger"]
    secure, secure_lines = check_security()
    platform, platform_lines = check_platform(args.deploy, args.upgraded, args.failover, jobs, args.parks)
    observed, observed_lines = check_observability(jobs)
    print("\n".join(lines + secure_lines + platform_lines + observed_lines))
    ok &= secure and platform and observed
    print("check: PASS" if ok else "check: FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
