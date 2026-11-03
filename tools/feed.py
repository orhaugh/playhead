#!/usr/bin/env python3
"""Play the tape into the cloud's Kafka the way the apps and the CDN would.

Three producers run for the whole feed, one per schema the topics carry:
playback events from apps before 5.0 (Avro v1), from app 5.0 (Avro v2) and the
CDN's edge requests (Protobuf). rpk frames each record for the Schema Registry
under the id it is given. The tape is fed in ten-second slices of event time,
in order, at --speed times real time. The records carry no key: rpk does not
take a key and a registry schema together, and nothing here depends on one (the
jobs window by event time and join on the session within a time band).

App 5.0 rolls out at minute 15: v2 is registered under playback.events-value
then, mid-run, and its producer starts. One message no schema decodes is put
on playback.events at --poison-minute; the jobs are declared to skip such a
message and count it.

--at MINUTE:COMMAND runs COMMAND (through the shell) when the feed reaches
MINUTE of event time, for the scenes run.sh stages (the Worker kill).

Standard library only.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import gen  # noqa: E402  (the tape's constants)

REGISTRY = "http://127.0.0.1:18081"
SLICE_MS = 10_000
COMPOSE = ["docker", "compose", "-f", str(ROOT / "cloud" / "compose.yml"),
           "--env-file", str(ROOT / "state" / "cloud.env")]


def registry(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(REGISTRY + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def producer(topic: str, schema_id: int | None) -> subprocess.Popen:
    cmd = COMPOSE + ["exec", "-T", "redpanda", "rpk", "-X", "brokers=redpanda:9092",
                     "-X", "registry.hosts=localhost:8081", "topic", "produce", topic]
    if schema_id is not None:
        cmd += ["--schema-id", str(schema_id)]
    return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)


def load(name: str) -> list[dict]:
    with open(ROOT / "data" / name) as f:
        return [json.loads(line) for line in f]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed", type=float, default=15.0)
    ap.add_argument("--poison-minute", type=int, default=8)
    ap.add_argument("--at", action="append", default=[], metavar="MINUTE:COMMAND")
    args = ap.parse_args()
    hooks = sorted((int(h.split(":", 1)[0]), h.split(":", 1)[1]) for h in args.at)

    v1_id = registry("GET", "/subjects/playback.events-value/versions/1")["id"]
    edge_id = registry("GET", "/subjects/cdn.edge-value/versions/latest")["id"]
    v1, v2, edges = load("playback-v1.jsonl"), load("playback-v2.jsonl"), load("edge.jsonl")
    p_v1, p_edge, p_v2 = producer("playback.events", v1_id), producer("cdn.edge", edge_id), None
    i1 = i2 = ie = 0
    end = gen.T0 + (gen.NIGHT_MIN + gen.TAIL_MIN) * gen.MINUTE
    poisoned = False
    started = time.monotonic()
    t = gen.T0
    while t < end:
        hi = t + SLICE_MS
        minute = (t - gen.T0) // gen.MINUTE
        if p_v2 is None and minute >= gen.ROLLOUT_MIN:
            schema = (ROOT / "schemas" / "playback-v2.avsc").read_text()
            v2_id = registry("POST", "/subjects/playback.events-value/versions",
                             {"schema": schema, "schemaType": "AVRO"})["id"]
            print(f"feed: minute {minute}: app 5.0 rolls out, v2 registered as schema id {v2_id}", flush=True)
            p_v2 = producer("playback.events", v2_id)
        while i1 < len(v1) and v1[i1]["ts"] < hi:
            p_v1.stdin.write(json.dumps(v1[i1], separators=(",", ":")) + "\n")
            i1 += 1
        while i2 < len(v2) and v2[i2]["ts"] < hi:
            assert p_v2 is not None, "a v2 event before the rollout"
            p_v2.stdin.write(json.dumps(v2[i2], separators=(",", ":")) + "\n")
            i2 += 1
        while ie < len(edges) and edges[ie]["ts"] < hi:
            p_edge.stdin.write(json.dumps(edges[ie], separators=(",", ":")) + "\n")
            ie += 1
        for p in (p_v1, p_v2, p_edge):
            if p is not None:
                p.stdin.flush()
        if not poisoned and minute >= args.poison_minute:
            raw = producer("playback.events", None)
            raw.communicate("this is not a registry-framed record\n")
            poisoned = True
            print(f"feed: minute {minute}: one undecodable message on playback.events", flush=True)
        while hooks and hooks[0][0] <= minute:
            at, cmd = hooks.pop(0)
            print(f"feed: minute {at}: {cmd}", flush=True)
            subprocess.run(cmd, shell=True, check=False)
        if (t - gen.T0) % gen.MINUTE == 0:
            print(f"feed: minute {minute}", flush=True)
        t = hi
        wait = started + (t - gen.T0) / 1000.0 / args.speed - time.monotonic()
        if wait > 0:
            time.sleep(wait)
    for p in (p_v1, p_v2, p_edge):
        if p is not None:
            p.stdin.close()
            p.wait()
    print(f"feed: {i1 + i2} playback events ({i1} v1, {i2} v2) and {ie} edge requests sent", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
