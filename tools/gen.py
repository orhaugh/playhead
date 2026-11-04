#!/usr/bin/env python3
"""Generate a deterministic half hour of viewing, the tape every check is held to.

Sessions start across thirty minutes of event time, then five more minutes of
ordinary traffic follow so that every checked window closes. Each session
starts, shows its first frame or fails, stalls now and then, switches bitrate,
and stops; while it plays, the player fetches a segment from a CDN edge every
few seconds. Two incidents are planted: from minute 12 to minute 16 one title's
new encode fails to start on most devices, and from minute 20 to minute 23 one
edge location answers slowly and sometimes with 503s. App 5.0 rolls out at
minute 15; its events carry the network the player was on, in the v2 schema.

Then the CDN's next delivery of edge logs, for the scale-to-zero scene: after
the half hour its logs stop and the edges job parks; the vendor then delivers
six more minutes in one batch, which wakes it. The batch starts ten seconds
before the tape's last minute ends, so the window still open when the job
parked takes requests from both sides of the park. It draws from a random
stream of its own, so the tape is the same with or without it.

Output, under data/ (one JSON object per line, in event-time order):
  playback-v1.jsonl   events from apps before 5.0, Avro-JSON for the v1 schema
  playback-v2.jsonl   events from app 5.0, Avro-JSON for the v2 schema
  edge.jsonl          edge requests, JSON for the Protobuf message
  sessions.jsonl      each session's title and start, for the checks
  edge-later.jsonl    the CDN's later batch of edge requests

Avro's JSON encoding wraps a non-null union value in its branch name
({"long": 250}); rpk, which frames the records for the registry, requires it.

Standard library only.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
T0 = 1_767_603_600_000          # 2026-01-05 09:00:00 UTC, the start of the half hour
MINUTE = 60_000
NIGHT_MIN = 30                  # sessions start within these minutes
TAIL_MIN = 5                    # ordinary traffic after them, so the checked windows close
TITLES = [f"t{i:02d}" for i in range(1, 13)]
POPS = ["lhr", "fra", "ams", "iad", "sfo", "nrt"]
DEVICES = ["TV", "MOBILE", "WEB", "CONSOLE"]
NETWORKS = ["WIFI", "CELLULAR", "ETHERNET"]
BAD_TITLE, BAD_FROM, BAD_TO = "t07", 12, 16      # the broken encode, minutes [12, 16)
SLOW_POP, SLOW_FROM, SLOW_TO = "fra", 20, 23     # the slow edge, minutes [20, 23)
ROLLOUT_MIN = 15                                  # app 5.0 from here on
LATER_FROM = T0 + (NIGHT_MIN + TAIL_MIN) * MINUTE - 10_000   # the later batch, from 34:50
LATER_TO = LATER_FROM + 6 * MINUTE + 10_000                   # to 41:00
LATER_SESSIONS = 300
LATER_PLAYING = 100                               # already playing as the batch starts


def union(branch: str, value):
    return None if value is None else {branch: value}


def later_batch(rnd: random.Random) -> list[dict]:
    edges: list[dict] = []
    for n in range(LATER_SESSIONS):
        start = LATER_FROM if n < LATER_PLAYING else LATER_FROM + rnd.randrange(5 * MINUTE)
        stop = min(start + rnd.randrange(60_000, 5 * MINUTE), LATER_TO - 1)
        pop, bitrate, t = rnd.choice(POPS), rnd.choice([1800, 3200, 5000, 7800]), start
        while True:
            t += rnd.randrange(4000, 8000)
            if t >= stop:
                break
            status = 500 if rnd.random() < 0.003 else 200
            edges.append({"request_id": f"q{len(edges) + 1:07d}", "session_id": f"l{n:05d}", "pop": pop,
                          "bytes": 0 if status != 200 else bitrate * 6 * 125 + rnd.randrange(0, 4096),
                          "ttfb_ms": rnd.randrange(15, 90), "status": status, "ts": t})
    edges.sort(key=lambda e: (e["ts"], e["request_id"]))
    return edges


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sessions", type=int, default=6000)
    ap.add_argument("--out", type=Path, default=ROOT / "data")
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)

    events: list[tuple[int, int, dict]] = []   # (ts, schema version, record)
    edges: list[dict] = []
    sessions: list[dict] = []
    seq = 0
    end_ms = T0 + (NIGHT_MIN + TAIL_MIN) * MINUTE

    def emit(version: int, ts: int, rec: dict) -> None:
        nonlocal seq
        seq += 1
        rec = {"event_id": f"e{seq:07d}", **rec, "ts": ts}
        events.append((ts, version, rec))

    for n in range(args.sessions):
        start = T0 + rnd.randrange((NIGHT_MIN + TAIL_MIN - 1) * MINUTE)
        minute = (start - T0) // MINUTE
        sid = f"s{n:05d}"
        title = rnd.choice(TITLES)
        device = rnd.choice(DEVICES)
        new_app = minute >= ROLLOUT_MIN and rnd.random() < 0.6
        version = 2 if new_app else 1
        app = "5.0.0" if new_app else rnd.choice(["4.8.2", "4.9.0", "4.9.1"])
        network = rnd.choice(NETWORKS) if new_app else None
        sessions.append({"session_id": sid, "title_id": title, "start": start, "version": version})

        def ev(ts: int, kind: str, **extra) -> None:
            rec = {"session_id": sid, "title_id": title, "device": device, "app_version": app,
                   "event_type": kind,
                   "startup_ms": union("long", extra.get("startup_ms")),
                   "stall_ms": union("long", extra.get("stall_ms")),
                   "bitrate_kbps": union("int", extra.get("bitrate_kbps")),
                   "error_code": union("string", extra.get("error_code"))}
            if version == 2:
                rec["network"] = union("media.playhead.Network", network)
            emit(version, ts, rec)

        ev(start, "START")
        broken = title == BAD_TITLE and BAD_FROM <= minute < BAD_TO and device != "WEB"
        if broken or rnd.random() < 0.02:
            ev(start + rnd.randrange(800, 4000), "ERROR",
               error_code="DECODE_PIPELINE" if broken else rnd.choice(["DRM_LICENSE", "NETWORK_TIMEOUT"]))
            continue
        startup = rnd.randrange(400, 2500)
        first = start + startup
        ev(first, "FIRST_FRAME", startup_ms=startup)
        watch = rnd.randrange(60_000, 9 * MINUTE)
        stop = min(first + watch, end_ms - 1)
        t = first
        bitrate = rnd.choice([1800, 3200, 5000, 7800])
        pop = rnd.choice(POPS)
        while True:
            t += rnd.randrange(4000, 8000)
            if t >= stop:
                break
            pop_minute = (t - T0) // MINUTE
            slow = pop == SLOW_POP and SLOW_FROM <= pop_minute < SLOW_TO
            status = 503 if slow and rnd.random() < 0.25 else (500 if rnd.random() < 0.003 else 200)
            edges.append({"request_id": f"r{len(edges) + 1:07d}", "session_id": sid, "pop": pop,
                          "bytes": 0 if status != 200 else bitrate * 6 * 125 + rnd.randrange(0, 4096),
                          "ttfb_ms": rnd.randrange(400, 1600) if slow else rnd.randrange(15, 90),
                          "status": status, "ts": t})
            r = rnd.random()
            if r < 0.04:
                ev(t, "REBUFFER", stall_ms=rnd.randrange(300, 6000))
            elif r < 0.07:
                bitrate = rnd.choice([1800, 3200, 5000, 7800])
                ev(t, "BITRATE_CHANGE", bitrate_kbps=bitrate)
        ev(stop, "STOP")

    events.sort(key=lambda e: (e[0], e[2]["event_id"]))
    edges.sort(key=lambda e: (e["ts"], e["request_id"]))
    with open(args.out / "playback-v1.jsonl", "w") as v1, open(args.out / "playback-v2.jsonl", "w") as v2:
        for _, version, rec in events:
            (v2 if version == 2 else v1).write(json.dumps(rec, separators=(",", ":")) + "\n")
    with open(args.out / "edge.jsonl", "w") as f:
        for e in edges:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
    with open(args.out / "sessions.jsonl", "w") as f:
        for s in sessions:
            f.write(json.dumps(s, separators=(",", ":")) + "\n")
    later = later_batch(random.Random(args.seed + 1))
    with open(args.out / "edge-later.jsonl", "w") as f:
        for e in later:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
    nv2 = sum(1 for e in events if e[1] == 2)
    print(f"gen: {len(sessions)} sessions, {len(events)} playback events "
          f"({len(events) - nv2} v1, {nv2} v2), {len(edges)} edge requests, "
          f"and a later batch of {len(later)} edge requests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
