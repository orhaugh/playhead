#!/usr/bin/env python3
"""Deliver the CDN's later batch of edge logs (data/edge-later.jsonl) in one go.

The scale-to-zero scene's wake-up: the vendor's log shipping, quiet since the
half hour ended, delivers the next six minutes at once, onto cdn.edge in
Protobuf framed under the registry's id, as the feed puts the tape there.

The shipper puts each edge location's logs on one partition, as one keyed by
location would (rpk takes no key beside a registry id, so the partition is
named instead). Every partition then carries the batch to its end: the edges
job holds its watermark at its slowest partition, and an unkeyed burst would
leave some partitions minutes behind the others, with nothing after it to
move them on.

Standard library only.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import feed  # noqa: E402  (the registry and the producer)
import gen  # noqa: E402


def partitions(topic: str) -> int:
    out = subprocess.run(feed.COMPOSE + ["exec", "-T", "redpanda", "rpk", "-X", "brokers=redpanda:9092",
                                         "topic", "describe", topic, "-p"],
                         capture_output=True, text=True, check=True).stdout
    return sum(1 for line in out.splitlines()[1:] if line.strip())


def main() -> int:
    edge_id = feed.registry("GET", "/subjects/cdn.edge-value/versions/latest")["id"]
    rows = feed.load("edge-later.jsonl")
    n = partitions("cdn.edge")
    producers = {p: feed.producer("cdn.edge", edge_id, p) for p in range(n)}
    for r in rows:
        producers[gen.POPS.index(r["pop"]) % n].stdin.write(json.dumps(r, separators=(",", ":")) + "\n")
    failed = 0
    for p in producers.values():
        p.stdin.close()
        failed += p.wait() != 0
    if failed:
        print("batch: rpk failed", file=sys.stderr)
        return 1
    span = [(rows[i]["ts"] - gen.T0) / gen.MINUTE for i in (0, -1)]
    print(f"batch: {len(rows)} edge requests delivered across {n} partitions, "
          f"event time {span[0]:.2f} to {span[1]:.2f} minutes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
