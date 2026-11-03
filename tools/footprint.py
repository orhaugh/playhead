#!/usr/bin/env python3
"""Measure what the run costs: clink's pods, and the cloud's containers.

Samples every --interval seconds until it is sent SIGTERM, then writes --out
(JSON: the summary and every sample) and prints the summary. Inside the kind
node, crictl reports each container's cumulative CPU time and working set;
clink's pods are the Coordinators, the Workers and the operator. The kind node
as a whole (Kubernetes and every pod) and the cloud's containers are read from
`docker stats`.

CPU is the time each container ran between the first sample and the last,
summed over a pod's containers: a killed pod comes back as a new container,
whose counter starts again. Memory is the working set, what the kernel would
not reclaim. "Together" is the most the pods held at one sample, not the sum
of each pod's own peak.

Standard library only.
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NODE = "playhead-control-plane"
NAMESPACES = {"clink", "clink-operator-system"}
UNITS = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "kB": 1000, "MB": 1000**2, "GB": 1000**3, "B": 1}
MIB = 1024**2


def pod_role(pod: str) -> str:
    # The operator's pod name ends in its ReplicaSet's hash; the rest are stable.
    return "clink-operator" if pod.startswith("clink-operator-controller") else pod


def clink_containers() -> list[dict]:
    got = subprocess.run(["docker", "exec", NODE, "crictl", "stats", "-o", "json"],
                         capture_output=True, text=True, timeout=30)
    if got.returncode != 0:
        return []
    out = []
    for s in json.loads(got.stdout).get("stats", []):
        labels = s["attributes"].get("labels", {})
        cpu = s.get("cpu", {}).get("usageCoreNanoSeconds", {}).get("value")
        mem = s.get("memory", {}).get("workingSetBytes", {}).get("value")
        # A container that has only just started has no readings yet.
        if labels.get("io.kubernetes.pod.namespace") not in NAMESPACES or cpu is None or mem is None:
            continue
        out.append({
            "id": s["attributes"]["id"],
            "pod": pod_role(labels.get("io.kubernetes.pod.name", "?")),
            "cpu_ns": int(cpu),
            "mem": int(mem),
        })
    return out


def to_bytes(text: str) -> int:
    for unit, scale in UNITS.items():
        if text.endswith(unit):
            return int(float(text[: -len(unit)]) * scale)
    return 0


def docker_memory() -> tuple[int, dict[str, int]]:
    """The kind node's memory, and each of the cloud's containers' by service."""
    names = subprocess.run(["docker", "ps", "--filter", "name=^playhead-cloud-", "--format", "{{.Names}}"],
                           capture_output=True, text=True, timeout=30).stdout.split()
    got = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}", NODE, *names],
                         capture_output=True, text=True, timeout=60)
    node, cloud = 0, {}
    for line in got.stdout.splitlines():
        name, _, usage = line.partition("\t")
        used = to_bytes(usage.split("/")[0].strip())
        if name == NODE:
            node = used
        else:
            cloud[name.removeprefix("playhead-cloud-").rsplit("-", 1)[0]] = used
    return node, cloud


def summarise(samples: list[dict]) -> dict:
    span = samples[-1]["t"] - samples[0]["t"] if samples else 0.0
    first, last, pod_of = {}, {}, {}
    for s in samples:
        for c in s["clink"]:
            first.setdefault(c["id"], c["cpu_ns"])
            last[c["id"]] = c["cpu_ns"]
            pod_of[c["id"]] = c["pod"]
    cpu: dict[str, float] = {}
    for cid, ns in last.items():
        cpu[pod_of[cid]] = cpu.get(pod_of[cid], 0.0) + (ns - first[cid]) / 1e9
    peak: dict[str, int] = {}
    together = 0
    cloud_peak: dict[str, int] = {}
    cloud_together = node_peak = everything = 0
    for s in samples:
        now: dict[str, int] = {}
        for c in s["clink"]:
            now[c["pod"]] = now.get(c["pod"], 0) + c["mem"]
        for pod, mem in now.items():
            peak[pod] = max(peak.get(pod, 0), mem)
        together = max(together, sum(now.values()))
        for service, mem in s["cloud"].items():
            cloud_peak[service] = max(cloud_peak.get(service, 0), mem)
        cloud_together = max(cloud_together, sum(s["cloud"].values()))
        node_peak = max(node_peak, s["node"])
        everything = max(everything, s["node"] + sum(s["cloud"].values()))
    return {
        "seconds": round(span, 1),
        "samples": len(samples),
        "pods": {pod: {"cpu_seconds": round(cpu.get(pod, 0.0), 1), "peak_mib": round(peak[pod] / MIB)}
                 for pod in sorted(peak)},
        "clink_cpu_seconds": round(sum(cpu.values()), 1),
        "clink_peak_mib": round(together / MIB),
        "cloud_peak_mib": {service: round(mem / MIB) for service, mem in sorted(cloud_peak.items())},
        "cloud_together_peak_mib": round(cloud_together / MIB),
        "node_peak_mib": round(node_peak / MIB),
        "everything_peak_mib": round(everything / MIB),
    }


def report(summary: dict) -> str:
    secs = summary["seconds"]
    lines = [f"footprint: {summary['samples']} samples over {secs:.0f} s of the tape playing"]
    for pod, p in summary["pods"].items():
        lines.append(f"  {pod:<22} {p['cpu_seconds']:>7.1f} CPU s  {p['peak_mib']:>6} MiB at its peak")
    cores = summary["clink_cpu_seconds"] / secs if secs else 0.0
    lines.append(f"  clink: {summary['clink_cpu_seconds']:.1f} CPU s ({cores:.2f} cores on average), "
                 f"{summary['clink_peak_mib']} MiB together at the peak")
    lines.append(f"  the kind node, Kubernetes and every pod: {summary['node_peak_mib']} MiB at the peak")
    lines.append(f"  the cloud's {len(summary['cloud_peak_mib'])} containers: "
                 f"{summary['cloud_together_peak_mib']} MiB together at the peak")
    lines.append(f"  the node and the cloud: {summary['everything_peak_mib']} MiB together at the peak")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--out", default=str(ROOT / "state" / "footprint.json"))
    args = ap.parse_args()
    stop = False

    def on_signal(*_: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    samples: list[dict] = []
    start = time.monotonic()
    while not stop:
        at = time.monotonic()
        node, cloud = docker_memory()
        samples.append({"t": round(at - start, 1), "clink": clink_containers(), "node": node, "cloud": cloud})
        while not stop and time.monotonic() < at + args.interval:
            time.sleep(0.2)
    summary = summarise(samples)
    Path(args.out).write_text(json.dumps({"summary": summary, "samples": samples}, indent=1) + "\n")
    print(report(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
