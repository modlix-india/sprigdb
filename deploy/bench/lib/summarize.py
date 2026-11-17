#!/usr/bin/env python3
"""Writes the paired result of a multi-run measurement: the median of its runs.

The workload scripts write one file per measured run (config "run": N, kept
out of the report's pairing with "pair": false). This reads every per-run file
of one invocation (its run id, e.g. 20261117-201500) and writes, for each
model / op / workload / variant / metric / product, one measurement whose value
is the median of the runs, with the runs' values, minimum, maximum and spread
((max - min) / median) in its config. The report pairs these. Standard library only.

  summarize.py --run-id 20261117-201500 [--results DIR]
"""
import argparse
import glob
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import result  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", required=True, help="the --run id the workload script was given")
    p.add_argument("--results", default=None)
    a = p.parse_args()
    root = result.results_root(a.results)
    tag = result.slug(a.run_id) + "-run"
    groups = {}
    for path in glob.glob(os.path.join(root, "**", "*.json"), recursive=True):
        try:
            r = json.load(open(path, encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if tag not in r.get("run", "") or "run" not in r.get("config", {}) or r.get("kind") != "measurement":
            continue
        key = (r["model"], r["op"], r.get("workload"), r.get("variant") or "", r.get("metric"), r["product"])
        groups.setdefault(key, []).append(r)
    n = 0
    for key, rs in sorted(groups.items()):
        rs.sort(key=lambda r: int(str(r["config"]["run"])))
        vals = [r["value"] for r in rs if isinstance(r.get("value"), (int, float))]
        if not vals:
            print(f"no numeric values for {key}", file=sys.stderr)
            continue
        med = statistics.median(vals)
        base = dict(rs[0])
        cfg = {k: v for k, v in base.get("config", {}).items() if k != "run"}
        for r in rs[1:]:
            for k, v in r.get("config", {}).items():
                if k != "run":
                    cfg.setdefault(k, v)
        spread = (max(vals) - min(vals)) / med if med else None
        cfg.update({"runs": len(vals), "run_values": vals, "min": min(vals), "max": max(vals),
                    "spread": round(spread, 4) if spread is not None else None, "aggregate": "median"})
        base.update({"value": round(med, 3) if isinstance(med, float) else med, "config": cfg,
                     "run": base["run"].replace(tag + str(rs[0]["config"]["run"]), result.slug(a.run_id) + "-median")})
        if not base.get("keep_unpaired"):
            base.pop("pair", None)  # the median is the paired result
        base["notes"] = ((base.get("notes") or "") + f" Median of {len(vals)} runs ({min(vals)}–{max(vals)}).").strip()
        base.pop("host", None)
        base["host"] = rs[0].get("host")
        print(result.write(base, root))
        n += 1
    print(f"{n} medians written", file=sys.stderr)


if __name__ == "__main__":
    main()
