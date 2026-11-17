#!/usr/bin/env python3
"""Writes one benchmark result as JSON: <results>/<model>/<product>/<run>.json.

Every workload script in deploy/bench ends by calling this, once per metric,
so the market report's generator reads one format. Standard library only.

  result.py --model kv --op correct --workload redis-corpus --metric matched \
      --value 416 --total 416 --unit commands --better higher \
      --product Valkey --product-version 9.0.0 --competitor Valkey \
      --config ref=redis:7.4.11 --command "kv/redis-corpus.py ..."

Kinds of record:
  measurement  one product's value for one metric (the default). The report
               pairs SprigDB's measurement with each rival's for the same
               model, op, workload, variant and metric, and works out the
               verdict from the ratio.
  fact         an already-compared result with its own verdict inputs
               (--verdict-json), for results that don't pair one to one.
  planned      a workload that is written but not run yet (a "not measured"
               cell naming the script that will fill it).

The results directory is $BENCH_RESULTS, else ./results next to this
directory. Hosts and ports are always arguments of the workload scripts, so
the same command runs on a laptop or on the benchmark VMs.
"""
import argparse
import datetime
import json
import os
import platform
import re
import socket
import subprocess
import sys

SCHEMA = "sprigdb-market-result/1"
HERE = os.path.dirname(os.path.abspath(__file__))


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-") or "x"


def host_stamp():
    """Where the run happened: hostname class, OS, CPUs and memory."""
    h = {"os": platform.system(), "machine": platform.machine(), "cpus": os.cpu_count()}
    label = os.environ.get("BENCH_HOST_LABEL")
    if label:
        h["label"] = label
    else:
        h["label"] = "laptop" if platform.system() == "Darwin" else socket.gethostname()
    try:
        if platform.system() == "Darwin":
            mem = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout)
        else:
            with open("/proc/meminfo") as f:
                mem = int(f.readline().split()[1]) * 1024
        h["memory_gb"] = round(mem / 2**30)
    except Exception:
        pass
    return h


def results_root(explicit=None):
    return explicit or os.environ.get("BENCH_RESULTS") or os.path.join(os.path.dirname(HERE), "results")


def write(rec, root=None):
    """Fills the common fields, writes the file and returns its path."""
    rec.setdefault("schema", SCHEMA)
    rec.setdefault("kind", "measurement")
    rec.setdefault("date", datetime.date.today().isoformat())
    rec.setdefault("host", host_stamp())
    rec.setdefault("build", os.environ.get("BENCH_BUILD", "not recorded"))
    for k in ("model", "op", "product"):
        if not rec.get(k):
            raise SystemExit(f"result.py: {k} is required")
    if rec["kind"] == "measurement":
        for k in ("workload", "metric"):
            if not rec.get(k):
                raise SystemExit(f"result.py: {k} is required for a measurement")
    run = rec.get("run") or "-".join(
        x for x in (rec.get("workload") or rec.get("op"), rec.get("variant"), rec.get("metric"), rec["date"]) if x)
    rec["run"] = slug(run)
    d = os.path.join(results_root(rec.pop("_root", root)), rec["model"], slug(rec["product"]))
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, rec["run"] + ".json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2, ensure_ascii=False, sort_keys=False)
        f.write("\n")
    return path


def num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except ValueError:
        return v
    return int(f) if f.is_integer() else f


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--kind", default="measurement", choices=["measurement", "fact", "planned"])
    p.add_argument("--model", required=True)
    p.add_argument("--op", required=True)
    p.add_argument("--workload")
    p.add_argument("--variant", help="what separates comparable runs, e.g. threads=16")
    p.add_argument("--metric")
    p.add_argument("--value")
    p.add_argument("--total", help="the denominator for counts (tests run, commands sent)")
    p.add_argument("--unit")
    p.add_argument("--better", choices=["higher", "lower"])
    p.add_argument("--product", required=True, help="the product measured (SprigDB, Valkey, ...)")
    p.add_argument("--product-version", default="")
    p.add_argument("--competitor", help="the report column this product belongs to")
    p.add_argument("--title")
    p.add_argument("--notes")
    p.add_argument("--durability", help="what an acknowledged write means for this run, e.g. appendfsync always")
    p.add_argument("--config", action="append", default=[], help="key=value, repeatable")
    p.add_argument("--build")
    p.add_argument("--date")
    p.add_argument("--run")
    p.add_argument("--source", default="", help="where the number can be checked (log file, file:line)")
    p.add_argument("--command", default="")
    p.add_argument("--verdict-json", help="verdict inputs for a fact, as JSON")
    p.add_argument("--extra-json", help="more fields to merge, as a JSON object")
    p.add_argument("--results", help="results directory (default $BENCH_RESULTS)")
    a = p.parse_args(argv)
    rec = {
        "kind": a.kind, "model": a.model, "op": a.op, "workload": a.workload, "variant": a.variant,
        "metric": a.metric, "value": num(a.value), "total": num(a.total), "unit": a.unit, "better": a.better,
        "product": a.product, "product_version": a.product_version,
        "competitor": a.competitor or a.product, "title": a.title, "notes": a.notes, "durability": a.durability,
        "config": dict(kv.split("=", 1) for kv in a.config),
        "source": a.source, "command": a.command or " ".join(sys.argv),
    }
    for k in ("build", "date", "run"):
        if getattr(a, k):
            rec[k] = getattr(a, k)
    if a.verdict_json:
        rec["verdict_inputs"] = json.loads(a.verdict_json)
    if a.extra_json:
        rec.update(json.loads(a.extra_json))
    rec = {k: v for k, v in rec.items() if v is not None}
    print(write(rec, a.results))


if __name__ == "__main__":
    main()
