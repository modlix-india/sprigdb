#!/usr/bin/env python3
"""The E0 kill -9 runner: loops of write load, a SIGKILL of the server at a
random moment, a restart on the same data, and a check that every
acknowledged write survived and no multi-record write is half there.

Run it through e0/crash.sh (it sources env.sh):

  e0/crash.sh --arm e0|redis|mysql|mariadb --proto redis|sql [--loops 100] [--min-s 1] [--max-s 6] [--conns 8]

Each loop: start crashload.py write in a container (8 connections), wait a
random 1-6 s, `stack.sh kill <arm>` (SIGKILL; the container's processes die
with it), wait for the writer to notice, `stack.sh start <arm>` (recovery on
the same data), then crashload.py verify. Writes one result: <model>/ha/
kill9-<proto> with acknowledged writes kept of acknowledged (lost = total -
value) and the partial count in the config; a per-loop JSON log beside it.
"""
import argparse
import datetime
import json
import os
import random
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
E0_DIR = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(os.path.dirname(E0_DIR), "lib"))
import result  # noqa: E402

ENV = os.environ
DOCKER = ENV.get("E0_DOCKER", "docker").split()
NET, IMG_RUN = ENV.get("E0_NET", "e0bench"), ENV.get("IMG_RUN", "e0bench-run:13.7")
CLIENT_CPUS = ENV.get("E0_CLIENT_CPUS", "12-15")
LOGS = os.path.abspath(ENV.get("BENCH_LOGS", "."))
APP_PW = ENV.get("E0_APP_PW", "e0bench-app")

ARMS = {  # arm -> redis endpoint, sql endpoint, user, product, competitor, durability
    "redis": ("e0bench-redis:6379", None, "", "Redis", "Redis 7.4 / 8.1", "appendfsync always (fsync per write)"),
    "mysql": (None, "e0bench-mysql:3306", ENV.get("E0_APP_USER", "sbtest"), "MySQL", "MySQL 8.4", "innodb_flush_log_at_trx_commit=1, sync_binlog=1"),
    "mariadb": (None, "e0bench-mariadb:3306", ENV.get("E0_APP_USER", "sbtest"), "MariaDB", "MariaDB 11.8 (direct)", "innodb_flush_log_at_trx_commit=1, sync_binlog=1"),
    "e0": ("e0bench-sprig:6379", "e0bench-sprig:3306", ENV.get("E0_NODE_USER", "bench.app"), "SprigDB E0", "SprigDB E0",
           ENV.get("E0_DURABILITY", "commit per write: one fdatasync per commit batch (group commit)")),
}


def say(*a):
    print(f"e0-crash: {time.strftime('%H:%M:%S')}", *a, file=sys.stderr, flush=True)


def tool(args, acks, name, timeout=None):
    cmd = DOCKER + ["run", "--rm", "--name", name, "--network", NET, "--cpuset-cpus", CLIENT_CPUS,
                    "-v", f"{E0_DIR}:/opt/e0:ro", "-v", f"{acks}:/acks", IMG_RUN, "python3", "-I", "/opt/e0/lib/crashload.py"] + args
    return cmd


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arm", required=True, choices=sorted(ARMS))
    ap.add_argument("--proto", required=True, choices=["redis", "sql"])
    ap.add_argument("--loops", type=int, default=100)
    ap.add_argument("--min-s", type=float, default=1)
    ap.add_argument("--max-s", type=float, default=6)
    ap.add_argument("--conns", type=int, default=8)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--run-id", default=datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    rd, sq, user, product, comp, dur = ARMS[a.arm]
    target = rd if a.proto == "redis" else sq
    if not target:
        raise SystemExit(f"arm {a.arm} has no {a.proto} endpoint")
    if a.loops > 130 or a.conns > 16:
        raise SystemExit("at most 130 loops and 16 connections (SQL ids are 32-bit)")
    rng = random.Random(a.seed)
    rel = f"e0-crash-{a.arm}-{a.proto}-{a.run_id}"
    out = os.path.join(LOGS, rel)
    acks = os.path.join(out, "acks")
    os.makedirs(acks, exist_ok=True)
    cred = (["--user", user] if user else []) + (["--password", APP_PW] if user else []) + ["--db", "e0crash"]
    stack = [os.path.join(E0_DIR, "stack.sh")]
    loops, tot_acked, tot_lost, tot_partial, tot_multi = [], 0, 0, 0, 0
    t_start = time.time()
    for i in range(1, a.loops + 1):
        delay = rng.uniform(a.min_s, a.max_s)
        w = subprocess.Popen(tool(["write", "--proto", a.proto, "--target", target, "--loop", str(i), "--dir", "/acks",
                                   "--conns", str(a.conns), "--seconds", str(a.max_s + 30)] + cred, acks, f"e0bench-crw-{os.getpid()}"),
                             stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(delay)
        subprocess.run(["bash"] + stack + ["kill", a.arm], capture_output=True)
        t_kill = time.time()
        try:
            w.wait(timeout=60)
        except subprocess.TimeoutExpired:
            subprocess.run(DOCKER + ["rm", "-f", f"e0bench-crw-{os.getpid()}"], capture_output=True)
            w.wait()
        r = subprocess.run(["bash"] + stack + ["start", a.arm], capture_output=True, text=True)
        if r.returncode != 0:
            say(f"loop {i}: restart failed: {r.stderr[-500:]}")
            loops.append({"loop": i, "error": "restart failed", "stderr": r.stderr[-500:]})
            break
        t_up = time.time()
        v = subprocess.run(tool(["verify", "--proto", a.proto, "--target", target, "--loop", str(i), "--dir", "/acks"] + cred,
                                acks, f"e0bench-crv-{os.getpid()}"), capture_output=True, text=True)
        try:
            res = json.loads(v.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            say(f"loop {i}: verify failed: {v.stderr[-800:]}")
            loops.append({"loop": i, "error": "verify failed", "stderr": v.stderr[-800:]})
            break
        res.update(kill_after_s=round(delay, 2), recovery_s=round(t_up - t_kill, 2))
        loops.append(res)
        tot_acked += res["acked"]
        tot_lost += res["lost"]
        tot_partial += res["partial"]
        tot_multi += res["attempted_multi"]
        say(f"loop {i}: killed after {delay:.1f} s, up in {t_up - t_kill:.1f} s; acked {res['acked']}, lost {res['lost']}, partial {res['partial']}")
        if res["lost"] or res["partial"]:
            say("  " + "; ".join(res["details"][:5]))
    with open(os.path.join(out, "loops.jsonl"), "w") as f:
        for x in loops:
            f.write(json.dumps(x) + "\n")
    done = sum(1 for x in loops if "error" not in x)
    model = "kv" if a.proto == "redis" else "sql"
    summary = (f"{done} kill -9 loops: {tot_acked - tot_lost} of {tot_acked} acknowledged writes kept, "
               f"{tot_lost} lost, {tot_partial} partial multi-record writes (of {tot_multi} attempted)")
    print(summary)
    result.write({
        "kind": "measurement", "model": model, "op": "ha", "workload": f"kill9-{a.proto}", "metric": "acked_kept",
        "value": tot_acked - tot_lost, "total": tot_acked, "unit": "acknowledged writes", "better": "higher",
        "product": product, "product_version": ENV.get("E0_VERSION", ""), "competitor": comp, "durability": dur,
        "build": ENV.get("BENCH_BUILD", "not recorded"), "title": summary,
        "notes": (f"Each loop: {a.conns} connections writing (single writes, multi-record writes, counters), SIGKILL after "
                  f"{a.min_s}-{a.max_s} s, restart on the same data, then every acknowledged write checked."
                  + ("" if done == a.loops else f" Stopped after {done} of {a.loops} loops: {loops[-1].get('error')}.")),
        "config": {"loops": done, "partial": tot_partial, "multi_attempted": tot_multi, "lost": tot_lost,
                   "seconds": round(time.time() - t_start)},
        "source": f"logs/{rel}", "run": f"kill9-{a.proto}-{result.slug(product)}-{a.run_id}",
        "command": "e0/crash.sh " + " ".join(sys.argv[1:]),
    })
    sys.exit(0 if (tot_lost == 0 and tot_partial == 0 and done == a.loops) else 1)


if __name__ == "__main__":
    main()
