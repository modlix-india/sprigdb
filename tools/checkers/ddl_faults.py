#!/usr/bin/env python3
"""Follower reads under DDL and faults: the two-version rule across nodes.

A writer creates tables t0, t1, ... on the tenant unit's leader (following
the leader after kills). Readers on the other replicas keep reading the
table before the newest acknowledged one: under the two-version rule a
replica never serves a catalog older than the committed version minus one,
so that read must never answer "doesn't exist". Faults, in turn: SIGSTOP a
follower for 8 s, kill -9 a follower, kill -9 the leader (each restarted).
At the end every replica lists the same tables. Exit status 0 when there was
no violation and every replica lists every table.

DDL_FAULTS (comma-separated) picks the faults: stop-follower, kill-follower,
kill-leader, cut-follower, cut-leader. A cut is a network partition of the
Raft links only, for 8 s: the victim's and the others' cut files
(<state>/cut-<n>, SPRIGDB_ENGINE_RAFT_CUT_FILE) name each other, so Raft
frames between them are dropped both ways while the victim keeps its clients
and etcd.

usage: ddl_faults.py <tenant> <tables> [fault_every_s] [pace_s]

Against the local stack: tools/live/stack.sh up 3; stack.sh tenant <tenant>.
Variables: SPRIG_LIVE_PORT, SPRIG_LIVE_STATE (the stack's), DDL_PASSWORD
(default sprig-live-app), DDL_DB (default ddlf, created if missing). Needs the
`mysql` command-line client.
"""
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
STACK = os.path.join(ROOT, "tools", "live", "stack.sh")
B = int(os.environ.get("SPRIG_LIVE_PORT", "45000"))
STATE = os.environ.get("SPRIG_LIVE_STATE", os.path.join(ROOT, "target", "live-stack"))
N = 3
PW = os.environ.get("DDL_PASSWORD", "sprig-live-app")
DB = os.environ.get("DDL_DB", "ddlf")


def mysql(gw, tenant, sql, timeout=20, db=True):
    r = subprocess.run(
        ["mysql", "-h127.0.0.1", f"-P{B + 100 * gw + 6}", f"-u{tenant}.app", f"-p{PW}", "--ssl-mode=REQUIRED",
         "-N", "-B", *(["-D", DB] if db else []), "-e", sql],
        capture_output=True, text=True, timeout=timeout)
    return r.returncode, (r.stdout + r.stderr).strip()


def units_of(gw):
    with urllib.request.urlopen(f"http://127.0.0.1:{B + 100 * gw + 1}/admin/v1/engine/units", timeout=3) as r:
        return json.load(r)


def leader(tid):
    for gw in range(1, N + 1):
        try:
            for u in units_of(gw)["units"]:
                if u["unit"] == tid and u["leading"]:
                    return gw
        except Exception:
            pass
    return None


def np(*args):
    subprocess.run(["bash", STACK, *args], capture_output=True, timeout=180)


RETRYABLE = ("39110", "ShardMoved", "Can't connect", "Lost connection", "gone away", "1213",
             "39070", "39071", "doesn't lead", "no leader", "retry")


class State:
    def __init__(self):
        self.acked = 0          # tables t0..t{acked-1} acknowledged
        self.down = set()       # gateways stopped or killed now
        self.stop = False
        self.violations = []
        self.reads_ok = 0
        self.reads_retry = 0
        self.lock = threading.Lock()


def reader(st, tenant, gw):
    while not st.stop:
        with st.lock:
            k = st.acked - 2  # the table before the newest acknowledged one
            down = gw in st.down
        if k < 0 or down:
            time.sleep(0.05)
            continue
        rc, out = mysql(gw, tenant, f"SELECT COUNT(*) FROM t{k}", timeout=30)
        if rc == 0:
            st.reads_ok += 1
        elif "doesn't exist" in out or "1146" in out:
            st.violations.append((gw, k, out[-160:]))
        else:
            st.reads_retry += 1
        time.sleep(0.02)


def main():
    tenant, tables = sys.argv[1], int(sys.argv[2])
    fault_every = float(sys.argv[3]) if len(sys.argv) > 3 else 12.0
    pace = float(sys.argv[4]) if len(sys.argv) > 4 else 0.15
    shown = subprocess.run(["bash", STACK, "ctl", "-o", "json", "tenant", "show", tenant],
                           capture_output=True, text=True, check=True).stdout
    tid = int(json.loads(shown)["tenant_id"])
    for _ in range(30):
        if mysql(1, tenant, f"CREATE DATABASE IF NOT EXISTS {DB}", db=False)[0] == 0:
            break
        time.sleep(1)
    import base64
    key = base64.b64encode(f"/sprig/dc/dc-a/units/{tid}/{tid}".encode()).decode()
    req = urllib.request.Request(f"http://127.0.0.1:{B + 79}/v3/kv/range",
                                 data=json.dumps({"key": key}).encode(), method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        kvs = json.load(r).get("kvs")
    # With placement on, the unit's descriptor names its voters; without it
    # every node holds every unit.
    members = json.loads(base64.b64decode(kvs[0]["value"]))["voters"] if kvs else list(range(1, N + 1))
    print(f"tenant {tenant} {tid} on gateways {members}", flush=True)
    st = State()
    threads = [threading.Thread(target=reader, args=(st, tenant, gw), daemon=True) for gw in members]
    for t in threads:
        t.start()
    faults = os.environ.get("DDL_FAULTS", "stop-follower,kill-follower,kill-leader").split(",")
    done_faults = []

    def injector():
        # One fault at a time, each over before the next: every
        # `fault_every` seconds.
        fi = 0
        while not st.stop:
            time.sleep(fault_every)
            if st.stop:
                return
            kind = faults[fi % len(faults)]
            fi += 1
            l = leader(tid) or members[0]
            f = random.choice([g for g in members if g != l])
            victim = l if kind in ("kill-leader", "cut-leader") else f
            with st.lock:
                st.down.add(victim)
                at = st.acked
            t0 = time.time()
            if kind.startswith("cut-"):
                others = [g for g in range(1, N + 1) if g != victim]
                open(f"{STATE}/cut-{victim}", "w").write(",".join(map(str, others)))
                for g in others:
                    open(f"{STATE}/cut-{g}", "w").write(str(victim))
                time.sleep(8.0)
                for g in range(1, N + 1):
                    open(f"{STATE}/cut-{g}", "w").write("")
            elif kind == "stop-follower":
                pid = int(open(f"{STATE}/gw{victim}.pid").read().strip())
                os.kill(pid, signal.SIGSTOP)
                time.sleep(8.0)
                os.kill(pid, signal.SIGCONT)
            else:
                np("kill", f"gw{victim}")
                np("start", f"gw{victim}")
            with st.lock:
                st.down.discard(victim)
                done_faults.append((kind, victim, at, st.acked, time.time() - t0))
            print(f"fault {kind} on gw{victim} from table {at} to {st.acked} ({time.time() - t0:.1f}s)", flush=True)

    inj = threading.Thread(target=injector, daemon=True)
    inj.start()
    ddl_ms = []
    retries = 0
    k = 0
    while k < tables:
        l = leader(tid)
        if l is None:
            time.sleep(0.2)
            continue
        t0 = time.time()
        rc, out = mysql(l, tenant, f"CREATE TABLE IF NOT EXISTS t{k} (id INT PRIMARY KEY)", timeout=60)
        if rc != 0:
            if not any(x in out for x in RETRYABLE):
                print(f"t{k} on gw{l}: unexpected: {out[-200:]}", flush=True)
            retries += 1
            time.sleep(0.2)
            continue
        ddl_ms.append((time.time() - t0) * 1000)
        with st.lock:
            st.acked = k + 1
        k += 1
        time.sleep(pace)
    fi = len(done_faults)
    time.sleep(10)
    st.stop = True
    for t in threads:
        t.join(timeout=40)
    # Every replica lists the same tables.
    lists = {}
    for gw in members:
        for _ in range(30):
            rc, out = mysql(gw, tenant, "SHOW TABLES")
            if rc == 0:
                lists[gw] = sorted(x for x in out.split() if x[:1] == "t" and x[1:].isdigit())
                break
            time.sleep(1)
    same = len({tuple(v) for v in lists.values()}) == 1 and len(lists) == len(members)
    count = len(next(iter(lists.values()))) if lists else 0
    ddl_ms.sort()
    p = lambda q: ddl_ms[min(len(ddl_ms) - 1, int(q * len(ddl_ms)))] if ddl_ms else 0
    print(f"SUMMARY tables {tables} acked {st.acked} retries {retries} faults {fi} "
          f"ddl p50 {p(0.5):.0f} ms p99 {p(0.99):.0f} ms max {max(ddl_ms or [0]):.0f} ms; "
          f"follower reads ok {st.reads_ok} retryable {st.reads_retry} violations {len(st.violations)}; "
          f"replicas list the same tables: {same} ({count})")
    for v in st.violations[:10]:
        print("VIOLATION", v)
    sys.exit(0 if same and not st.violations and count == tables else 1)


if __name__ == "__main__":
    main()
