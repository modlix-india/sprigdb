#!/usr/bin/env python3
"""Three-process local cluster of replicated engine units: kill -9 the leader
of a tenant's unit in a loop under writes, then check RPO (every acknowledged
write on every replica) and RTO (kill -> first acknowledged write after it).

usage: repl_kill_loop.py <repl_node binary> <rounds> [writers]
Ports: raft 26171-26173, clients 26181-26183 (loopback only).
"""
import os, socket, subprocess, sys, threading, time, random, signal, json

BIN = sys.argv[1]
ROUNDS = int(sys.argv[2])
WRITERS = int(sys.argv[3]) if len(sys.argv) > 3 else 4
TENANTS = [9101, 9102]
HERE = os.path.dirname(os.path.abspath(__file__))
# Each node's directory and log: REPL_KILL_DIR, default target/repl-kill-loop.
DATA = os.environ.get("REPL_KILL_DIR", os.path.join(HERE, "..", "..", "target", "repl-kill-loop"))
RAFT = ",".join(f"127.0.0.1:{26170 + i}" for i in (1, 2, 3))
CLIENT = {i: ("127.0.0.1", 26180 + i) for i in (1, 2, 3)}

procs = {}
logs = {}


def start(i):
    os.makedirs(os.path.join(DATA, f"n{i}"), exist_ok=True)
    logs[i] = open(os.path.join(DATA, f"n{i}.log"), "ab")
    procs[i] = subprocess.Popen(
        [BIN, str(i), os.path.join(DATA, f"n{i}"), RAFT, f"127.0.0.1:{26180 + i}"],
        stdout=logs[i], stderr=logs[i])


class Conn:
    def __init__(self, i):
        self.i = i
        self.s = socket.create_connection(CLIENT[i], timeout=10)
        self.f = self.s.makefile("rw")

    def ask(self, line):
        self.f.write(line + "\n")
        self.f.flush()
        r = self.f.readline()
        if not r:
            raise ConnectionError("closed")
        return r.strip()

    def close(self):
        try:
            self.s.close()
        except OSError:
            pass


def ask(i, line, timeout=10):
    c = Conn(i)
    c.s.settimeout(timeout)
    try:
        return c.ask(line)
    finally:
        c.close()


def wait_up(i, limit=60):
    end = time.time() + limit
    while time.time() < end:
        try:
            ask(i, "LEADER 1", 2)
            return
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"node {i} didn't come up")


def leader(t, limit=30):
    end = time.time() + limit
    while time.time() < end:
        for i in (1, 2, 3):
            try:
                if ask(i, f"LEADER {t}", 2).startswith("YES"):
                    return i
            except OSError:
                pass
        time.sleep(0.05)
    return None


lock = threading.Lock()
acked = []          # (tenant, id, value, time)
errors = {"retry": 0, "fail": 0, "conn": 0}
stop = threading.Event()


def writer(w):
    n = 0
    target = 1
    conn = None
    rng = random.Random(w)
    while not stop.is_set():
        t = rng.choice(TENANTS)
        n += 1
        rid = w * 10_000_000 + n
        val = f"v{rid}"
        try:
            if conn is None:
                conn = Conn(target)
                conn.s.settimeout(10)
            r = conn.ask(f"PUT {t} {rid} {val}")
        except OSError:
            r = "ERR conn"
            if conn:
                conn.close()
            conn = None
        if r == "OK":
            with lock:
                acked.append((t, rid, val, time.time()))
        else:
            kind = r.split()[1] if len(r.split()) > 1 else "fail"
            with lock:
                errors[kind] = errors.get(kind, 0) + 1
            if kind == "fail":
                with lock:
                    errors.setdefault("samples", []).append(r[:200]) if len(errors.get("samples", [])) < 5 else None
            # Try another node: the leader moved or this one is down.
            target = target % 3 + 1
            if conn:
                conn.close()
            conn = None
            time.sleep(0.01)


def main():
    subprocess.run(["rm", "-rf", DATA])
    for i in (1, 2, 3):
        start(i)
    for i in (1, 2, 3):
        wait_up(i)
    # Start the tenants' units.
    for t in TENANTS:
        end = time.time() + 30
        while time.time() < end:
            r = ask(1, f"PUT {t} 0 init")
            if r == "OK":
                break
            l = leader(t, 2)
            if l and ask(l, f"PUT {t} 0 init") == "OK":
                break
            time.sleep(0.1)
    ws = [threading.Thread(target=writer, args=(w + 1,), daemon=True) for w in range(WRITERS)]
    for w in ws:
        w.start()
    rtos = []
    kills = []
    for rnd in range(ROUNDS):
        time.sleep(random.uniform(1.0, 2.0))
        t = TENANTS[rnd % len(TENANTS)]
        l = leader(t)
        if l is None:
            print(f"round {rnd}: no leader for {t}", flush=True)
            continue
        at = time.time()
        procs[l].send_signal(signal.SIGKILL)
        procs[l].wait()
        # RTO: from the kill to the first write of that tenant acknowledged after it.
        rto = None
        end = at + 30
        while time.time() < end:
            with lock:
                after = [a[3] for a in acked[-2000:] if a[0] == t and a[3] > at]
            if after:
                rto = min(after) - at
                break
            time.sleep(0.01)
        rtos.append(rto if rto is not None else float("inf"))
        kills.append((rnd, t, l, rto))
        print(f"round {rnd}: killed n{l} (leader of {t}); rto {rto if rto is None else round(rto, 3)} s; acked {len(acked)}", flush=True)
        time.sleep(0.3)
        start(l)
        wait_up(l)
    stop.set()
    for w in ws:
        w.join()
    # Convergence: every replica's checksums equal, per tenant.
    for t in TENANTS:
        # One more write through the leader so every replica learns the
        # final commit index.
        for _ in range(100):
            l = leader(t)
            if l and ask(l, f"PUT {t} 1 final") == "OK":
                break
        end = time.time() + 60
        while True:
            sums = {i: ask(i, f"SUM {t}") for i in (1, 2, 3)}
            rows = {i: s.split()[1:3] for i, s in sums.items()}
            if len({tuple(v) for v in rows.values()}) == 1:
                break
            if time.time() > end:
                print(f"tenant {t}: replicas did not converge: {sums}", flush=True)
                break
            time.sleep(0.2)
    lost = []
    conns = {i: Conn(i) for i in (1, 2, 3)}
    for (t, rid, val, _) in acked:
        for i in (1, 2, 3):
            r = conns[i].ask(f"GET {t} {rid}")
            if r != f"VAL {val}":
                lost.append((t, rid, i, r))
    finite = sorted(x for x in rtos if x != float("inf"))
    def pct(p):
        if not finite:
            return None
        return round(finite[min(len(finite) - 1, int(p * len(finite)))], 3)
    res = {
        "rounds": ROUNDS, "acked": len(acked), "lost": len(lost), "lost_samples": lost[:10],
        "errors": errors, "rto_p50": pct(0.5), "rto_p99": pct(0.99),
        "rto_max": round(max(rtos), 3) if rtos else None,
        "rto_missing": sum(1 for x in rtos if x == float("inf")),
    }
    print(json.dumps(res), flush=True)
    for p in procs.values():
        p.send_signal(signal.SIGKILL)
    return not lost


if __name__ == "__main__":
    ok = False
    try:
        ok = main()
    finally:
        for p in procs.values():
            try:
                p.send_signal(signal.SIGKILL)
            except Exception:
                pass
    sys.exit(0 if ok else 1)
