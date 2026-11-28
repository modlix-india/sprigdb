#!/usr/bin/env python3
"""Kill loop: kill -9 the gateway that leads the tenant's unit, again and
again, under SQL and Redis writes; then check RPO (every acknowledged write
is on every replica) and RTO (kill to the first acknowledged write after it,
per front end).

usage: gw_kill.py <rounds> [sql writers] [redis writers]

Against the local stack (tools/live/stack.sh up 3; stack.sh tenant kill):
tenant `kill`, login kill.app / sprig-live-app; it makes database kl.
Exit status 0 when no acknowledged write is missing on any replica.

Variables (defaults: the local stack):
  KL_TENANT, KL_PASSWORD   the tenant (login <tenant>.app) and its password
  SPRIG_LIVE_PORT          the stack's port base (tools/live/stack.sh)
  GW_HOSTS                 three hosts, comma-separated, for a remote cluster
  GW_MYSQL, GW_REDIS, GW_READYZ, GW_HTTP   one port each for every remote host
  GW_CTL                   a command taking `kill gwN` / `start gwN`
                           (default tools/live/stack.sh)
Pure standard library; probe.py beside it speaks the MySQL protocol.
"""
import contextlib, io, json, os, random, socket, ssl, struct, subprocess, sys, threading, time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probe  # noqa: E402

# probe.login prints its handshake; silence it per module (redirecting sys.stdout races across threads).
probe.print = lambda *a, **k: None

HERE = os.path.dirname(os.path.abspath(__file__))
NR = os.environ.get("GW_CTL", os.path.join(HERE, "..", "live", "stack.sh"))
HOST = dict(zip((1, 2, 3), os.environ.get("GW_HOSTS", "127.0.0.1,127.0.0.1,127.0.0.1").split(",")))
ROUNDS = int(sys.argv[1])
SQLW = int(sys.argv[2]) if len(sys.argv) > 2 else 4
REDW = int(sys.argv[3]) if len(sys.argv) > 3 else 4
GW = (1, 2, 3)
B = int(os.environ.get("SPRIG_LIVE_PORT", "45000"))


def gw_port(var, n, off):
    return int(os.environ[var]) if var in os.environ else B + 100 * n + off


MY = {n: gw_port("GW_MYSQL", n, 6) for n in GW}
RE = {n: gw_port("GW_REDIS", n, 9) for n in GW}
HT = {n: gw_port("GW_HTTP", n, 1) for n in GW}
RZ = {n: int(os.environ["GW_READYZ"]) if "GW_READYZ" in os.environ else B + 90 + n for n in GW}
TENANT = os.environ.get("KL_TENANT", "kill")
USER, PW = f"{TENANT}.app", os.environ.get("KL_PASSWORD", "sprig-live-app")
RUN = str(int(time.time()))  # Redis keys of this run: r:<run>:<writer>:<n>
stop = threading.Event()
lock = threading.Lock()
acked_sql, acked_red = set(), set()
uncertain_sql, uncertain_red = set(), set()
last_ok = {"sql": (0.0, 0), "red": (0.0, 0)}  # (time, gateway) of the latest acknowledged write
ok_times = {"sql": [], "red": []}
errors = {}


def note_err(kind, msg):
    with lock:
        k = f"{kind}: {msg[:90]}"
        errors[k] = errors.get(k, 0) + 1


class My:
    def __init__(self, n):
        try:
            self.c, self.caps = probe.login(HOST[n], MY[n], USER, PW, "kl", probe.DEFAULT, True)
        except SystemExit as e:  # probe reports a refused login this way
            raise ConnectionError(str(e)) from None
        self.c.s.settimeout(15)

    def q(self, sql):
        """-> ('ok'|'err'|'rows', payload)"""
        self.c.write(b"\x03" + sql.encode(), reset=True)
        sink = []
        probe.response(self.c, self.caps, "", sink=sink)
        f = sink[0]
        if f[0] == 0x00:
            return "ok", None
        if f[0] == 0xFF:
            code = struct.unpack_from("<H", f, 1)[0]
            return "err", (code, f[9:].decode(errors="replace"))
        rows = []
        ncol, _ = probe.lenenc_int(f, 0)
        for p in sink[1 + ncol:]:
            if p[0] == 0xFE and len(p) < 9:
                continue
            if p[0] == 0xFF:
                return "err", (struct.unpack_from("<H", p, 1)[0], p[9:].decode(errors="replace"))
            # text row: lenenc strings
            i, row = 0, []
            while i < len(p):
                if p[i] == 0xFB:
                    row.append(None); i += 1; continue
                ln, i = probe.lenenc_int(p, i)
                row.append(p[i:i + ln].decode()); i += ln
            rows.append(row)
        return "rows", rows

    def close(self):
        with contextlib.suppress(OSError):
            self.c.s.close()


class Red:
    def __init__(self, n):
        self.s = socket.create_connection((HOST[n], RE[n]), timeout=15)
        self.f = self.s.makefile("rb")
        r = self.cmd("AUTH", USER, PW)
        if isinstance(r, Exception):
            raise r

    def cmd(self, *a):
        out = b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x.encode()), x.encode()) for x in a)
        self.s.sendall(out)
        return self.read()

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            return d[:-2].decode()
        if t == b"*":
            n = int(rest)
            return [self.read() for _ in range(max(n, 0))]
        raise ConnectionError(f"bad reply {line!r}")

    def close(self):
        with contextlib.suppress(OSError):
            self.s.close()


def writer(kind, w):
    n, gi, conn = 0, random.randrange(3), None
    while not stop.is_set():
        g = GW[gi % 3]
        try:
            if conn is None:
                conn = (My if kind == "sql" else Red)(g)
            n += 1
            if kind == "sql":
                ident = w * 1_000_000_000 + n
                st, p = conn.q(f"INSERT INTO t VALUES ({ident}, {w}, {n})")
                ok = st == "ok"
                if not ok:
                    note_err("sql", f"{p[0]} {p[1]}")
            else:
                ident = f"r:{RUN}:{w}:{n}"
                r = conn.cmd("SET", ident, str(n))
                ok = r == "OK"
                if not ok:
                    note_err("red", str(r))
            with lock:
                if ok:
                    (acked_sql if kind == "sql" else acked_red).add(ident)
                    now = time.time()
                    last_ok[kind] = (now, g)
                    ok_times[kind].append((now, g))
            if not ok:
                # Refused (not the leader, or the leader changed): nothing was written. Next gateway.
                conn.close(); conn = None; gi += 1; time.sleep(0.05)
        except Exception as e:  # connection lost mid-statement: the outcome is unknown
            if conn is not None:
                with lock:
                    if kind == "sql":
                        uncertain_sql.add(w * 1_000_000_000 + n)
                    else:
                        uncertain_red.add(f"r:{RUN}:{w}:{n}")
                conn.close()
            note_err(kind, type(e).__name__ + " " + str(e))
            conn = None; gi += 1; time.sleep(0.05)
    if conn is not None:
        conn.close()


def nr(*a, wait=True):
    # `start` waits for the gateway to be ready; the loop below waits itself.
    if not wait and a and a[0] == "start":
        return subprocess.Popen(["bash", NR, *a], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subprocess.run(["bash", NR, *a], capture_output=True, text=True)


def leader_of(tenant_id):
    """The gateway (1-3) leading the tenant's unit, as the gateways' engine report says."""
    for n in GW:
        try:
            with urllib.request.urlopen(f"http://{HOST[n]}:{HT[n]}/admin/v1/engine/units", timeout=3) as r:
                for u in json.load(r).get("units", []):
                    if u.get("unit") == tenant_id and u.get("leading"):
                        return int(u.get("leader") or n)
        except Exception:
            pass
    return None


def tenant_id():
    try:
        out = subprocess.run(["bash", NR, "ctl", "-o", "json", "tenant", "show", TENANT],
                             capture_output=True, text=True, timeout=20).stdout
        d = json.loads(out)
        return int(d.get("tenant_id") or d.get("id"))
    except Exception:
        return None


def ready(g):
    import urllib.request
    try:
        urllib.request.urlopen(f"http://{HOST[g]}:{RZ[g]}/readyz", timeout=1)
        return True
    except Exception:
        return False


def setup():
    """Makes kl.t empty, on whichever gateway leads the tenant's unit (a write commits there)."""
    def run(c, caps, q):
        c.write(b"\x03" + q.encode(), reset=True)
        sink = []
        probe.response(c, caps, "", sink=sink)
        return sink[0]
    for _ in range(30):
        for g in GW:
            try:
                c, caps = probe.login(HOST[g], MY[g], USER, PW, "", [x for x in probe.DEFAULT if x != "CONNECT_WITH_DB"], True)
            except BaseException:
                continue
            run(c, caps, "CREATE DATABASE IF NOT EXISTS kl")
            run(c, caps, "CREATE TABLE IF NOT EXISTS kl.t (id BIGINT PRIMARY KEY, w INT, v BIGINT)")
            run(c, caps, "DELETE FROM kl.t WHERE id = -1")
            if run(c, caps, "INSERT INTO kl.t VALUES (-1, 0, 0)")[0] == 0:
                r = run(c, caps, "DELETE FROM kl.t")
                print(f"setup on gw{g}: DELETE FROM kl.t ->", "ok" if r[0] == 0 else r[9:])
                return
        time.sleep(1)
    raise SystemExit("setup: no gateway accepted a write")


TID = None


def main():
    global TID
    TID = tenant_id()
    setup()
    ths = [threading.Thread(target=writer, args=("sql", w), daemon=True) for w in range(1, SQLW + 1)]
    ths += [threading.Thread(target=writer, args=("red", w), daemon=True) for w in range(1, REDW + 1)]
    for t in ths:
        t.start()
    time.sleep(3)
    rto = {"sql": [], "red": []}
    for rd in range(1, ROUNDS + 1):
        # The leader: the gateway of the latest acknowledged write (both front ends write the same tenant).
        with lock:
            leader = max(last_ok.values())[1]
        # Sessions are forwarded to the leader, so a write that succeeded on a
        # gateway doesn't say it leads: ask the gateways which one does.
        if TID is not None:
            leader = leader_of(TID) or leader
        if not leader:
            print("no acknowledged write yet; errors:", errors); break
        t0 = time.time()
        nr("kill", f"gw{leader}")
        # RTO: the first acknowledged write after the kill, per front end.
        deadline = t0 + 30
        got = {}
        while time.time() < deadline and len(got) < 2:
            with lock:
                for k in ("sql", "red"):
                    if k not in got:
                        after = [t for (t, g) in ok_times[k][-5000:] if t > t0 and g != leader]
                        if after:
                            got[k] = min(after) - t0
            time.sleep(0.02)
        for k in ("sql", "red"):
            rto[k].append(got.get(k, 99.0))
        nr("start", f"gw{leader}", wait=False)
        while not ready(leader):
            time.sleep(0.2)
        time.sleep(2.0)  # let it catch up before the next kill
        with lock:
            ns, nre = len(acked_sql), len(acked_red)
        print(f"round {rd}: killed gw{leader}; rto sql {got.get('sql', 99):.2f}s redis {got.get('red', 99):.2f}s; acked sql {ns} redis {nre}", flush=True)
    stop.set()
    for t in ths:
        t.join(20)
    time.sleep(3)
    sys.exit(0 if verify(rto) else 1)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else float("nan")


def verify(rto):
    print("errors seen (refusals/reconnects):")
    for k, v in sorted(errors.items(), key=lambda x: -x[1])[:12]:
        print(f"  {v:6d}  {k}")
    # RPO on the leader first (followers' reads can trail; see the report), then every
    # replica after a restart of all three gateways.
    sql_sets, red_sets = read_all()
    with lock:
        leader = max(last_ok.values())[1]
    print(f"on the leader gw{leader} before the restart:")
    report({leader: sql_sets[leader]}, {leader: red_sets[leader]}, None)
    for g in GW:
        nr("stop", f"gw{g}")
    for g in GW:
        nr("start", f"gw{g}", wait=False)
    for g in GW:
        while not ready(g):
            time.sleep(0.5)
    time.sleep(5)
    print("every replica after restarting all three:")
    prev = None
    for _ in range(30):
        sql_sets, red_sets = read_all()
        sig = [len(x) if x is not None else -1 for x in list(sql_sets.values()) + list(red_sets.values())]
        if sig == prev and len(set(sig[:3])) == 1 and len(set(sig[3:])) == 1:
            break
        prev = sig
        time.sleep(2)
    return report(sql_sets, red_sets, rto)


def read_all():
    sql_sets, red_sets = {}, {}
    for g in GW:
        try:
            c = My(g)
            st, rows = c.q("SELECT id FROM t")
            sql_sets[g] = {int(r[0]) for r in rows} if st == "rows" else None
            if st != "rows":
                print(f"gw{g} select: {rows}")
            c.close()
        except BaseException as e:
            print(f"gw{g} sql read failed: {e!r}"); sql_sets[g] = None
        try:
            r = Red(g)
            keys, cur = set(), "0"
            while True:
                rep = r.cmd("SCAN", cur, "MATCH", f"r:{RUN}:*", "COUNT", "1000")
                if isinstance(rep, Exception):
                    raise rep
                cur, ks = rep
                keys.update(ks)
                if cur == "0":
                    break
            red_sets[g] = keys
            r.close()
        except BaseException as e:
            print(f"gw{g} redis read failed: {e!r}"); red_sets[g] = None
    return sql_sets, red_sets


def report(sql_sets, red_sets, rto):
    """Prints RPO (and RTO); returns whether every replica holds every acknowledged write."""
    good = True
    for name, acked, unc, sets in (("SQL", acked_sql, uncertain_sql, sql_sets), ("Redis", acked_red, uncertain_red, red_sets)):
        print(f"{name}: acknowledged {len(acked)}, outcome unknown {len(unc)}")
        for g, s in sets.items():
            if s is None:
                print(f"  gw{g}: not read"); good = False; continue
            lost = acked - s
            good = good and not lost
            extra = s - acked - unc
            print(f"  gw{g}: {len(s)} rows; lost (acked, missing) {len(lost)}; present though never sent or refused {len(extra)}")
        ok_sets = [s for s in sets.values() if s is not None]
        if ok_sets:
            print(f"  replicas equal: {all(s == ok_sets[0] for s in ok_sets)}")
    if rto is None:
        return good
    for k, name in (("sql", "SQL"), ("red", "Redis")):
        xs = rto[k]
        print(f"RTO {name}: n {len(xs)} p50 {pct(xs, 50):.2f}s p99 {pct(xs, 99):.2f}s max {max(xs) if xs else 0:.2f}s")
    print("RESULT", "no acknowledged write lost" if good else "LOST acknowledged writes")
    return good


if __name__ == "__main__":
    main()
