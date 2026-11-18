#!/usr/bin/env python3
"""The E0 speed runner: sysbench, memtier, redis-benchmark and footprints
against one arm of e0/stack.sh, with the CPU of every system-under-test
container counted over each run (cgroup v2 usage, read by the e0bench-cgmon
sidecar) and the machine-wide /proc/stat beside it. Writes results in the
market format (deploy/bench/lib/result.py), one file per run plus the median
of the runs (deploy/bench/lib/summarize.py), with the same model / op /
workload / variant / metric names as the market harness, so they pair with the
round-1 baseline.

Run it through e0/bench.sh, which sources env.sh:

  e0/bench.sh calib --arm redis
  e0/bench.sh sql   --arm mysql [--tests oltp_point_select] [--threads "1 16 64"] [--time 60] [--warmup 30] [--runs 3] [--no-prepare]
  e0/bench.sh kv    --arm redis [--pipelines "1 16"] [--rb-commands "set incr lpush get"] [--rb-requests 100000] [--no-preload]

CPU per operation: the SUT containers' cgroup CPU over the run, divided by
the operations the client counted (E0_CPU=cgroup, the laptop default), or the
whole machine's busy CPU from /proc/stat (E0_CPU=vm, for a dedicated
benchmark VM, as the market round measured). Both are kept in the config.
"""
import argparse
import datetime
import json
import os
import re
import shlex
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
E0_DIR = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(os.path.dirname(E0_DIR), "lib"))
import result  # noqa: E402

ENV = os.environ
DOCKER = shlex.split(ENV.get("E0_DOCKER", "docker"))
NET = ENV.get("E0_NET", "e0bench")
IMG_RUN = ENV.get("IMG_RUN", "e0bench-run:13.7")
IMG_REDIS = ENV.get("IMG_REDIS", "redis:7.4.11")
IMG_MEMTIER = ENV.get("IMG_MEMTIER", "redislabs/memtier_benchmark:2.1.4")
CLIENT_CPUS = ENV.get("E0_CLIENT_CPUS", "12-15")
SUT_CPUS = ENV.get("E0_SUT_CPUS", "8-11")
MEM = ENV.get("E0_MEM", "768m")
WORK = ENV.get("E0_WORK", os.path.join(E0_DIR, "..", "..", "..", "target", "agent-e0-bench"))
LOGS = ENV.get("BENCH_LOGS", os.path.join(WORK, "logs"))
APP_USER, APP_PW = ENV.get("E0_APP_USER", "sbtest"), ENV.get("E0_APP_PW", "e0bench-app")
TABLES, TABLE_SIZE = int(ENV.get("E0_TABLES", "10")), int(ENV.get("E0_TABLE_SIZE", "100000"))
CPU_MODE = ENV.get("E0_CPU", "cgroup")
INNODB = "innodb_flush_log_at_trx_commit=1, sync_binlog=1"


def stamp(name):
    try:
        with open(os.path.join(WORK, "bin", name, "STAMP")) as f:
            return f.readline().split(":", 1)[1].strip()
    except OSError:
        return "not recorded"


def arms():
    e0_user = ENV.get("E0_NODE_USER", "bench.app")
    return {
        "mysql": dict(sql=("e0bench-mysql", 3306, APP_USER, APP_PW, "sbtest"), sut=["e0bench-mysql"],
                      data=[("e0bench-mysql", "/var/lib/mysql")], product="MySQL", version="8.4.11",
                      comp="MySQL 8.4", dur=INNODB, build="release image mysql:8.4 (8.4.11)"),
        "mariadb": dict(sql=("e0bench-mariadb", 3306, APP_USER, APP_PW, "sbtest"), sut=["e0bench-mariadb"],
                        data=[("e0bench-mariadb", "/var/lib/mysql")], product="MariaDB", version="11.8.9",
                        comp="MariaDB 11.8 (direct)", dur=INNODB + ", binlog on", build="release image mariadb:11.8 (11.8.9)"),
        "redis": dict(kv=("e0bench-redis", 6379, "", ""), sut=["e0bench-redis"], data=[("e0bench-redis", "/data")],
                      product="Redis", version="7.4.11", comp="Redis 7.4 / 8.1", dur="appendfsync always (fsync per write)",
                      build="release image redis:7.4.11"),
        "sprig": dict(sql=("e0bench-sprig", 3306, "bench.app", APP_PW, "sbtest"), kv=("e0bench-sprig", 6379, "bench.app", APP_PW),
                      sut=["e0bench-sprig", "e0bench-sprigmaria", "e0bench-etcd"], data=[("e0bench-sprigmaria", "/var/lib/mysql")],
                      product="SprigDB", version=stamp("before"), comp="SprigDB",
                      dur="commit per write: MariaDB " + INNODB, build="release " + stamp("before") + ", Linux build in e0bench-build"),
        "e0": dict(sql=("e0bench-node", 3306, e0_user, APP_PW, "sbtest"), kv=("e0bench-node", 6379, e0_user, APP_PW),
                   sut=["e0bench-node"], data=[("e0bench-node", "/data")], product="SprigDB E0", version=stamp("e0"),
                   comp="SprigDB E0", dur=ENV.get("E0_DURABILITY", "commit per write: one fdatasync per commit batch (group commit)"),
                   build="release " + stamp("e0") + ", Linux build in e0bench-build"),
    }


def sh(cmd, check=True, capture=True, timeout=None):
    r = subprocess.run(cmd, capture_output=capture, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {' '.join(cmd)}\n{r.stdout[-2000:] if r.stdout else ''}{r.stderr[-2000:] if r.stderr else ''}")
    return r


def say(*a):
    print(f"e0: {time.strftime('%H:%M:%S')}", *a, file=sys.stderr, flush=True)


# ---- CPU accounting -------------------------------------------------------------------------

class Cpu:
    """Snapshots of the SUT containers' cgroups and of /proc/stat, through the cgmon sidecar."""

    def __init__(self, containers):
        self.names = containers
        self.ids = {}
        for c in containers:
            r = sh(DOCKER + ["inspect", "-f", "{{.Id}}", c], check=False)
            if r.returncode == 0:
                self.ids[c] = r.stdout.strip()
        self.ensure_sidecar()

    @staticmethod
    def ensure_sidecar():
        r = sh(DOCKER + ["inspect", "-f", "{{.State.Running}}", "e0bench-cgmon"], check=False)
        if r.returncode == 0 and r.stdout.strip() == "true":
            return
        sh(DOCKER + ["rm", "-f", "e0bench-cgmon"], check=False)
        sh(DOCKER + ["run", "-d", "--name", "e0bench-cgmon", "--cgroupns", "host", "--cpuset-cpus", CLIENT_CPUS,
                     "-m", "64m", "-v", "/sys/fs/cgroup:/hostcg:ro", "-v", f"{E0_DIR}:/opt/e0:ro", IMG_RUN, "sleep", "infinity"])

    def snap(self):
        r = sh(DOCKER + ["exec", "e0bench-cgmon", "python3", "-I", "/opt/e0/lib/cgsnap.py"] + list(self.ids.values()))
        s = json.loads(r.stdout)
        s["cg"] = {n: s["cg"].get(i) for n, i in self.ids.items()}
        return s

    @staticmethod
    def delta(a, b):
        wall = b["t"] - a["t"]
        per = {}
        for n in a["cg"]:
            if a["cg"][n] and b["cg"].get(n):
                per[n] = {k: b["cg"][n][k] - a["cg"][n][k] for k in ("usage_usec", "user_usec", "system_usec")}
        pa, pb = a["proc"], b["proc"]
        tot = pb["total"] - pa["total"]
        vm_busy_cores = (pb["busy"] - pa["busy"]) / tot * pa["ncpu"] if tot else 0.0
        return {"wall_s": wall, "containers": per, "sut_usec": sum(v["usage_usec"] for v in per.values()),
                "vm_busy_usec": vm_busy_cores * wall * 1e6, "vm_busy_cores": vm_busy_cores, "ncpu": pa["ncpu"]}


def per_op(d, ops, idle=None):
    """CPU microseconds per operation from a delta: (primary, details)."""
    sut = d["sut_usec"] / ops if ops else None
    vm = d["vm_busy_usec"] / ops if ops else None
    det = {"cpu_cgroup_us_per_op": round(sut, 2) if sut else None, "cpu_vm_us_per_op": round(vm, 2) if vm else None,
           "sut_busy_cores": round(d["sut_usec"] / 1e6 / d["wall_s"], 3) if d["wall_s"] else None,
           "vm_busy_cores": round(d["vm_busy_cores"], 3)}
    for n, v in d["containers"].items():
        det[f"cpu_us_per_op[{n}]"] = round(v["usage_usec"] / ops, 2) if ops else None
    if idle:
        bg = sum(idle.get(n, 0) for n in d["containers"]) * d["wall_s"]
        det["cpu_cgroup_us_per_op_minus_idle"] = round((d["sut_usec"] - bg) / ops, 2) if ops else None
    return (vm if CPU_MODE == "vm" else sut), det


def measure_idle(cpu, secs=10):
    a = cpu.snap()
    time.sleep(secs)
    b = cpu.snap()
    d = Cpu.delta(a, b)
    return {n: v["usage_usec"] / d["wall_s"] for n, v in d["containers"].items()}


# ---- results ---------------------------------------------------------------------------------

class Out:
    def __init__(self, arm, a, rid, cmdline):
        self.arm, self.a, self.rid, self.cmd = arm, a, rid, cmdline
        self.logrel = f"e0-{arm}-{rid}"
        self.logdir = os.path.abspath(os.path.join(LOGS, self.logrel))
        os.makedirs(self.logdir, exist_ok=True)
        self.host_note = ENV.get("E0_HOST_NOTE", "")

    def emit(self, model, op, workload, variant, metric, value, unit, better, run=None, config=None, notes=""):
        a = self.a
        rec = {"kind": "measurement", "model": model, "op": op, "workload": workload, "variant": variant, "metric": metric,
               "value": value, "unit": unit, "better": better, "product": a["product"], "product_version": a["version"],
               "competitor": a["comp"], "durability": a["dur"], "build": a["build"],
               "notes": " ".join(x for x in (notes, self.host_note) if x),
               "config": dict(config or {}), "source": f"logs/{self.logrel}", "command": self.cmd}
        tag = f"{result.slug(workload)}-{result.slug(variant or 'none')}-{result.slug(metric)}-{result.slug(a['product'])}-{self.rid}"
        if run is not None:
            rec["config"]["run"] = run
            rec["pair"] = False
            rec["run"] = f"{tag}-run{run}"
        else:
            rec["run"] = tag
        rec["config"] = {k: (str(v) if not isinstance(v, (str, int, float)) else v) for k, v in rec["config"].items()}
        root = result.results_root()
        os.makedirs(root, exist_ok=True)
        result.write(rec, root)

    def medians(self):
        sh([sys.executable, "-I", os.path.join(os.path.dirname(E0_DIR), "lib", "summarize.py"), "--run-id", self.rid])


# ---- clients -----------------------------------------------------------------------------------

def client(image, args, name, entry=None, mount=None, timeout=None):
    """Runs a load generator in a fresh container on the client CPUs; returns (stdout, client cgroup µs)."""
    cmd = DOCKER + ["run", "--rm", "--name", name, "--network", NET, "--cpuset-cpus", CLIENT_CPUS, "--ulimit", "nofile=65536:65536"]
    if mount:
        cmd += ["-v", f"{mount}:/out"]
    script = 'cat /sys/fs/cgroup/cpu.stat | sed -n 1p; echo ===RUN; "$@"; rc=$?; echo ===END; cat /sys/fs/cgroup/cpu.stat | sed -n 1p; exit $rc'
    cmd += ["--entrypoint", "sh", image, "-c", script, "_", entry] + args
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = r.stdout
    m0 = re.match(r"usage_usec (\d+)", out)
    m1 = re.search(r"===END\n.*?usage_usec (\d+)", out, re.S)
    cli = int(m1.group(1)) - int(m0.group(1)) if m0 and m1 else None
    body = out.split("===RUN\n", 1)[-1].split("===END", 1)[0]
    if r.returncode != 0:
        say(f"client {entry} exited {r.returncode}: {r.stderr[-800:]}")
    return body + ("\n" + r.stderr if r.stderr else ""), cli, r.returncode


def write_log(path, text):
    with open(path, "w") as f:
        f.write(text)


# ---- sysbench ---------------------------------------------------------------------------------

def sb_args(a, test, threads, extra):
    h, p, u, pw, db = a["sql"]
    return [test, "--db-driver=mysql", f"--mysql-host={h}", f"--mysql-port={p}", f"--mysql-user={u}", f"--mysql-password={pw}",
            f"--mysql-db={db}", "--mysql-ssl=off", f"--tables={TABLES}", f"--table-size={TABLE_SIZE}", f"--threads={threads}"] + extra


def parse_sysbench(t):
    m = re.search(r"transactions:\s+(\d+)\s+\(([\d.]+) per sec", t)
    if not m:
        return None
    hist = [(float(a), int(b)) for a, b in re.findall(r"^\s*([\d.]+)\s+\|[*\s]*(\d+)\s*$", t, re.M)]
    n = sum(c for _, c in hist)

    def pct(p):
        acc = 0
        for v, c in hist:
            acc += c
            if acc >= p * n:
                return v
        return None

    errs = re.search(r"errors:\s+(\d+)", t)
    return {"tx": int(m.group(1)), "tps": float(m.group(2)), "p50": pct(0.5), "p99": pct(0.99),
            "errors": int(errs.group(1)) if errs else None}


def datadir_kb(a):
    tot = 0
    for c, d in a["data"]:
        r = sh(DOCKER + ["exec", c, "du", "-sk", d], check=False)
        tot += int(r.stdout.split()[0]) if r.returncode == 0 and r.stdout else 0
    return tot


def resident_mib(a):
    r = sh(DOCKER + ["stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"] + a["sut"], check=False)
    tot = 0.0
    for line in r.stdout.splitlines():
        v = line.split()[1]
        num = float(re.match(r"[\d.]+", v).group(0))
        tot += num * (1024 if v.endswith("GiB") else 1 / 1024 if v.endswith("KiB") else 1 if v.endswith("MiB") else 1 / 2**20)
    return round(tot, 1)


def cmd_sql(arm, a, o, args):
    cpu = Cpu(a["sut"])
    idle = measure_idle(cpu)
    write_log(os.path.join(o.logdir, "idle.json"), json.dumps(idle, indent=1))
    ds = f"sysbench-{TABLES}x{TABLE_SIZE // 1000}k" if TABLE_SIZE < 1000000 else f"sysbench-{TABLES}x{TABLE_SIZE // 1000000}M"
    if args.prepare:
        say(f"{arm}: sysbench prepare {TABLES} x {TABLE_SIZE}")
        k0 = datadir_kb(a)
        t0 = time.time()
        out, _, rc = client(IMG_RUN, sb_args(a, "oltp_read_write", 16, []) + ["prepare"], f"e0bench-sb-{os.getpid()}", entry="sysbench")
        load_s = round(time.time() - t0, 1)
        write_log(os.path.join(o.logdir, "prepare.log"), out)
        if rc != 0:
            raise SystemExit(f"{arm}: prepare failed; see {o.logdir}/prepare.log")
        time.sleep(5)
        disk = round((datadir_kb(a) - k0) / 1024, 1)
        rss = resident_mib(a)
        rows = f"{TABLE_SIZE // 1000}k" if TABLE_SIZE < 1000000 else f"{TABLE_SIZE // 1000000}M"
        o.emit("sql", "load", "sysbench-prepare", f"tables={TABLES},rows={rows}", "load_s", load_s, "s", "lower",
               notes="sysbench oltp_read_write prepare, 16 threads (includes the container start, about 1 s).")
        o.emit("sql", "mem", "footprint", f"dataset={ds}", "disk_mb", disk, "MiB", "lower",
               notes="Growth of the data directory during the prepare (logs and binlogs included).")
        o.emit("sql", "mem", "footprint", f"dataset={ds}", "resident_mb", rss, "MiB", "lower",
               notes="Resident memory of the arm's containers after the prepare (docker stats).")
        say(f"{arm}: prepare {load_s} s, disk +{disk} MiB, resident {rss} MiB")
    for test in args.tests.split():
        op = "point" if test == "oltp_point_select" else "complex"
        for th in [int(x) for x in args.threads.split()]:
            variant = f"test={test},threads={th}"
            extra = [f"--time={args.warmup}", "--report-interval=0", "run"]
            out, _, _ = client(IMG_RUN, sb_args(a, test, th, extra), f"e0bench-sb-{os.getpid()}", entry="sysbench")
            write_log(os.path.join(o.logdir, f"{test}-{th}-warmup.log"), out)
            for r in range(1, args.runs + 1):
                extra = [f"--time={args.time}", "--report-interval=0", "--histogram=on", "--percentile=99", "run"]
                s0 = cpu.snap()
                out, cli, rc = client(IMG_RUN, sb_args(a, test, th, extra), f"e0bench-sb-{os.getpid()}", entry="sysbench")
                s1 = cpu.snap()
                write_log(os.path.join(o.logdir, f"{test}-{th}-run{r}.log"), out)
                p = parse_sysbench(out)
                if not p:
                    say(f"{arm}: {variant} run {r}: no result (rc {rc}); see the log")
                    continue
                d = Cpu.delta(s0, s1)
                write_log(os.path.join(o.logdir, f"{test}-{th}-run{r}.cpu.json"), json.dumps(d, indent=1))
                cpu_us, det = per_op(d, p["tx"], idle)
                det["client_us_per_op"] = round(cli / p["tx"], 2) if cli and p["tx"] else None
                cfg = {"tables": TABLES, "table_size": TABLE_SIZE, "seconds": args.time, "errors": p["errors"]}
                o.emit("sql", op, "sysbench", variant, "tps", p["tps"], "transactions/s", "higher", run=r, config=cfg)
                o.emit("sql", op, "sysbench", variant, "p50_ms", p["p50"], "ms", "lower", run=r)
                o.emit("sql", op, "sysbench", variant, "p99_ms", p["p99"], "ms", "lower", run=r)
                o.emit("sql", "eff", "sysbench", variant, "cpu_us_per_tx", round(cpu_us, 2), "CPU µs/transaction", "lower", run=r,
                       config=dict(det, tps=p["tps"], cpu_basis=CPU_MODE),
                       notes=("CPU of the system under test's containers (cgroup v2 usage) over the run, divided by the "
                              "transactions sysbench counted." if CPU_MODE != "vm" else
                              "The SUT machine's whole CPU (/proc/stat) over the run, divided by transactions."))
                say(f"{arm}: {variant} run {r}: {p['tps']:.0f} tps, p99 {p['p99']} ms, {cpu_us:.1f} µs/tx "
                    f"(vm {det['cpu_vm_us_per_op']}, client {det['client_us_per_op']})")
    o.medians()


# ---- Redis protocol -----------------------------------------------------------------------------

def mt_args(a, extra):
    h, p, u, pw = a["kv"]
    auth = [f"--authenticate={u}:{pw}" if u else f"--authenticate={pw}"] if pw else []
    return ["-s", h, "-p", str(p)] + auth + ["--data-size=100", "--hide-histogram"] + extra


def parse_memtier(path):
    t = json.load(open(path))["ALL STATS"]
    tot = t["Totals"]
    pl = tot.get("Percentile Latencies", {})
    ops = tot.get("Count")
    runtime = t.get("Runtime", {})
    dur_s = runtime.get("Total duration", 0) / 1000 if runtime.get("Total duration") else None
    if ops is None and dur_s:
        ops = tot["Ops/sec"] * dur_s
    return {"ops_s": tot["Ops/sec"], "ops": ops, "dur_s": dur_s, "p50": pl.get("p50.00", pl.get("p50")),
            "p99": pl.get("p99.00", pl.get("p99"))}


def cmd_kv(arm, a, o, args):
    cpu = Cpu(a["sut"])
    idle = measure_idle(cpu)
    write_log(os.path.join(o.logdir, "idle.json"), json.dumps(idle, indent=1))
    name = f"e0bench-mt-{os.getpid()}"
    if args.preload:
        k0 = datadir_kb(a)
        t0 = time.time()
        out, _, rc = client(IMG_MEMTIER, mt_args(a, ["--ratio=1:0", "--key-pattern=P:P", "--key-minimum=1", "--key-maximum=1000000",
                                                     "--requests=allkeys", "--clients=10", "--threads=4", "--pipeline=16"]),
                            name, entry="memtier_benchmark", mount=o.logdir)
        load_s = round(time.time() - t0, 1)
        write_log(os.path.join(o.logdir, "preload.log"), out)
        time.sleep(3)
        disk = round((datadir_kb(a) - k0) / 1024, 1)
        rss = resident_mib(a)
        v = "dataset=1M keys x 100 B"
        o.emit("kv", "load", "memtier-preload", "keys=1M,size=100B", "load_s", load_s, "s", "lower",
               notes="memtier SET of keys 1..1M, 100 B values, 40 connections, pipeline 16.")
        o.emit("kv", "mem", "footprint", v, "disk_mb", disk, "MiB", "lower",
               notes="Growth of the data directory during the 1M-key preload.")
        o.emit("kv", "mem", "footprint", v, "resident_mb", rss, "MiB", "lower",
               notes="Resident memory of the arm's containers after the preload (docker stats).")
        say(f"{arm}: preload {load_s} s (rc {rc}), disk +{disk} MiB, resident {rss} MiB")
    for pl in [int(x) for x in args.pipelines.split()]:
        variant = f"pipeline={pl}"
        base = ["--ratio=1:10", "--clients=10", "--threads=5", "--key-pattern=R:R", "--key-maximum=1000000", f"--pipeline={pl}"]
        out, _, _ = client(IMG_MEMTIER, mt_args(a, base + [f"--test-time={args.warmup}"]), name, entry="memtier_benchmark", mount=o.logdir)
        write_log(os.path.join(o.logdir, f"memtier-p{pl}-warmup.log"), out)
        for r in range(1, args.runs + 1):
            js = f"memtier-p{pl}-run{r}.json"
            s0 = cpu.snap()
            out, cli, rc = client(IMG_MEMTIER, mt_args(a, base + [f"--test-time={args.time}", f"--json-out-file=/out/{js}"]),
                                  name, entry="memtier_benchmark", mount=o.logdir)
            s1 = cpu.snap()
            write_log(os.path.join(o.logdir, f"memtier-p{pl}-run{r}.log"), out)
            try:
                m = parse_memtier(os.path.join(o.logdir, js))
            except (OSError, ValueError, KeyError) as e:
                say(f"{arm}: memtier {variant} run {r}: no result ({e}, rc {rc})")
                continue
            d = Cpu.delta(s0, s1)
            cpu_us, det = per_op(d, m["ops"], idle)
            det["client_us_per_op"] = round(cli / m["ops"], 2) if cli and m["ops"] else None
            cfg = {"run_s": args.time, "ratio": "SET:GET 1:10", "clients": 50, "durability": a["dur"]}
            o.emit("kv", "point", "memtier", variant, "ops_per_s", m["ops_s"], "ops/s", "higher", run=r, config=cfg)
            o.emit("kv", "point", "memtier", variant, "p50_ms", m["p50"], "ms", "lower", run=r)
            o.emit("kv", "point", "memtier", variant, "p99_ms", m["p99"], "ms", "lower", run=r)
            o.emit("kv", "eff", "memtier", variant, "cpu_us_per_op", round(cpu_us, 2), "CPU µs/op", "lower", run=r,
                   config=dict(det, ops_per_s=m["ops_s"], cpu_basis=CPU_MODE))
            say(f"{arm}: memtier {variant} run {r}: {m['ops_s']:.0f} ops/s, p99 {m['p99']} ms, {cpu_us:.1f} µs/op")
    h, p, u, pw = a["kv"]
    auth = (["--user", u] if u else []) + (["-a", pw] if pw else [])
    req = args.rb_requests or int(ENV.get(f"E0_RB_REQUESTS_{arm.upper()}", "100000"))
    for c in args.rb_commands.split():
        variant = f"cmd={c.upper()}"
        for r in range(1, args.runs + 1):
            s0 = cpu.snap()
            out, cli, rc = client(IMG_REDIS, ["-h", h, "-p", str(p)] + auth + ["-c", "50", "-n", str(req), "-t", c, "--csv"],
                                  f"e0bench-rb-{os.getpid()}", entry="redis-benchmark")
            s1 = cpu.snap()
            write_log(os.path.join(o.logdir, f"rb-{c}-run{r}.csv"), out)
            m = re.search(r'^"([A-Z]+)[^"]*","([\d.]+)"', out, re.M)
            if not m:
                say(f"{arm}: redis-benchmark {c} run {r}: no result (rc {rc})")
                continue
            rps = float(m.group(2))
            d = Cpu.delta(s0, s1)
            cpu_us, det = per_op(d, req, idle)
            cfg = {"clients": 50, "requests": req, "key": "one key (no -r): hot-key writes", "durability": a["dur"]}
            o.emit("kv", "point", "redis-benchmark", variant, "ops_per_s", rps, "ops/s", "higher", run=r, config=cfg)
            o.emit("kv", "eff", "redis-benchmark", variant, "cpu_us_per_op", round(cpu_us, 2), "CPU µs/op", "lower", run=r,
                   config=dict(det, cpu_basis=CPU_MODE))
            say(f"{arm}: redis-benchmark {c.upper()} run {r}: {rps:.0f} ops/s, {cpu_us:.1f} µs/op")
    o.medians()


# ---- calibration ----------------------------------------------------------------------------------

def cmd_calib(arm, a, o, args):
    """Synced 4 KiB writes (dd oflag=dsync) in a container on the SUT CPUs and memory limit, as the
    market round's calibration; recorded in the log directory and printed."""
    cmd = DOCKER + ["run", "--rm", "--name", f"e0bench-calib-{os.getpid()}", "--cpuset-cpus", SUT_CPUS, "-m", MEM, IMG_RUN, "sh", "-c",
                    "for i in 1 2 3; do dd if=/dev/zero of=/tmp/dsync bs=4k count=2000 oflag=dsync 2>&1 | tail -1; done; rm -f /tmp/dsync"]
    out = sh(cmd).stdout
    write_log(os.path.join(o.logdir, "calib-dsync.txt"), out)
    print(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["sql", "kv", "calib", "idle"])
    ap.add_argument("--arm", required=True, choices=sorted(arms()))
    ap.add_argument("--run-id", default=datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--tests", default="oltp_point_select")
    ap.add_argument("--threads", default="1 16 64")
    ap.add_argument("--pipelines", default="1 16")
    ap.add_argument("--rb-commands", default="set incr lpush get")
    ap.add_argument("--rb-requests", type=int, default=0)
    ap.add_argument("--time", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--no-prepare", dest="prepare", action="store_false")
    ap.add_argument("--no-preload", dest="preload", action="store_false")
    args = ap.parse_args()
    a = arms()[args.arm]
    o = Out(args.arm, a, args.run_id, "e0/bench.sh " + " ".join(shlex.quote(x) for x in sys.argv[1:]))
    if args.cmd == "idle":
        print(json.dumps(measure_idle(Cpu(a["sut"])), indent=1))
        return
    {"sql": cmd_sql, "kv": cmd_kv, "calib": cmd_calib}[args.cmd](args.arm, a, o, args)
    say(f"{args.arm}: {args.cmd} done; logs {o.logdir}")


if __name__ == "__main__":
    main()
