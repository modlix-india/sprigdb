"""Shared code for the feature probes in deploy/bench/features.

Each probe script checks one feature on one product, through the product's
own interface, and records a measurement: features / feature-<name> / probe,
value 1 (it ran and passed), 0 (it ran and failed, or the product has no such
construct), or no value (not probed, with the reason). A feature counts as
present only when its probe ran and passed.

  features/<feature>.py --conf targets.json --product surrealdb

targets.json names each product's family and endpoints (see README.md):
  {"surrealdb": {"family": "surreal", "label": "SurrealDB", "version": "2.3.10",
                 "http": "127.0.0.1:47501", "user": "root", "password": "...",
                 "ns": "bench", "db": "bench"}, ...}
Families: mysql, pg, mongo, surreal, arango, couchbase, sprig (SprigDB: "mysql"
and "mongo" endpoints, plus optional "redis" and "bolt").
Standard library plus the mysql, psql and mongosh command-line clients.
"""
import argparse
import base64
import json
import os
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import result  # noqa: E402

PSQL = os.environ.get("BENCH_PSQL", "psql")


class NotOffered(Exception):
    """The product has no construct for the feature (the probe still ran)."""


class NotProbed(Exception):
    """The probe could not run here (needs more nodes, a client library...)."""


class Fail(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise Fail(msg)


class Target:
    def __init__(self, name, conf):
        self.name, self.c = name, conf
        self.family = conf["family"]
        self.stamp = str(int(time.time()))

    def hp(self, key):
        v = self.c.get(key)
        if not v:
            raise NotProbed(f"no {key} endpoint configured")
        h, p = v.rsplit(":", 1)
        return h, int(p)

    # SQL over the MySQL protocol (MySQL, SprigDB).
    def mysql(self, sql, user=None, password=None, db=None):
        h, p = self.hp("mysql")
        cmd = ["mysql", "-h", h, "-P", str(p), "-u", user or self.c["user"], "--ssl-mode=" + self.c.get("ssl_mode", "PREFERRED"), "-N", "-B"]
        db = db or self.c.get("db")
        if db:
            cmd += ["-D", db]
        env = dict(os.environ, MYSQL_PWD=password or self.c.get("password", ""))
        r = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True, env=env, timeout=120)
        if r.returncode:
            raise Fail(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else f"mysql exit {r.returncode}")
        return [l.split("\t") for l in r.stdout.splitlines()]

    def psql(self, sql, user=None, password=None):
        h, p = self.hp("pg")
        conn = f"host={h} port={p} user={user or self.c['user']} dbname={self.c.get('db', 'postgres')}"
        env = dict(os.environ, PGPASSWORD=password or self.c.get("password", ""))
        r = subprocess.run([PSQL, conn, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True,
                           env=env, timeout=120)
        if r.returncode:
            raise Fail(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else f"psql exit {r.returncode}")
        return [l.split("|") for l in r.stdout.splitlines()]

    def mongo(self, js):
        h, p = self.hp("mongo")
        cred = ""
        if self.c.get("mongo_user"):
            cred = f"{self.c['mongo_user']}:{self.c['mongo_password']}@"
        uri = f"mongodb://{cred}{h}:{p}/{self.c.get('mongo_db', 'bench')}?authSource=admin&directConnection=true"
        r = subprocess.run(["mongosh", "--quiet", uri, "--eval", js], capture_output=True, text=True, timeout=120)
        out = (r.stdout + r.stderr).strip()
        if r.returncode:
            raise Fail(out.splitlines()[-1] if out else f"mongosh exit {r.returncode}")
        return r.stdout.strip()

    def http(self, method, url, body=None, headers=None, auth=None, timeout=60):
        data = None
        if body is not None:
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        if auth:
            req.add_header("Authorization", "Basic " + base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                txt = r.read().decode()
                try:
                    return r.status, (json.loads(txt) if txt.strip()[:1] in "[{" else txt)
                except ValueError:
                    return r.status, txt
        except urllib.error.HTTPError as e:
            txt = e.read().decode(errors="replace")
            try:
                return e.code, json.loads(txt)
            except ValueError:
                return e.code, txt

    def surreal(self, sql, token=None, allow_err=False):
        h, p = self.hp("http")
        hdr = {"Accept": "application/json", "surreal-ns": self.c["ns"], "surreal-db": self.c["db"]}
        auth = None
        if token:
            hdr["Authorization"] = "Bearer " + token
        else:
            auth = (self.c["user"], self.c["password"])
        st, res = self.http("POST", f"http://{h}:{p}/sql", sql, hdr, auth)
        if st != 200 or not isinstance(res, list):
            info = res.get("information", res) if isinstance(res, dict) else res
            raise Fail(f"HTTP {st}: {str(info)[:300]}")
        for r in res:
            if r.get("status") != "OK" and not allow_err:
                raise Fail(str(r.get("result"))[:200])
        return [r.get("result") for r in res]

    def arango(self, path, body=None, method="POST"):
        h, p = self.hp("http")
        st, res = self.http(method, f"http://{h}:{p}/_db/{self.c.get('db', '_system')}{path}", body,
                            {"Content-Type": "application/json"}, (self.c["user"], self.c["password"]))
        if st >= 400 or (isinstance(res, dict) and res.get("error")):
            raise Fail(f"HTTP {st}: {str(res)[:240]}")
        return res

    def aql(self, q, bind=None):
        return self.arango("/_api/cursor", {"query": q, "bindVars": bind or {}})["result"]

    def n1ql(self, stmt, **extra):
        h, p = self.hp("query")
        body = {"statement": stmt, **extra}
        st, res = self.http("POST", f"http://{h}:{p}/query/service", body, {"Content-Type": "application/json"},
                            (self.c["user"], self.c["password"]))
        if st >= 400 or res.get("status") != "success":
            raise Fail(f"HTTP {st}: {str(res.get('errors', res))[:240]}")
        return res.get("results", [])


# Wire handshakes, used by the protocol probes.
def wire_mysql(h, p):
    with socket.create_connection((h, p), timeout=5) as s:
        s.settimeout(5)
        hdr = s.recv(4)
        if len(hdr) < 4:
            return False
        n = hdr[0] | hdr[1] << 8 | hdr[2] << 16
        body = s.recv(n)
        return len(body) > 1 and body[0] == 10  # protocol version 10 greeting


def wire_mongo(h, p):
    doc = b'\x10hello\x00\x01\x00\x00\x00\x02$db\x00\x06\x00\x00\x00admin\x00\x00'
    doc = struct.pack("<i", len(doc) + 4) + doc
    body = struct.pack("<I", 0) + b"\x00" + doc
    msg = struct.pack("<iiii", 16 + len(body), 1, 0, 2013) + body
    with socket.create_connection((h, p), timeout=5) as s:
        s.settimeout(5)
        s.sendall(msg)
        hdr = s.recv(16)
        return len(hdr) == 16 and struct.unpack("<i", hdr[12:16])[0] == 2013


def wire_redis(h, p):
    with socket.create_connection((h, p), timeout=5) as s:
        s.settimeout(5)
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        r = s.recv(64)
        return r.startswith(b"+PONG") or r.startswith(b"-NOAUTH") or r.startswith(b"-ERR auth")


def wire_bolt(h, p):
    with socket.create_connection((h, p), timeout=5) as s:
        s.settimeout(5)
        s.sendall(b"\x60\x60\xb0\x17" + b"\x00\x08\x08\x05" + b"\x00\x00\x04\x04" + b"\x00\x00\x00\x03" + b"\x00" * 4)
        r = s.recv(4)
        # The server answers with one of the offered versions: 00 00 minor major.
        return len(r) == 4 and r[0] == 0 and r[1] == 0 and r[3] in (3, 4, 5) and r[2] <= 8


def run(feature, model, probes, title):
    """CLI entry: runs the family's probe for one product and records it."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", required=True)
    ap.add_argument("--product", required=True)
    ap.add_argument("--out")
    ap.add_argument("--run")
    a = ap.parse_args()
    conf = json.load(open(a.conf))[a.product]
    t = Target(a.product, conf)
    fn = probes.get(t.family)
    started = time.time()
    status, value, detail = "not probed", None, f"no probe written for family {t.family}"
    if fn:
        try:
            detail = fn(t) or "passed"
            status, value = "pass", 1
        except NotOffered as e:
            status, value, detail = "not offered", 0, str(e)
        except NotProbed as e:
            status, value, detail = "not probed", None, str(e)
        except Fail as e:
            status, value, detail = "fail", 0, str(e)
        except Exception as e:  # noqa: BLE001 - a crash in the probe is a failed probe, recorded
            status, value, detail = "fail", 0, f"{type(e).__name__}: {e}"
    label = conf.get("label", a.product)
    print(f"{feature:<16} {label:<12} {status:<12} {detail[:160]}")
    rec = {
        "model": model, "op": "features", "workload": f"feature-{feature}", "metric": "probe",
        "value": value, "unit": "pass", "better": "higher", "status": status,
        "product": label, "product_version": conf.get("version", ""),
        "competitor": conf.get("competitor", label),
        "title": f"{title}: {status}", "notes": detail[:600],
        "config": {"family": t.family, "seconds": round(time.time() - started, 1)},
        "source": f"deploy/bench/features/{feature}.py",
        "command": f"features/{feature}.py --conf <targets> --product {a.product}",
        "run": a.run or f"feature-{feature}",
    }
    if conf.get("build"):
        rec["build"] = conf["build"]
    result.write(rec, a.out)
