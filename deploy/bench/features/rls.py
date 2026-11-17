#!/usr/bin/env python3
"""Feature probe: row-level security. A policy on a table lets a bound user
read only the rows it allows (region 'eu' of 'eu' and 'us'), enforced by the
server for an ordinary query that doesn't mention the policy."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import Fail, NotOffered, check


def sprig(t):
    n = f"rls{t.stamp}"
    t.mysql(f"CREATE TABLE {n} (id INT PRIMARY KEY, region VARCHAR(4)); INSERT INTO {n} VALUES (1,'eu'),(2,'us');"
            f"CREATE POLICY only_eu ON {n} FOR SELECT USING (region = 'eu');"
            f"ALTER TABLE {n} ENABLE ROW LEVEL SECURITY; ALTER TABLE {n} FORCE ROW LEVEL SECURITY;")
    try:
        r = t.mysql(f"SELECT id FROM {n} ORDER BY id")
        check(r == [["1"]], f"bound user saw {r}")
    finally:
        # Dropping a FORCEd table fails with 39366 on the current build, so un-force it first.
        t.mysql(f"ALTER TABLE {n} NO FORCE ROW LEVEL SECURITY; DROP TABLE {n}")
    return "CREATE POLICY + FORCE ROW LEVEL SECURITY: the table's manager sees 1 of 2 rows"


def pg(t):
    n, role = f"rls{t.stamp}", f"rls{t.stamp}u"
    t.psql(f"CREATE TABLE {n} (id INT PRIMARY KEY, region TEXT); INSERT INTO {n} VALUES (1,'eu'),(2,'us');"
           f"CREATE POLICY only_eu ON {n} FOR SELECT USING (region = 'eu'); ALTER TABLE {n} ENABLE ROW LEVEL SECURITY;"
           f"CREATE ROLE {role} LOGIN PASSWORD 'pw'; GRANT SELECT ON {n} TO {role};")
    try:
        r = t.psql(f"SELECT id FROM {n} ORDER BY id", user=role, password="pw")
        check(r == [["1"]], f"bound user saw {r}")
    finally:
        t.psql(f"DROP TABLE {n}; DROP ROLE {role}")
    return "CREATE POLICY + ENABLE ROW LEVEL SECURITY: a plain role sees 1 of 2 rows"


def mysql(t):
    try:
        t.mysql("CREATE POLICY p ON nosuch USING (1=1)")
    except Fail as e:
        raise NotOffered(f"no row policies; CREATE POLICY fails: {e}")
    raise Fail("CREATE POLICY unexpectedly accepted")


def surreal(t):
    t.surreal("""DEFINE TABLE rdoc SCHEMALESS PERMISSIONS FOR select WHERE region = $auth.region FOR create, update, delete NONE;
DEFINE ACCESS racc ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM ruser WHERE name = $name AND crypto::argon2::compare(pass, $pass)) DURATION FOR SESSION 1h;
CREATE ruser:u1 SET name = 'u1', pass = crypto::argon2::generate('pw'), region = 'eu';
CREATE rdoc:1 SET region = 'eu'; CREATE rdoc:2 SET region = 'us';""")
    try:
        st, res = t.http("POST", f"http://{t.c['http']}/signin",
                         {"ns": t.c["ns"], "db": t.c["db"], "ac": "racc", "name": "u1", "pass": "pw"},
                         {"Accept": "application/json", "Content-Type": "application/json"})
        check(st == 200 and isinstance(res, dict) and res.get("token"), f"signin {st} {str(res)[:200]}")
        r = t.surreal("SELECT VALUE meta::id(id) FROM rdoc;", token=res["token"])
        check(r[0] == [1], f"record user saw {r[0]}")
    finally:
        t.surreal("REMOVE TABLE rdoc; REMOVE TABLE ruser; REMOVE ACCESS racc ON DATABASE;")
    return "table PERMISSIONS FOR select WHERE ... $auth: a record user sees 1 of 2 rows"


def none(name, what):
    def f(t):
        raise NotOffered(f"{name} has no row-level policies ({what})")
    return f


PROBES = {"sprig": sprig, "pg": pg, "mysql": mysql, "surreal": surreal,
          "mongo": none("MongoDB", "access control stops at collections and views"),
          "arango": none("ArangoDB", "permissions stop at collections"),
          "couchbase": none("Couchbase", "RBAC stops at collections")}
if __name__ == "__main__":
    lib.run("rls", "multi", PROBES, "Row-level security")
