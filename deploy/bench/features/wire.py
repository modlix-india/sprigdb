#!/usr/bin/env python3
"""Feature probes: the MySQL, MongoDB, Redis and Bolt wire protocols. Each
handshake (MySQL server greeting, MongoDB OP_MSG hello, Redis PING, Bolt
version negotiation) is tried on every port the product's entry lists under
"ports"; the protocol counts when any port answers it.

  features/wire.py --conf targets.json --product surrealdb --protocol mysql"""
import sys

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import NotOffered, NotProbed, check

PROTO = {"mysql": (lib.wire_mysql, "sql", "MySQL protocol"), "mongo": (lib.wire_mongo, "doc", "MongoDB protocol"),
         "redis": (lib.wire_redis, "kv", "Redis protocol"), "bolt": (lib.wire_bolt, "graph", "Bolt protocol")}


def probe(proto):
    fn = PROTO[proto][0]

    def f(t):
        if t.c.get("unprobed", {}).get(proto):
            raise NotProbed(t.c["unprobed"][proto])
        ports = t.c.get("ports") or []
        if not ports:
            raise NotProbed("no ports listed")
        tried = []
        for hp in ports:
            h, p = hp.rsplit(":", 1)
            try:
                if fn(h, int(p)):
                    return f"{proto} handshake answered on {hp}"
                tried.append(f"{hp} no")
            except OSError as e:
                tried.append(f"{hp} {type(e).__name__}")
        raise NotOffered(f"no {proto} handshake on any listener ({', '.join(tried)})")
    return f


if __name__ == "__main__":
    i = sys.argv.index("--protocol")
    proto = sys.argv[i + 1]
    del sys.argv[i:i + 2]
    check(proto in PROTO, f"unknown protocol {proto}")
    fn = probe(proto)
    lib.run(f"wire_{proto}", PROTO[proto][1], {k: fn for k in ("mysql", "pg", "mongo", "surreal", "arango", "couchbase", "sprig")}, PROTO[proto][2])
