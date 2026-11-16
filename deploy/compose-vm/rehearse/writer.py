#!/usr/bin/env python3
"""A client writing through the load balancer during a rehearsal.

One connection, kept open (TLS, the deployment CA verified, the host name
checked); a new one only after the old one failed. Every attempt is one
JSON line: id, ok, MySQL error code, error text, start and end time. A row
is acknowledged when its INSERT returned success.

  writer.py <host> <port> <login> <password> <ca> <table> <log> <stopfile> [interval s] [reconnect every n]

With n, the connection is also replaced every n writes (new connections go
wherever the load balancer, or the name it is reached by, sends them).

Needs PyMySQL (MIT).
"""
import json
import os
import sys
import time

import pymysql

host, port, login, password, ca, table, log, stop = sys.argv[1:9]
interval = float(sys.argv[9]) if len(sys.argv) > 9 else 0.05
every = int(sys.argv[10]) if len(sys.argv) > 10 else 0

conn = None
i = 0
with open(log, "a", encoding="utf-8") as f:
    while not os.path.exists(stop):
        i += 1
        t0 = time.time()
        ok, code, err = False, None, ""
        try:
            if conn is None:
                conn = pymysql.connect(host=host, port=int(port), user=login, password=password,
                                       ssl_ca=ca, ssl_verify_cert=True, ssl_verify_identity=True,
                                       connect_timeout=3, read_timeout=30, write_timeout=30,
                                       autocommit=True)
            with conn.cursor() as c:
                c.execute(f"INSERT INTO {table} (id) VALUES (%s)", (i,))
            ok = True
        except pymysql.MySQLError as e:
            code = e.args[0] if e.args and isinstance(e.args[0], int) else None
            # PyMySQL reads the code as a signed 16-bit number: SprigDB's
            # 39xxx codes come out negative.
            if code is not None and code < 0:
                code += 65536
            err = str(e)[:200]
            # A connection-level error: start a new connection next time.
            if code is None or code >= 2000:
                try:
                    if conn is not None:
                        conn.close()
                except Exception:  # noqa: BLE001 - closing a broken connection
                    pass
                conn = None
        except Exception as e:  # noqa: BLE001 - reported, then a new connection
            err = repr(e)[:200]
            conn = None
        f.write(json.dumps({"id": i, "ok": ok, "code": code, "err": err,
                            "t0": round(t0, 3), "t1": round(time.time(), 3)}) + "\n")
        f.flush()
        if every and conn is not None and i % every == 0:
            conn.close()
            conn = None
        time.sleep(interval)
if conn is not None:
    conn.close()
