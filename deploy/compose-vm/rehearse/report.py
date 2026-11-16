#!/usr/bin/env python3
"""Summary of writer.py's log, checked against the rows in the table.

  report.py <log> [ids-in-table file or -] [event time ...]

Prints the attempts, the failures by MySQL error code, and the rows that
were acknowledged but are missing from the table (lost). With event times
(Unix seconds), also the failures from one second before each event to
the next event (or the end).
"""
import collections
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
acked = {r["id"] for r in rows if r["ok"]}
fails = [r for r in rows if not r["ok"]]
print(f"writes tried {len(rows)}, acknowledged {len(acked)}, failed {len(fails)}; codes: "
      + (", ".join(f"{c} x{n}" for c, n in collections.Counter(r["code"] for r in fails).most_common()) or "none"))
for text, n in collections.Counter(r["err"][:110] for r in fails).most_common(5):
    print(f"   {n} x {text}")
events = [float(t) for t in sys.argv[3:]]
for k, t in enumerate(events):
    end = events[k + 1] if k + 1 < len(events) else float("inf")
    near = [r for r in rows if t - 1 <= r["t0"] < end]
    bad = [r for r in near if not r["ok"]]
    span = f", over {max(r['t1'] for r in bad) - min(r['t0'] for r in bad):.2f} s" if bad else ""
    print(f"event {k + 1} at {t:.3f}: {len(near)} writes, {len(bad)} failed{span}"
          + (" (" + ", ".join(f"{c} x{n}" for c, n in collections.Counter(r['code'] for r in bad).items()) + ")" if bad else ""))
if len(sys.argv) > 2 and sys.argv[2] != "-":
    table = {int(x) for x in open(sys.argv[2], encoding="utf-8").read().split() if x.isdigit()}
    lost = sorted(acked - table)
    extra = sorted(table - acked)
    print(f"in table {len(table)}; acknowledged but missing (lost): {len(lost)} {lost[:10]}; "
          f"in table but not acknowledged: {len(extra)} {extra[:10]}")
    sys.exit(1 if lost else 0)
