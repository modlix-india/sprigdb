#!/usr/bin/env python3
"""Outcome differential for validations: SprigDB with CREATE VALIDATION
against a server with the equivalent BEFORE trigger + SIGNAL, or a CHECK
constraint.

  vdiff.py SIDE SCRIPT        SIDE: maria | mysql

SCRIPT lines (one statement per line, '#' comments):
  both: SQL      run on both; outcome compared
  sprig: SQL     SprigDB only (setup), must succeed
  oracle: SQL    the server only (setup), must succeed
  check: SQL     run on both; rows compared
  @maria / @mysql before any of these: that side only.

Outcomes: OK; REFUSED (SprigDB 39863, the server's 1644 SIGNAL, 4025 or
3819 CHECK; with SIGNAL the messages must match too); ERR <code>.
"""
import os
import subprocess
import sys

def _ep(var, default):
    """host:port:user:password from a SQLDIFF_* variable (tools/sqldiff/README.md)."""
    return tuple(os.environ.get(var, default).split(':', 3))


# (SprigDB, the server) per side.
SIDES = {
    'maria': (_ep('SQLDIFF_SPRIG_MARIA', '127.0.0.1:45107:sqldiff.app:sprig-live-app'),
              _ep('SQLDIFF_ORACLE_MARIA', '127.0.0.1:45611:root:sqldiff-root')),
    'mysql': (_ep('SQLDIFF_SPRIG_MYSQL', '127.0.0.1:45106:sqldiff.app:sprig-live-app'),
              _ep('SQLDIFF_ORACLE_MYSQL', '127.0.0.1:45621:root:sqldiff-root')),
}
DB = os.environ.get('VD_DB', 'rvalid_diff')


def run(conn, stmts):
    """Runs stmts in one session; per statement (rows, error)."""
    host, port, user, pw = conn
    lines = ['DELIMITER //', f'DROP DATABASE IF EXISTS {DB}//', f'CREATE DATABASE {DB}//', f'USE {DB}//']
    where = {}
    for i, s in enumerate(stmts):
        lines.append(f"SELECT '#{i}'//")
        lines.append(s + '//')
        where[len(lines)] = i
    lines.append(f"SELECT '#end'//")
    lines.append(f'DROP DATABASE IF EXISTS {DB}//')
    p = subprocess.run(['mysql', '--protocol=tcp', '-h', host, '-P', port, '-u', user, '-N', '--batch', '--force',
                        '--connect-timeout=10'], input='\n'.join(lines) + '\n', capture_output=True, text=True,
                       env=dict(os.environ, MYSQL_PWD=pw))
    rows = {i: [] for i in range(len(stmts))}
    errs = {}
    cur = None
    for l in p.stdout.splitlines():
        if l.startswith('#') and (l[1:].isdigit() or l == '#end'):
            cur = int(l[1:]) if l[1:].isdigit() else None
            continue
        if cur is not None:
            rows[cur].append(l)
    for l in p.stderr.splitlines():
        # ERROR 1644 (45000) at line 7: message
        if not l.startswith('ERROR '):
            continue
        head, _, msg = l.partition(': ')
        parts = head.split()
        try:
            code = int(parts[1])
            line = int(parts[-1])
        except (ValueError, IndexError):
            continue
        if line in where:
            errs[where[line]] = (code, msg)
    return [(rows[i], errs.get(i)) for i in range(len(stmts))]


def outcome(err, sprig):
    if err is None:
        return ('OK', None)
    code, msg = err
    if (sprig and code == 39863) or (not sprig and code in (1644, 4025, 3819)):
        return ('REFUSED', msg if (sprig or code == 1644) else None)
    return ('ERR', code)


def main():
    side, script = sys.argv[1], sys.argv[2]
    items = []
    for l in open(script):
        l = l.rstrip('\n')
        if not l.strip() or l.startswith('#'):
            continue
        if l.startswith('@'):
            tag, _, l = l.partition(' ')
            if tag[1:] != side:
                continue
        kind, _, sql = l.partition(': ')
        items.append((kind, sql))
    sprig_stmts = [(k, s) for k, s in items if k in ('both', 'check', 'sprig')]
    oracle_stmts = [(k, s) for k, s in items if k in ('both', 'check', 'oracle')]
    s_res = run(SIDES[side][0], [s for _, s in sprig_stmts])
    o_res = run(SIDES[side][1], [s for _, s in oracle_stmts])
    bad = 0
    for k, s, r in [(k, s, r) for (k, s), r in zip(sprig_stmts, s_res) if k == 'sprig'] + \
                   [(k, s, r) for (k, s), r in zip(oracle_stmts, o_res) if k == 'oracle']:
        if r[1] is not None:
            print(f'SETUP FAILED ({k}): {s}\n   {r[1]}')
            bad += 1
    s_cmp = [r for (k, _), r in zip(sprig_stmts, s_res) if k in ('both', 'check')]
    o_cmp = [r for (k, _), r in zip(oracle_stmts, o_res) if k in ('both', 'check')]
    cmp_items = [(k, s) for k, s in items if k in ('both', 'check')]
    same = 0
    for (k, s), sr, orr in zip(cmp_items, s_cmp, o_cmp):
        a, b = outcome(sr[1], True), outcome(orr[1], False)
        ok = a[0] == b[0] and (a[1] is None or b[1] is None or a[1] == b[1])
        if k == 'check' or a[0] == 'OK':
            ok = ok and sr[0] == orr[0]
        if ok:
            same += 1
        else:
            print(f'DIFF {s}\n   sprig : {a} {sr[0][:5]}\n   server: {b} {orr[0][:5]}')
    print(f'{same}/{len(cmp_items)} same, {bad} setup failures')
    sys.exit(0 if same == len(cmp_items) and bad == 0 else 1)


if __name__ == '__main__':
    main()
