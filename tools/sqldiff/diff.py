#!/usr/bin/env python3
"""Byte-level differential: runs one script against an oracle and SprigDB
and compares every response packet.

  diff.py ORACLE SPRIG SCRIPT [--caps +X,-Y]
    ORACLE / SPRIG: host:port:user:password:db[:tls]
    SCRIPT: one probe.py item per line (q:, p:, e:); '#' comments.

Exit status 0 when every packet is identical. Mismatches print both sides.
"""
import sys

sys.path.insert(0, __import__('os').path.join(__import__('os').path.dirname(__import__('os').path.abspath(__file__)), '..', 'checkers'))
import probe  # noqa: E402


def side(spec, caps_names):
    parts = spec.split(':')
    host, port, user, pw, db = parts[:5]
    tls = len(parts) > 5 and parts[5] == 'tls'
    return probe.login(host, int(port), user, pw, db, caps_names, tls)


def describe(ps):
    """A readable line or two for one side's packets of a mismatching statement."""
    import struct
    if not ps:
        return '(nothing)'
    f = ps[0]
    if f[0] == 0xff:
        return f'ERR {struct.unpack_from("<H", f, 1)[0]} [{f[4:9].decode("latin1")}] {f[9:].decode("utf8", "replace")}'
    if f[0] == 0x00 and len(ps) == 1:
        aff, j = probe.lenenc_int(f, 1)
        lid, j = probe.lenenc_int(f, j)
        st, w = struct.unpack_from('<HH', f, j)
        return f'OK affected={aff} last_id={lid} status={st:#x} warnings={w} info={f[j + 4:]!r}'
    out = []
    try:
        n, _ = probe.lenenc_int(f, 0)
        for p in ps[1:1 + n]:
            i = 0
            parts = []
            for _ in range(6):
                ln, j = probe.lenenc_int(p, i)
                parts.append(p[j:j + ln])
                i = j + ln
            i += 1
            cs, ln, ty, fl, dec = struct.unpack_from('<HIBHB', p, i)
            out.append(f'{parts[4].decode("utf8", "replace")}:t{ty}/l{ln}/d{dec}/f{fl:#x}/cs{cs}')
        rows = [repr(p) for p in ps[1 + n:]]
        return ' '.join(out) + ' || ' + ' '.join(rows)[:600]
    except Exception as e:  # a shape this helper doesn't read
        return f'({e})'


def main():
    a = sys.argv[1:]
    oracle, sprig, script = a[0], a[1], a[2]
    caps_names = list(probe.DEFAULT)
    if len(a) > 4 and a[3] == '--caps':
        for x in a[4].split(','):
            if x.startswith('-'):
                caps_names.remove(x[1:])
            else:
                caps_names.append(x.lstrip('+'))
    # Lines for one server only start with `@maria ` or `@mysql `; an `x:` line runs as a query on
    # both sides without being compared (DDL whose warnings are the gateway's DDL path's, not this
    # work's).
    only = None
    if '--side' in a:
        only = a[a.index('--side') + 1]
    items = []
    for l in open(script):
        l = l.rstrip('\n')
        if not l.strip() or l.startswith('#'):
            continue
        if l.startswith('@'):
            tag, _, rest = l.partition(' ')
            if only is not None and tag[1:] != only:
                continue
            l = rest
        items.append(l)
    skipped = {i for i, l in enumerate(items) if l.startswith('x:')}
    items = ['q:' + l[2:] if l.startswith('x:') else l for l in items]
    results = []
    for spec in (oracle, sprig):
        c, caps = side(spec, caps_names)
        r = []
        probe.run_script(c, caps, items, r)
        results.append(r)
    def lenenc(b, i):
        x = b[i]
        if x < 251: return x, i + 1
        if x == 0xfb: return None, i + 1
        if x == 0xfc: return int.from_bytes(b[i+1:i+3], 'little'), i + 3
        if x == 0xfd: return int.from_bytes(b[i+1:i+4], 'little'), i + 4
        return int.from_bytes(b[i+1:i+9], 'little'), i + 9

    def values(ps):
        # Decoded results: errors by code, OKs by affected rows, result sets by column names and
        # row values (text rows only; binary rows compared as bytes).
        out = []
        i = 0
        try:
            while i < len(ps):
                p = ps[i]
                if p[0] == 0xff:
                    out.append(('ERR', int.from_bytes(p[1:3], 'little'))); i += 1; continue
                if p[0] == 0x00 or (p[0] == 0xfe and len(p) < 9):
                    n, _ = lenenc(p, 1) if p[0] == 0 else (0, 0)
                    out.append(('OK', n)); i += 1; continue
                ncol, _ = lenenc(p, 0)
                names = []
                for d in ps[i + 1:i + 1 + ncol]:
                    k = 0; parts = []
                    for _ in range(6):
                        n, k = lenenc(d, k); parts.append(d[k:k + n]); k += n
                    names.append(parts[4])
                j = i + 1 + ncol
                if j < len(ps) and ps[j][0] == 0xfe and len(ps[j]) < 9:
                    j += 1
                rows = []
                while j < len(ps):
                    r = ps[j]; j += 1
                    if r[0] == 0xfe and len(r) < 0xffffff and (len(r) < 9 or True):
                        if len(r) < 12:
                            break
                    if r[0] == 0xff:
                        rows.append(('ERR', int.from_bytes(r[1:3], 'little'))); break
                    k = 0; vals = []
                    for _ in range(ncol):
                        n, k = lenenc(r, k)
                        if n is None: vals.append(None)
                        else: vals.append(r[k:k + n]); k += n
                    rows.append(tuple(vals))
                out.append(('RS', tuple(names), tuple(rows)))
                i = j
        except Exception as e:
            out.append(('UNREADABLE', str(e)))
        return out

    bad = 0
    total = 0
    vbad = 0
    for n, ((item, pa), (_, pb)) in enumerate(zip(*results)):
        if n in skipped:
            continue
        total += 1
        # A statement id is the server's own counter (MariaDB's is server-wide):
        # compare PREPARE_OK without it.
        if item.startswith('p:'):
            for ps in (pa, pb):
                if ps and ps[0][:1] == b'\x00' and len(ps[0]) >= 5:
                    ps[0] = ps[0][:1] + b'\0\0\0\0' + ps[0][5:]
        # The servers' temporary table names (`#sql-...`, from their process and connection
        # ids) differ between any two servers: compared without them.
        import re
        # Whether the server used an index (EOF status 0x10, 0x20) follows its plan, not the
        # result: compared without those two bits.
        def unflag(p):
            if p[:1] == b'\xfe' and len(p) in (5, 7):
                st = (p[3] | (p[4] << 8)) & ~0x30
                return p[:3] + bytes([st & 0xff, st >> 8]) + p[5:]
            return p
        pa = [unflag(p) for p in pa]
        pb = [unflag(p) for p in pb]
        pa = [re.sub(rb'#sql-[A-Za-z0-9_-]+', b'#sql-X', p) for p in pa]
        pb = [re.sub(rb'#sql-[A-Za-z0-9_-]+', b'#sql-X', p) for p in pb]
        if pa == pb:
            continue
        bad += 1
        same_values = values(pa) == values(pb)
        if not same_values:
            vbad += 1
        print(f'MISMATCH{" (values equal)" if same_values else " (VALUES DIFFER)"} {item}')
        print(f'  oracle: {describe(pa)}')
        print(f'  sprig : {describe(pb)}')
        for i in range(max(len(pa), len(pb))):
            x = pa[i].hex() if i < len(pa) else '-'
            y = pb[i].hex() if i < len(pb) else '-'
            mark = '  ' if x == y else '!!'
            print(f'  {mark} oracle {x}')
            if x != y:
                print(f'  {mark} sprig  {y}')
    print(f'values equal {total - vbad}/{total}')
    print(f'{total - bad}/{total} statements byte-identical')
    sys.exit(1 if bad else 0)


main()
