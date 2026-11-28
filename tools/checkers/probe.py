#!/usr/bin/env python3
"""Raw MySQL-protocol probe: prints every response packet's payload as hex.

  probe.py HOST PORT USER PASSWORD DB [--caps +X,-Y] [--tls] script...

script items:
  q:<sql>              COM_QUERY
  p:<sql>              COM_STMT_PREPARE (statement id remembered as $1, $2, ...)
  e:<n>:<type>=<val>,...  COM_STMT_EXECUTE of prepared statement n with params
                       (type: i=LONGLONG, s=VAR_STRING, n=NULL)
Used only to observe the oracles' bytes (black box) and SprigDB's.
"""
import hashlib, socket, ssl, struct, sys

CAPS = {
    'LONG_PASSWORD': 1, 'FOUND_ROWS': 2, 'LONG_FLAG': 4, 'CONNECT_WITH_DB': 8,
    'LOCAL_FILES': 0x80, 'PROTOCOL_41': 0x200, 'SSL': 0x800, 'TRANSACTIONS': 0x2000,
    'SECURE_CONNECTION': 0x8000, 'MULTI_STATEMENTS': 0x10000, 'MULTI_RESULTS': 0x20000,
    'PS_MULTI_RESULTS': 0x40000, 'PLUGIN_AUTH': 0x80000, 'CONNECT_ATTRS': 0x100000,
    'PLUGIN_AUTH_LENENC': 0x200000, 'SESSION_TRACK': 0x800000, 'DEPRECATE_EOF': 0x1000000,
    'QUERY_ATTRIBUTES': 1 << 27,
    'MARIADB_CLIENT_PROGRESS': 1 << 32, 'MARIADB_STMT_BULK': 1 << 34,
    'MARIADB_EXTENDED_METADATA': 1 << 35, 'MARIADB_CACHE_METADATA': 1 << 36,
}
DEFAULT = ['LONG_PASSWORD', 'LONG_FLAG', 'CONNECT_WITH_DB', 'PROTOCOL_41', 'TRANSACTIONS',
           'SECURE_CONNECTION', 'MULTI_RESULTS', 'PS_MULTI_RESULTS', 'PLUGIN_AUTH',
           'PLUGIN_AUTH_LENENC', 'SESSION_TRACK', 'DEPRECATE_EOF']


# LOCAL INFILE requests are answered from this directory (by base name).
INFILE_DIR = __import__('os').environ.get(
    'PROBE_INFILE_DIR',
    __import__('os').path.join(__import__('os').path.dirname(__import__('os').path.abspath(__file__)), '..', 'sqldiff', 'infile'))


class Conn:
    def __init__(self, host, port):
        self.s = socket.create_connection((host, port))
        self.seq = 0

    def recv_exact(self, n):
        b = b''
        while len(b) < n:
            c = self.s.recv(n - len(b))
            if not c:
                raise EOFError('closed')
            b += c
        return b

    def read(self):
        h = self.recv_exact(4)
        ln = h[0] | h[1] << 8 | h[2] << 16
        self.seq = (h[3] + 1) & 0xFF
        return self.recv_exact(ln)

    def write(self, payload, reset=False):
        if reset:
            self.seq = 0
        self.s.sendall(struct.pack('<I', len(payload))[:3] + bytes([self.seq]) + payload)
        self.seq = (self.seq + 1) & 0xFF


def lenenc_int(b, i):
    c = b[i]
    if c < 0xFB:
        return c, i + 1
    if c == 0xFC:
        return struct.unpack_from('<H', b, i + 1)[0], i + 3
    if c == 0xFD:
        return int.from_bytes(b[i + 1:i + 4], 'little'), i + 4
    return struct.unpack_from('<Q', b, i + 1)[0], i + 9


def native(pw, salt):
    h1 = hashlib.sha1(pw).digest()
    h2 = hashlib.sha1(h1).digest()
    h3 = hashlib.sha1(salt + h2).digest()
    return bytes(a ^ b for a, b in zip(h1, h3))


def sha2(pw, salt):
    h1 = hashlib.sha256(pw).digest()
    h2 = hashlib.sha256(hashlib.sha256(h1).digest() + salt).digest()
    return bytes(a ^ b for a, b in zip(h1, h2))


def login(host, port, user, pw, db, caps_names, tls):
    c = Conn(host, port)
    g = c.read()
    i = 1
    i = g.index(b'\0', i) + 1  # version
    i += 4
    salt = g[i:i + 8]
    i += 9
    scaps = struct.unpack_from('<H', g, i)[0]
    i += 2
    coll = g[i]
    i += 3
    scaps |= struct.unpack_from('<H', g, i)[0] << 16
    i += 2
    alen = g[i]
    i += 11
    mcaps = struct.unpack_from('<I', g, i - 4)[0]
    salt += g[i:i + max(13, alen - 8) - 1]
    i += max(13, alen - 8)
    plugin = g[i:g.index(b'\0', i)].decode() if i < len(g) else 'mysql_native_password'
    caps = 0
    for n in caps_names:
        caps |= CAPS[n]
    if tls:
        caps |= CAPS['SSL']
    caps &= (scaps | (mcaps << 32)) | CAPS['SSL']
    print(f'# server caps {scaps:#x} mariadb {mcaps:#x} coll {coll} plugin {plugin}; client {caps:#x}')
    base = struct.pack('<IIB', caps & 0xFFFFFFFF, 1 << 24, int(__import__('os').environ.get('PROBE_COLL', '45'))) + b'\0' * 19 + struct.pack('<I', caps >> 32)
    if tls:
        c.write(base)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        c.s = ctx.wrap_socket(c.s)
    auth = native(pw.encode(), salt) if plugin == 'mysql_native_password' else sha2(pw.encode(), salt)
    p = base + user.encode() + b'\0' + bytes([len(auth)]) + auth
    if caps & CAPS['CONNECT_WITH_DB']:
        p += db.encode() + b'\0'
    p += plugin.encode() + b'\0'
    c.write(p)
    while True:
        r = c.read()
        if r[0] == 0x00:
            break
        if r[0] == 0xFF:
            raise SystemExit(f'login failed: {r[3:]!r}')
        if r[0] == 0xFE:  # auth switch
            j = r.index(b'\0', 1)
            plugin = r[1:j].decode()
            salt = r[j + 1:].rstrip(b'\0')
            c.write(native(pw.encode(), salt) if plugin == 'mysql_native_password' else sha2(pw.encode(), salt))
            continue
        if r[0] == 0x01 and r[1:] == b'\x03':
            continue  # fast auth ok, OK follows
        if r[0] == 0x01 and r[1:] == b'\x04':
            c.write(pw.encode() + b'\0')  # full auth over TLS
            continue
        raise SystemExit(f'unexpected auth packet {r.hex()}')
    return c, caps


def _status(p, caps):
    """Server status of an OK/EOF/terminator packet (0 for others)."""
    try:
        if p[0] == 0xFE and len(p) < 9 and not caps & CAPS['DEPRECATE_EOF']:
            return struct.unpack_from('<H', p, 3)[0]
        if p[0] in (0x00, 0xFE):
            _, j = lenenc_int(p, 1)
            _, j = lenenc_int(p, j)
            return struct.unpack_from('<H', p, j)[0]
    except Exception:
        return 0
    return 0


def response(c, caps, label, prepared=None, sink=None):
    """Every result of a response (multi-results followed)."""
    r = _response1(c, caps, label, prepared, sink)
    if prepared is not None:
        return r
    while getattr(c, '_last_status', 0) & 0x0008:
        c._last_status = 0
        _response1(c, caps, label + ' (more)', None, sink)
    return r


def _response1(c, caps, label, prepared=None, sink=None):
    """Prints a response (or appends its packets to `sink`); returns the
    prepare-OK stmt id if any."""
    out = sink if sink is not None else None
    def emit(p, first=False):
        if out is not None:
            out.append(p)
        else:
            print(f'{label} <- {p.hex()}' if first else f'   <- {p.hex()}')
    first = c.read()
    emit(first, True)
    # LOCAL INFILE: the server names a file; it is sent from INFILE_DIR (by
    # its base name), in chunks, then an empty packet; the answer follows.
    if first[0] == 0xFB and prepared is None:
        import os
        name = os.path.basename(first[1:].decode(errors='replace'))
        path = os.path.join(INFILE_DIR, name)
        data = open(path, 'rb').read() if os.path.isfile(path) else b''
        for k in range(0, len(data), 16384):
            c.write(data[k:k + 16384])
        c.write(b'')
        first = c.read()
        emit(first)
    if first[0] in (0x00,) and prepared is not None:
        sid, ncol, npar = struct.unpack_from('<IHH', first, 1)
        n = npar + ncol
        eofs = (1 if npar else 0) + (1 if ncol else 0)
        if caps & CAPS['DEPRECATE_EOF']:
            eofs = 0
        for _ in range(n + eofs):
            emit(c.read())
        return sid
    c._last_status = 0
    if first[0] in (0x00, 0xFF, 0xFE):
        if first[0] != 0xFF:
            c._last_status = _status(first, caps)
        return None
    ncol, j = lenenc_int(first, 0)
    meta = True
    if caps & CAPS['MARIADB_CACHE_METADATA'] and j < len(first):
        meta = first[j] == 1
    if meta:
        for _ in range(ncol):
            emit(c.read())
    # The EOF after the definitions comes even when they were left out.
    if not caps & CAPS['DEPRECATE_EOF']:
        emit(c.read())
    while True:
        r = c.read()
        emit(r)
        if r[0] == 0xFE and len(r) < 0xFFFFFF:
            c._last_status = _status(r, caps)
            break
        if r[0] == 0xFF:
            break
    return None


def main():
    a = sys.argv[1:]
    host, port, user, pw, db = a[0], int(a[1]), a[2], a[3], a[4]
    rest = a[5:]
    caps_names = list(DEFAULT)
    tls = False
    while rest and rest[0].startswith('--'):
        if rest[0] == '--tls':
            tls = True
            rest = rest[1:]
        elif rest[0] == '--caps':
            for x in rest[1].split(','):
                if x.startswith('-'):
                    caps_names.remove(x[1:])
                else:
                    caps_names.append(x.lstrip('+'))
            rest = rest[2:]
    c, caps = login(host, port, user, pw, db, caps_names, tls)
    run_script(c, caps, rest)


def run_script(c, caps, items, results=None):
    """Runs `items`; with `results` (a list), appends (item, packets)."""
    stmts = []
    for item in items:
        sink = [] if results is not None else None
        if results is not None:
            results.append((item, sink))
        kind, _, body = item.partition(':')
        if kind == 'q':
            c.write(b'\x03' + body.encode(), reset=True)
            response(c, caps, f'Q {body}', sink=sink)
        elif kind == 'p':
            c.write(b'\x16' + body.encode(), reset=True)
            sid = response(c, caps, f'P {body}', prepared=True, sink=sink)
            stmts.append(sid)
        elif kind == 'e':
            n, _, ps = body.partition(':')
            sid = stmts[int(n) - 1]
            import re as _re
            params = [x.replace('\\,', ',').split('=', 1) for x in _re.split(r'(?<!\\),', ps)] if ps else []
            pkt = b'\x17' + struct.pack('<IBI', sid, 0, 1)
            if params:
                nb = bytearray((len(params) + 7) // 8)
                types = b''
                vals = b''
                for k, (t, v) in enumerate(params):
                    if t == 'n':
                        nb[k // 8] |= 1 << (k % 8)
                        types += b'\x06\x00'
                    elif t == 'i':
                        types += b'\x08\x00'
                        vals += struct.pack('<q', int(v))
                    elif t == 'u':
                        types += b'\x08\x80'
                        vals += struct.pack('<Q', int(v))
                    elif t == 'd':
                        types += b'\x05\x00'
                        vals += struct.pack('<d', float(v))
                    elif t == 'f':
                        types += b'\x04\x00'
                        vals += struct.pack('<f', float(v))
                    elif t == 'x':  # bytes given in hex, sent as a BLOB
                        raw = bytes.fromhex(v)
                        types += b'\xfc\x00'
                        vals += bytes([len(raw)]) + raw
                    elif t == 'D':  # DATETIME 'Y-M-D h:m:s[.u]' or DATE 'Y-M-D'
                        import re
                        m = [int(x) for x in re.split(r'[- :.]', v)]
                        types += b'\x0c\x00'
                        if len(m) == 3:
                            vals += bytes([4]) + struct.pack('<HBB', *m)
                        elif len(m) == 6:
                            vals += bytes([7]) + struct.pack('<HBBBBB', *m)
                        else:
                            vals += bytes([11]) + struct.pack('<HBBBBBI', *m)
                    elif t == 'T':  # TIME '[-]h:m:s[.u]'
                        neg = v.startswith('-')
                        m = [int(x) for x in v.lstrip('-').replace('.', ':').split(':')]
                        types += b'\x0b\x00'
                        d, h = divmod(m[0], 24)
                        if len(m) == 3:
                            vals += bytes([8]) + struct.pack('<BIBBB', neg, d, h, m[1], m[2])
                        else:
                            vals += bytes([12]) + struct.pack('<BIBBBI', neg, d, h, m[1], m[2], m[3])
                    else:
                        types += b'\xfd\x00'
                        raw = v.encode()
                        vals += bytes([len(raw)]) + raw
                pkt += bytes(nb) + b'\x01' + types + vals
            c.write(pkt, reset=True)
            response(c, caps, f'E {body}', sink=sink)


if __name__ == '__main__':
    main()
