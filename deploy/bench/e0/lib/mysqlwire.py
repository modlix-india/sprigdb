#!/usr/bin/env python3
"""A small MySQL client/server protocol client, standard library only, that
keeps every packet it receives so results can be compared byte for byte.

It speaks the classic protocol (no compression, no TLS): the v10 handshake,
mysql_native_password and caching_sha2_password (fast path, and the full path
over plain TCP with the server's RSA key and OAEP), COM_QUERY with text result
sets, and COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE with binary
result sets.

  mysqlwire.py --host H --port P --user U --password PW [--db D] -e "SQL; SQL"

prints each statement's rows (tab separated) or its OK/ERR line, for setup
scripts in containers that have no mysql client.
"""
import argparse
import hashlib
import os
import socket
import struct
import sys

# Capability flags
LONG_PASSWORD = 1
FOUND_ROWS = 2
LONG_FLAG = 4
CONNECT_WITH_DB = 8
PROTOCOL_41 = 0x200
TRANSACTIONS = 0x2000
SECURE_CONNECTION = 0x8000
MULTI_STATEMENTS = 0x10000
MULTI_RESULTS = 0x20000
PS_MULTI_RESULTS = 0x40000
PLUGIN_AUTH = 0x80000
PLUGIN_AUTH_LENENC = 0x200000
DEPRECATE_EOF = 0x1000000

DEFAULT_CAPS = (LONG_PASSWORD | FOUND_ROWS | LONG_FLAG | PROTOCOL_41 | TRANSACTIONS | SECURE_CONNECTION
                | MULTI_STATEMENTS | MULTI_RESULTS | PS_MULTI_RESULTS | PLUGIN_AUTH | PLUGIN_AUTH_LENENC)

# Column types (binary protocol decoding)
T_DECIMAL, T_TINY, T_SHORT, T_LONG, T_FLOAT, T_DOUBLE, T_NULL, T_TIMESTAMP, T_LONGLONG, T_INT24 = range(10)
T_DATE, T_TIME, T_DATETIME, T_YEAR = 10, 11, 12, 13
T_NEWDECIMAL = 246
UNSIGNED_FLAG = 32


class MySQLError(Exception):
    def __init__(self, code, state, msg):
        super().__init__(f"{code} ({state}): {msg}")
        self.code, self.state, self.msg = code, state, msg


def lenenc_int(b, i):
    c = b[i]
    if c < 0xFB:
        return c, i + 1
    if c == 0xFB:
        return None, i + 1
    if c == 0xFC:
        return struct.unpack_from("<H", b, i + 1)[0], i + 3
    if c == 0xFD:
        return int.from_bytes(b[i + 1:i + 4], "little"), i + 4
    return struct.unpack_from("<Q", b, i + 1)[0], i + 9


def lenenc_bytes(b, i):
    n, i = lenenc_int(b, i)
    if n is None:
        return None, i
    return bytes(b[i:i + n]), i + n


def put_lenenc_int(n):
    if n < 0xFB:
        return bytes([n])
    if n < 1 << 16:
        return b"\xfc" + struct.pack("<H", n)
    if n < 1 << 24:
        return b"\xfd" + n.to_bytes(3, "little")
    return b"\xfe" + struct.pack("<Q", n)


def put_lenenc_bytes(s):
    return put_lenenc_int(len(s)) + s


def xor(a, b):
    return bytes(x ^ y for x, y in zip(a, b))


def native_scramble(password, salt):
    if not password:
        return b""
    p = password.encode()
    s1 = hashlib.sha1(p).digest()
    s2 = hashlib.sha1(s1).digest()
    return xor(s1, hashlib.sha1(salt + s2).digest())


def sha2_scramble(password, salt):
    if not password:
        return b""
    p = password.encode()
    d1 = hashlib.sha256(p).digest()
    d2 = hashlib.sha256(d1).digest()
    return xor(d1, hashlib.sha256(d2 + salt).digest())


# --- RSA-OAEP (SHA-1) for caching_sha2_password's full authentication over plain TCP ---

def _der(b, i):
    tag = b[i]
    ln = b[i + 1]
    i += 2
    if ln & 0x80:
        n = ln & 0x7F
        ln = int.from_bytes(b[i:i + n], "big")
        i += n
    return tag, b[i:i + ln], i + ln


def rsa_public_key(pem):
    import base64
    body = b"".join(l for l in pem.strip().splitlines() if not l.startswith(b"-----"))
    der = base64.b64decode(body)
    _, spki, _ = _der(der, 0)          # SubjectPublicKeyInfo
    _, _, j = _der(spki, 0)            # AlgorithmIdentifier
    tag, bits, _ = _der(spki, j)       # BIT STRING
    if tag != 0x03:
        raise ValueError("unexpected public key encoding")
    _, seq, _ = _der(bits[1:], 0)      # RSAPublicKey
    _, n, k = _der(seq, 0)
    _, e, _ = _der(seq, k)
    return int.from_bytes(n, "big"), int.from_bytes(e, "big")


def oaep_encrypt(msg, n, e):
    k = (n.bit_length() + 7) // 8
    h = hashlib.sha1
    hlen = 20
    if len(msg) > k - 2 * hlen - 2:
        raise ValueError("password too long for the server's RSA key")

    def mgf1(seed, ln):
        out = b""
        c = 0
        while len(out) < ln:
            out += h(seed + c.to_bytes(4, "big")).digest()
            c += 1
        return out[:ln]

    db = h(b"").digest() + b"\x00" * (k - len(msg) - 2 * hlen - 2) + b"\x01" + msg
    seed = os.urandom(hlen)
    mdb = xor(db, mgf1(seed, k - hlen - 1))
    mseed = xor(seed, mgf1(mdb, hlen))
    em = b"\x00" + mseed + mdb
    return pow(int.from_bytes(em, "big"), e, n).to_bytes(k, "big")


class Result:
    """One statement's answer: OK (affected, insert_id, status, warnings, info), ERR, or a result set
    (columns, rows). `packets` holds every packet of the answer as received (payloads only)."""

    def __init__(self):
        self.kind = None  # "ok" | "err" | "rows"
        self.columns = []
        self.rows = []
        self.affected = self.insert_id = self.status = self.warnings = 0
        self.info = b""
        self.err = None
        self.packets = []
        self.more = False

    def __repr__(self):
        if self.kind == "err":
            return f"ERR {self.err.code} ({self.err.state}) {self.err.msg}"
        if self.kind == "ok":
            return f"OK affected={self.affected} insert_id={self.insert_id} warnings={self.warnings}"
        return f"ROWS {[c['name'] for c in self.columns]} {self.rows}"


class Conn:
    def __init__(self, host, port, user, password="", db=None, timeout=30, caps=DEFAULT_CAPS, charset=45):
        self.sock = socket.create_connection((host, int(port)), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb")
        self.seq = 0
        self.user, self.password, self.db = user, password, db
        self.handshake(caps, charset)

    # framing
    def read_packet(self):
        payload = b""
        while True:
            hdr = self.f.read(4)
            if len(hdr) < 4:
                raise ConnectionError("connection closed by the server")
            ln = int.from_bytes(hdr[:3], "little")
            self.seq = (hdr[3] + 1) & 0xFF
            chunk = self.f.read(ln)
            if len(chunk) < ln:
                raise ConnectionError("connection closed mid-packet")
            payload += chunk
            if ln < 0xFFFFFF:
                return payload

    def write_packet(self, payload):
        out = b""
        while True:
            chunk = payload[:0xFFFFFF]
            payload = payload[0xFFFFFF:]
            out += len(chunk).to_bytes(3, "little") + bytes([self.seq]) + chunk
            self.seq = (self.seq + 1) & 0xFF
            if len(chunk) < 0xFFFFFF:
                break
        self.sock.sendall(out)

    def command(self, payload):
        self.seq = 0
        self.write_packet(payload)

    @staticmethod
    def parse_err(p):
        code = struct.unpack_from("<H", p, 1)[0]
        if p[3:4] == b"#":
            return MySQLError(code, p[4:9].decode(), p[9:].decode("utf-8", "replace"))
        return MySQLError(code, "HY000", p[3:].decode("utf-8", "replace"))

    def handshake(self, caps, charset):
        p = self.read_packet()
        if p[0] == 0xFF:
            raise self.parse_err(p)
        self.server_greeting = p
        i = 1
        end = p.index(b"\x00", i)
        self.server_version = p[i:end].decode()
        i = end + 1
        self.thread_id = struct.unpack_from("<I", p, i)[0]
        i += 4
        salt = p[i:i + 8]
        i += 9
        cap_lo = struct.unpack_from("<H", p, i)[0]
        i += 2
        self.server_charset = p[i]
        i += 1
        i += 2  # status
        cap_hi = struct.unpack_from("<H", p, i)[0]
        i += 2
        self.server_caps = cap_lo | (cap_hi << 16)
        alen = p[i]
        i += 1 + 10
        rest = max(13, alen - 8)
        salt += p[i:i + rest].rstrip(b"\x00")[:12]
        i += rest
        plugin = p[i:p.index(b"\x00", i)].decode() if i < len(p) and b"\x00" in p[i:] else "mysql_native_password"
        self.salt = salt[:20]
        caps &= self.server_caps | PLUGIN_AUTH_LENENC
        if self.db:
            caps |= CONNECT_WITH_DB
        self.caps = caps
        auth = self.scramble(plugin, self.salt)
        resp = struct.pack("<IIB", caps, 1 << 24, charset) + b"\x00" * 23
        resp += self.user.encode() + b"\x00"
        resp += put_lenenc_bytes(auth) if caps & PLUGIN_AUTH_LENENC else bytes([len(auth)]) + auth
        if caps & CONNECT_WITH_DB:
            resp += self.db.encode() + b"\x00"
        resp += plugin.encode() + b"\x00"
        self.write_packet(resp)
        self.auth_loop(plugin)

    def scramble(self, plugin, salt):
        if plugin == "caching_sha2_password":
            return sha2_scramble(self.password, salt)
        if plugin == "mysql_clear_password":
            return self.password.encode() + b"\x00"
        return native_scramble(self.password, salt)

    def auth_loop(self, plugin):
        while True:
            p = self.read_packet()
            if p[0] == 0x00:
                return
            if p[0] == 0xFF:
                raise self.parse_err(p)
            if p[0] == 0xFE:  # auth switch
                end = p.index(b"\x00", 1)
                plugin = p[1:end].decode()
                self.salt = p[end + 1:].rstrip(b"\x00")[:20]
                self.write_packet(self.scramble(plugin, self.salt))
                continue
            if p[0] == 0x01 and plugin == "caching_sha2_password":
                if p[1:2] == b"\x03":  # fast auth succeeded; OK follows
                    continue
                if p[1:2] == b"\x04":  # full auth: ask for the RSA key
                    self.write_packet(b"\x02")
                    key = self.read_packet()
                    n, e = rsa_public_key(key[1:])
                    pw = self.password.encode() + b"\x00"
                    salt = (self.salt * (len(pw) // len(self.salt) + 1))[:len(pw)]
                    self.write_packet(oaep_encrypt(xor(pw, salt), n, e))
                    continue
            raise ConnectionError(f"unexpected auth packet {p[:8]!r}")

    # results
    @staticmethod
    def parse_ok(p, r):
        r.kind = "ok"
        i = 1
        r.affected, i = lenenc_int(p, i)
        r.insert_id, i = lenenc_int(p, i)
        r.status, r.warnings = struct.unpack_from("<HH", p, i)
        r.info = p[i + 4:]
        r.more = bool(r.status & 8)

    @staticmethod
    def parse_coldef(p):
        i = 0
        vals = []
        for _ in range(6):
            v, i = lenenc_bytes(p, i)
            vals.append(v)
        i += 1  # length of fixed fields (0x0c)
        charset, length, typ, flags, decimals = struct.unpack_from("<HIBHB", p, i)
        return {"catalog": vals[0], "schema": vals[1], "table": vals[2], "org_table": vals[3],
                "name": vals[4].decode("utf-8", "replace"), "org_name": vals[5], "charset": charset,
                "length": length, "type": typ, "flags": flags, "decimals": decimals}

    def is_eof(self, p):
        return p[0] == 0xFE and len(p) < 0xFFFFFF and len(p) <= 9 if not self.caps & DEPRECATE_EOF else (
            p[0] == 0xFE and len(p) < 0xFFFFFF)

    def read_result(self, binary=False):
        r = Result()
        p = self.read_packet()
        r.packets.append(p)
        if p[0] == 0x00:
            self.parse_ok(p, r)
            return r
        if p[0] == 0xFF:
            r.kind = "err"
            r.err = self.parse_err(p)
            return r
        ncols, _ = lenenc_int(p, 0)
        r.kind = "rows"
        for _ in range(ncols):
            c = self.read_packet()
            r.packets.append(c)
            r.columns.append(self.parse_coldef(c))
        if not self.caps & DEPRECATE_EOF:
            r.packets.append(self.read_packet())
        while True:
            p = self.read_packet()
            r.packets.append(p)
            if p[0] == 0xFE and len(p) < 0xFFFFFF and (len(p) <= 9 or self.caps & DEPRECATE_EOF):
                if self.caps & DEPRECATE_EOF:
                    t = Result()
                    self.parse_ok(p, t)
                    r.status, r.warnings, r.more = t.status, t.warnings, t.more
                else:
                    r.warnings, r.status = struct.unpack_from("<HH", p, 1)
                    r.more = bool(r.status & 8)
                return r
            if p[0] == 0xFF:
                r.kind = "err"
                r.err = self.parse_err(p)
                return r
            r.rows.append(self.binary_row(p, r.columns) if binary else self.text_row(p, len(r.columns)))

    @staticmethod
    def text_row(p, n):
        i = 0
        row = []
        for _ in range(n):
            v, i = lenenc_bytes(p, i)
            row.append(v)
        return tuple(row)

    @staticmethod
    def binary_row(p, cols):
        n = len(cols)
        nb = (n + 7 + 2) // 8
        bitmap = p[1:1 + nb]
        i = 1 + nb
        row = []
        for k, c in enumerate(cols):
            if bitmap[(k + 2) // 8] & (1 << ((k + 2) % 8)):
                row.append(None)
                continue
            t, uns = c["type"], bool(c["flags"] & UNSIGNED_FLAG)
            if t == T_LONGLONG:
                v = struct.unpack_from("<Q" if uns else "<q", p, i)[0]
                i += 8
            elif t in (T_LONG, T_INT24):
                v = struct.unpack_from("<I" if uns else "<i", p, i)[0]
                i += 4
            elif t in (T_SHORT, T_YEAR):
                v = struct.unpack_from("<H" if uns else "<h", p, i)[0]
                i += 2
            elif t == T_TINY:
                v = struct.unpack_from("<B" if uns else "<b", p, i)[0]
                i += 1
            elif t == T_DOUBLE:
                v = struct.unpack_from("<d", p, i)[0]
                i += 8
            elif t == T_FLOAT:
                v = struct.unpack_from("<f", p, i)[0]
                i += 4
            elif t in (T_DATE, T_DATETIME, T_TIMESTAMP, T_TIME):
                ln = p[i]
                v = bytes(p[i + 1:i + 1 + ln])
                i += 1 + ln
            else:
                v, i = lenenc_bytes(p, i)
            # text form, so text and binary answers compare equal
            row.append(v if isinstance(v, bytes) or v is None else str(v).encode())
        return tuple(row)

    def query(self, sql):
        """COM_QUERY; returns a list of Results (more than one for multi-statements)."""
        self.command(b"\x03" + sql.encode())
        out = [self.read_result()]
        while out[-1].kind != "err" and out[-1].more:
            out.append(self.read_result())
        return out

    def q1(self, sql):
        r = self.query(sql)[0]
        if r.kind == "err":
            raise r.err
        return r

    def prepare(self, sql):
        self.command(b"\x16" + sql.encode())
        p = self.read_packet()
        packets = [p]
        if p[0] == 0xFF:
            raise self.parse_err(p)
        stmt_id, ncols, nparams, _, warnings = struct.unpack_from("<IHHBH", p, 1)
        params, cols = [], []
        for _ in range(nparams):
            c = self.read_packet()
            packets.append(c)
            params.append(self.parse_coldef(c))
        if nparams and not self.caps & DEPRECATE_EOF:
            packets.append(self.read_packet())
        for _ in range(ncols):
            c = self.read_packet()
            packets.append(c)
            cols.append(self.parse_coldef(c))
        if ncols and not self.caps & DEPRECATE_EOF:
            packets.append(self.read_packet())
        return {"id": stmt_id, "params": params, "columns": cols, "nparams": nparams, "packets": packets,
                "warnings": warnings, "sent_types": False}

    def execute(self, stmt, args=()):
        """COM_STMT_EXECUTE with ints as LONGLONG, floats as DOUBLE, str/bytes as VAR_STRING, None as NULL."""
        n = stmt["nparams"]
        if len(args) != n:
            raise ValueError(f"statement takes {n} parameters, got {len(args)}")
        body = b"\x17" + struct.pack("<IBI", stmt["id"], 0, 1)
        if n:
            bitmap = bytearray((n + 7) // 8)
            types = b""
            vals = b""
            for k, a in enumerate(args):
                if a is None:
                    bitmap[k // 8] |= 1 << (k % 8)
                    types += bytes([T_NULL, 0])
                elif isinstance(a, bool) or isinstance(a, int):
                    types += bytes([T_LONGLONG, 0])
                    vals += struct.pack("<q", int(a))
                elif isinstance(a, float):
                    types += bytes([T_DOUBLE, 0])
                    vals += struct.pack("<d", a)
                else:
                    b = a.encode() if isinstance(a, str) else bytes(a)
                    types += bytes([253, 0])
                    vals += put_lenenc_bytes(b)
            body += bytes(bitmap) + b"\x01" + types + vals
        self.command(body)
        out = [self.read_result(binary=True)]
        while out[-1].kind != "err" and out[-1].more:
            out.append(self.read_result(binary=True))
        return out

    def close_stmt(self, stmt):
        self.command(b"\x19" + struct.pack("<I", stmt["id"]))

    def ping(self):
        self.command(b"\x0e")
        p = self.read_packet()
        return p[0] == 0

    def close(self):
        try:
            self.command(b"\x01")
        except OSError:
            pass
        self.sock.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", required=True, type=int)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db")
    ap.add_argument("-e", required=True, help="statements separated by ';' (sent as one multi-statement query)")
    ap.add_argument("--retry", type=int, default=0, help="seconds to keep retrying the login")
    a = ap.parse_args()
    import time
    t_end = time.time() + a.retry
    while True:
        try:
            c = Conn(a.host, a.port, a.user, a.password, a.db)
            break
        except (OSError, MySQLError) as e:
            if time.time() >= t_end:
                print(f"login failed: {e}", file=sys.stderr)
                sys.exit(1)
            time.sleep(1)
    rc = 0
    for r in c.query(a.e):
        if r.kind == "err":
            print(f"ERROR {r.err.code} ({r.err.state}): {r.err.msg}", file=sys.stderr)
            rc = 1
        elif r.kind == "rows":
            for row in r.rows:
                print("\t".join("NULL" if v is None else v.decode("utf-8", "replace") for v in row))
    c.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
