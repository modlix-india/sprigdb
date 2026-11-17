"""A small Bolt 4.4 client (standard library only) for the graph workloads:
handshake, HELLO with basic auth, RUN + PULL, RESET after a failure, and
PackStream values. Enough to compare answers between Bolt servers."""
import socket
import struct


class BoltError(Exception):
    def __init__(self, meta):
        self.code = meta.get("code", "?")
        self.message = meta.get("message", "")
        super().__init__(f"{self.code}: {self.message}")


class Struct:
    def __init__(self, tag, fields):
        self.tag, self.fields = tag, fields

    def __repr__(self):
        return f"Struct({self.tag:#x}, {self.fields!r})"


def pack(v, out):
    if v is None:
        out.append(b"\xC0")
    elif v is True:
        out.append(b"\xC3")
    elif v is False:
        out.append(b"\xC2")
    elif isinstance(v, int):
        if -16 <= v < 128:
            out.append(struct.pack(">b", v))
        elif -128 <= v < 128:
            out.append(b"\xC8" + struct.pack(">b", v))
        elif -32768 <= v < 32768:
            out.append(b"\xC9" + struct.pack(">h", v))
        elif -2**31 <= v < 2**31:
            out.append(b"\xCA" + struct.pack(">i", v))
        else:
            out.append(b"\xCB" + struct.pack(">q", v))
    elif isinstance(v, float):
        out.append(b"\xC1" + struct.pack(">d", v))
    elif isinstance(v, str):
        b = v.encode()
        n = len(b)
        out.append((bytes([0x80 + n]) if n < 16 else b"\xD0" + bytes([n]) if n < 256 else b"\xD1" + struct.pack(">H", n)) + b)
    elif isinstance(v, (list, tuple)):
        n = len(v)
        out.append(bytes([0x90 + n]) if n < 16 else b"\xD4" + bytes([n]))
        for x in v:
            pack(x, out)
    elif isinstance(v, dict):
        n = len(v)
        out.append(bytes([0xA0 + n]) if n < 16 else b"\xD8" + bytes([n]))
        for k, x in v.items():
            pack(k, out)
            pack(x, out)
    elif isinstance(v, Struct):
        out.append(bytes([0xB0 + len(v.fields), v.tag]))
        for x in v.fields:
            pack(x, out)
    else:
        raise TypeError(type(v))


class Reader:
    def __init__(self, b):
        self.b, self.i = b, 0

    def take(self, n):
        x = self.b[self.i:self.i + n]
        self.i += n
        return x

    def value(self):
        m = self.take(1)[0]
        if m < 0x80:
            return m
        if m >= 0xF0:
            return m - 0x100
        hi = m & 0xF0
        if hi == 0x80:
            return self.take(m & 0x0F).decode()
        if hi == 0x90:
            return [self.value() for _ in range(m & 0x0F)]
        if hi == 0xA0:
            return {self.value(): self.value() for _ in range(m & 0x0F)}
        if hi == 0xB0:
            tag = self.take(1)[0]
            return Struct(tag, [self.value() for _ in range(m & 0x0F)])
        if m == 0xC0:
            return None
        if m == 0xC1:
            return struct.unpack(">d", self.take(8))[0]
        if m in (0xC2, 0xC3):
            return m == 0xC3
        if m in (0xC8, 0xC9, 0xCA, 0xCB):
            n = {0xC8: 1, 0xC9: 2, 0xCA: 4, 0xCB: 8}[m]
            return int.from_bytes(self.take(n), "big", signed=True)
        if m in (0xCC, 0xCD, 0xCE):
            n = int.from_bytes(self.take({0xCC: 1, 0xCD: 2, 0xCE: 4}[m]), "big")
            return bytes(self.take(n))
        if m in (0xD0, 0xD1, 0xD2):
            n = int.from_bytes(self.take({0xD0: 1, 0xD1: 2, 0xD2: 4}[m]), "big")
            return self.take(n).decode()
        if m in (0xD4, 0xD5, 0xD6):
            n = int.from_bytes(self.take({0xD4: 1, 0xD5: 2, 0xD6: 4}[m]), "big")
            return [self.value() for _ in range(n)]
        if m in (0xD8, 0xD9, 0xDA):
            n = int.from_bytes(self.take({0xD8: 1, 0xD9: 2, 0xDA: 4}[m]), "big")
            return {self.value(): self.value() for _ in range(n)}
        raise ValueError(f"marker {m:#x}")


class Bolt:
    def __init__(self, hp, user, password, timeout=60):
        h, p = hp.rsplit(":", 1)
        self.s = socket.create_connection((h, int(p)), timeout=timeout)
        self.s.sendall(b"\x60\x60\xb0\x17" + b"\x00\x00\x04\x04" + b"\x00\x00\x03\x04" + b"\x00" * 8)
        v = self._recv(4)
        if v == b"\x00\x00\x00\x00":
            raise ConnectionError("no common Bolt version")
        self.version = (v[3], v[2])
        auth = {"scheme": "basic", "principal": user, "credentials": password} if user else {"scheme": "none"}
        self._send(Struct(0x01, [{"user_agent": "sprigdb-bench/1", **auth}]))
        tag, meta = self._message()
        if tag != 0x70:
            raise BoltError(meta[0] if meta else {})
        self.server = meta[0].get("server", "") if meta else ""

    def _recv(self, n):
        buf = b""
        while len(buf) < n:
            c = self.s.recv(n - len(buf))
            if not c:
                raise ConnectionError("closed")
            buf += c
        return buf

    def _send(self, st):
        out = []
        pack(st, out)
        data = b"".join(out)
        chunks = b""
        for i in range(0, len(data), 65535):
            part = data[i:i + 65535]
            chunks += struct.pack(">H", len(part)) + part
        self.s.sendall(chunks + b"\x00\x00")

    def _message(self):
        data = b""
        while True:
            n = struct.unpack(">H", self._recv(2))[0]
            if n == 0:
                if data:
                    break
                continue
            data += self._recv(n)
        st = Reader(data).value()
        return st.tag, st.fields

    def query(self, q, params=None, extra=None):
        """Runs q; returns (fields, records). Raises BoltError (after RESET)."""
        self._send(Struct(0x10, [q, params or {}, extra or {}]))
        self._send(Struct(0x3F, [{"n": -1}]))
        fields, records, err = [], [], None
        tag, meta = self._message()
        if tag == 0x70:
            fields = meta[0].get("fields", [])
        elif tag == 0x7F:
            err = meta[0]
        while True:
            tag, meta = self._message()
            if tag == 0x71:
                records.append(meta[0])
            elif tag == 0x70:
                break
            elif tag == 0x7F:
                err = err or meta[0]
                break
            elif tag == 0x7E:  # IGNORED
                break
        if err is not None:
            self._send(Struct(0x0F, []))
            while self._message()[0] not in (0x70, 0x7F):
                pass
            raise BoltError(err)
        return fields, records

    def close(self):
        try:
            self._send(Struct(0x02, []))
        except OSError:
            pass
        self.s.close()


def norm(v):
    """A value as comparable text, as the Rust Bolt differential prints it."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        r = repr(v)
        return r
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, bytes):
        return repr(list(v))
    if isinstance(v, list):
        return "[" + ", ".join(norm(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {x}" for k, x in sorted((k, norm(x)) for k, x in v.items())) + "}"
    if isinstance(v, Struct):
        f = v.fields
        if v.tag == 0x4E:
            return f"(:{':'.join(sorted(f[1]))} {norm(f[2])})"
        if v.tag == 0x52:
            return f"[:{f[3]} {norm(f[4])}]"
        if v.tag == 0x72:
            return f"[:{f[1]} {norm(f[2])}]"
        if v.tag == 0x50:
            nodes, rels, idx = f[0], f[1], f[2]
            s = norm(nodes[0])
            for j in range(0, len(idx), 2):
                r, n = idx[j], idx[j + 1]
                rel = norm(rels[abs(r) - 1])
                s += f"-{rel}->" if r > 0 else f"<-{rel}-"
                s += norm(nodes[n])
            return s
        return f"<{v.tag:#x} {' '.join(norm(x) for x in f)}>"
    return repr(v)
