"""A small RESP2/RESP3 client (standard library only) for the E0 differential
and crash runners. Replies are returned as plain Python values: str for simple
strings ("+OK" -> "OK", wrapped in Simple), bytes for bulk strings, int, None
for nulls, list for arrays and sets (sets marked), dict for maps, Err for
errors. Pipelines send many commands in one write and read the replies in order.
"""
import socket


class Err:
    def __init__(self, line):
        self.line = line
        self.code = line.split(" ", 1)[0]

    def __eq__(self, o):
        return isinstance(o, Err) and o.line == self.line

    def __repr__(self):
        return f"Err({self.line!r})"


class Simple(str):
    def __repr__(self):
        return f"+{str(self)}"


class Set(list):
    pass


class Push(list):
    pass


def encode(args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


class Conn:
    def __init__(self, hp, user=None, password=None, timeout=30):
        h, p = hp.rsplit(":", 1)
        self.s = socket.create_connection((h, int(p)), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile("rb")
        if password:
            r = self.cmd("AUTH", user, password) if user else self.cmd("AUTH", password)
            if isinstance(r, Err):
                raise ConnectionError(f"AUTH failed: {r.line}")

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return Simple(rest.decode())
        if t == b"-":
            return Err(rest.decode())
        if t == b":":
            return int(rest)
        if t in (b"$", b"="):
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)[:-2]
            return d
        if t in (b"*", b"~", b">"):
            n = int(rest)
            if n < 0:
                return None
            items = [self.read() for _ in range(n)]
            return Set(items) if t == b"~" else Push(items) if t == b">" else items
        if t == b"%":
            n = int(rest)
            return {self._key(self.read()): self.read() for _ in range(n)}
        if t == b"_":
            return None
        if t == b"#":
            return rest == b"t"
        if t == b",":
            return float(rest)
        if t == b"(":
            return int(rest)
        if t == b"!":
            n = int(rest)
            return Err(self.f.read(n + 2)[:-2].decode())
        raise ConnectionError(f"unknown reply type {line!r}")

    @staticmethod
    def _key(k):
        return k if not isinstance(k, list) else tuple(k)

    def cmd(self, *args):
        self.s.sendall(encode(args))
        return self.read()

    def pipeline(self, cmds):
        self.s.sendall(b"".join(encode(c) for c in cmds))
        return [self.read() for _ in cmds]

    def close(self):
        self.s.close()
