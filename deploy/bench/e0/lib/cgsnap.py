#!/usr/bin/env python3
"""Prints one JSON snapshot of CPU counters: each named container's cgroup v2
usage (user and system microseconds) and the machine-wide /proc/stat line.

Runs in the e0bench-cgmon sidecar, which mounts the host's cgroup tree at
/hostcg, so it can read every container, including images without a shell
(etcd). Container ids are matched against the cgroup directory names Docker
uses with either cgroup driver: docker/<id> and system.slice/docker-<id>.scope.

  cgsnap.py <container-id> ...
"""
import json
import os
import sys
import time

ROOT = "/hostcg"
CACHE = "/tmp/cgsnap-paths.json"


def find(cid, cache):
    if cid in cache and os.path.exists(cache[cid]):
        return cache[cid]
    for base in ("docker", "system.slice", "kubepods", ""):
        d = os.path.join(ROOT, base)
        if not os.path.isdir(d):
            continue
        for name in os.listdir(d):
            if cid in name and os.path.isfile(os.path.join(d, name, "cpu.stat")):
                cache[cid] = os.path.join(d, name)
                return cache[cid]
    for dirpath, dirs, _ in os.walk(ROOT):
        if dirpath.count("/") > 6:
            dirs[:] = []
            continue
        for name in dirs:
            if cid in name:
                cache[cid] = os.path.join(dirpath, name)
                return cache[cid]
    return None


def main():
    try:
        cache = json.load(open(CACHE))
    except (OSError, ValueError):
        cache = {}
    out = {"t": time.time(), "cg": {}}
    for cid in sys.argv[1:]:
        p = find(cid, cache)
        if not p:
            out["cg"][cid] = None
            continue
        st = {}
        with open(os.path.join(p, "cpu.stat")) as f:
            for line in f:
                k, v = line.split()
                st[k] = int(v)
        out["cg"][cid] = {"usage_usec": st.get("usage_usec"), "user_usec": st.get("user_usec"),
                          "system_usec": st.get("system_usec")}
    with open("/proc/stat") as f:
        cpu = f.readline().split()[1:]
        ncpu = sum(1 for l in f if l.startswith("cpu"))
    v = [int(x) for x in cpu]
    idle = v[3] + (v[4] if len(v) > 4 else 0)
    out["proc"] = {"busy": sum(v[:8]) - idle, "total": sum(v[:8]), "ncpu": ncpu, "hz": os.sysconf("SC_CLK_TCK")}
    json.dump(cache, open(CACHE, "w"))
    print(json.dumps(out))


if __name__ == "__main__":
    main()
