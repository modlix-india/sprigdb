#!/usr/bin/env python3
"""Feature probe: online resharding. Needs a cluster of at least two nodes
and a writer running while data moves, so it runs on the benchmark VMs
(track B), never on a single laptop node: every product records "not probed"
here until then."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import NotProbed


def later(t):
    raise NotProbed("needs two or more nodes and a writer during the move; runs on the benchmark VMs")


if __name__ == "__main__":
    lib.run("resharding", "multi", {k: later for k in ("mysql", "pg", "mongo", "surreal", "arango", "couchbase", "sprig")},
            "Online resharding")
