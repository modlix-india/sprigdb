# tools

Scripts that produce SprigDB's correctness evidence, kept with the code so a
number can be reproduced from a commit. Each folder's README says what it
needs and how to run it.

| Folder | What | Run |
|---|---|---|
| [`live/`](live/README.md) | An engine-only local stack (etcd, a controller, one to five gateways) and the runner for the live test suites, the tests marked `#[ignore = "live: ..."]` | `tools/live/stack.sh up`, `tools/live/run.sh <suite>` |
| [`checkers/`](checkers/README.md) | Fault checkers against a three-gateway stack: the kill loop (acknowledged writes survive `kill -9` of the leader), follower reads under DDL and faults, the replicated-unit kill loop | `python3 tools/checkers/gw_kill.py 10` |
| [`sqldiff/`](sqldiff/README.md) | The SQL differential: 84 statement corpora run through SprigDB and MySQL 8.4 or MariaDB 11.8, every response packet compared, with a floor per corpus | `tools/sqldiff/run.sh --oracles` |

Everything here is our own code and data. The oracle servers (MySQL,
MariaDB, Redis, MongoDB) are pulled as container images when a run needs
them; none of their files are in this repository.
