# The SQL differential

Each corpus in `corpus/` is a script of statements. `diff.py` runs it in
one session on an oracle server and on SprigDB, for the same persona
(MariaDB 11.8 behind SprigDB's MariaDB port, MySQL 8.4 behind its MySQL
port), and compares every response packet byte for byte: values, errors
with their codes and texts, column metadata, status flags, warnings. It
also says, for each mismatch, whether the decoded values are still equal
(a metadata-only difference).

```sh
tools/live/stack.sh up 1 && tools/live/stack.sh tenant sqldiff
tools/sqldiff/run.sh --oracles              # every corpus, throwaway oracle containers
tools/sqldiff/run.sh --oracles dates views  # some
```

`run.sh`'s header lists the endpoint variables (`SQLDIFF_SPRIG_*`,
`SQLDIFF_ORACLE_*`). Output per corpus and side goes to `target/sqldiff/`,
with a summary line each and `results.tsv`.

## Corpus format

One item per line; `#` starts a comment.

| Line | Meaning |
|---|---|
| `q:<sql>` | a query (COM_QUERY), compared |
| `p:<sql>` | prepare (COM_STMT_PREPARE); the statement id is remembered as `$1`, `$2`, ... |
| `e:<n>:<type>=<value>,...` | execute prepared statement n (`i` LONGLONG, `s` VAR_STRING, `n` NULL) |
| `x:<sql>` | run on both sides, not compared (setup and teardown) |
| `@maria <line>` / `@mysql <line>` | that side only |

`<name>.maria.diff` and `<name>.mysql.diff` run on one side only.
`infile/` holds the files LOCAL INFILE statements send.

## Floors and what is known to differ

`floors.tsv` records, per corpus and side, how many statements matched byte
for byte on a known build; `run.sh` fails when a corpus matches fewer. The
seeded floors are the counts the SQL work recorded when each corpus was
written (20 to 27 November 2026); corpora without a floor are reported but
not judged until `run.sh --record` writes theirs. Below a corpus's total,
the differences are known ones: mostly column metadata that follows the
oracle's plan (lengths, flags, decimals of derived columns), and a few
server quirks, each described in the corpus's comments or the SQL
compatibility notes.

## Validations

`validations/vdiff.py` compares `CREATE VALIDATION` on SprigDB with the
equivalent `BEFORE` trigger and `SIGNAL` (or a `CHECK` constraint) on the
oracle, by outcome (accepted, refused with the same message, other error):
`python3 tools/sqldiff/validations/vdiff.py maria tools/sqldiff/validations/vd1.diff`.
It uses the `mysql` client and the same `SQLDIFF_*` endpoints.

All corpora were written for SprigDB against black-box oracles; none comes
from the servers' own test suites.
