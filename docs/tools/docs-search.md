---
title: Searching the docs
audience: [user, dba, operator]
sidebar:
  order: 4
---

# Searching the docs

Every SprigDB release carries these pages as a search index, the **docs pack**. A running SprigDB loads it, so anyone signed in can search the docs of the exact release they use: from SQL, over REST, offline, with nothing sent anywhere.

The pack holds the pages cut into chunks of a few hundred words, each with its page, its heading path, its audience, the exact identifiers it mentions (error codes and names, SQLSTATEs, setting keys) and an embedding from a small model that ships with SprigDB (BAAI/bge-small-en-v1.5). Searches combine keyword search with similarity of meaning, so "why is replication slow" finds the page about replica lag even when it uses other words, and `39989` finds that error's entry exactly.

## How it gets there

The pack is a file shipped beside the `sprigdb` binary: `sprig-docs-<version>.pack`, or the file `docs.pack_path` names. Each node reads it into memory when it starts, after checking its SHA-256, and serves every docs search from that copy. Nothing is written anywhere, so nobody can change the docs a node serves. An upgrade brings the new release's pack with the new binary, so the docs always match the release that answers. A node also looks at the file again every minute and reads it again when it changed. A file that no longer reads, for example a damaged one, leaves the copy it had in place.

Without a pack, searches fail with 39989 `ER_SPRIG_DOCS_UNAVAILABLE` (retryable). The gateway logs the events `docs.pack.loaded` (with the release, the SHA-256 and the chunk count) and `docs.pack.unavailable` (no valid pack found).

## From SQL: the sprig_docs database

Every session can read `sprig_docs.chunks`, on any port that speaks SQL, whatever its grants. Nobody can change it.

Each node answers these statements from its copy of the pack in memory, so they read exactly the docs its REST search reads. `MATCH ... AGAINST` on `text`, `heading_path` or `terms` (any of them) ranks by the docs search's own keyword score (BM25 over the heading path, the text and the identifiers), best first.

```sql test
DESCRIBE sprig_docs.chunks;
```

```text output
Field	Type	Null	Key	Default	Extra
version	varchar(32)	NO	PRI	NULL	
chunk_id	binary(16)	NO	PRI	NULL	
page	varchar(255)	NO		NULL	
title	varchar(255)	NO		NULL	
heading_path	varchar(1024)	NO		NULL	
audience	set('user','dba','operator')	NO		NULL	
text	mediumtext	NO	MUL	NULL	
tokens	smallint(5) unsigned	NO		NULL	
terms	text	NO		NULL	
embedding	vector(384)	NO	MUL	NULL	
seq	smallint(5) unsigned	NO		0	
```

| Column | What |
|---|---|
| `version` | The release. `sprig_docs.chunks` shows only the running release's rows |
| `chunk_id` | The chunk's id, the same in every copy of the pack |
| `page` | The page, as a path under the docs, such as `security/tenant-administration.md` |
| `title` | The page's title |
| `heading_path` | The page title and the headings above the chunk, joined by ` > ` |
| `audience` | Who the page is for: `user`, `dba`, `operator` |
| `text` | The chunk's Markdown |
| `tokens` | Its length in the model's tokens |
| `terms` | The exact identifiers the chunk mentions, for keyword search |
| `embedding` | The chunk's embedding, 384 numbers |
| `seq` | The chunk's position in its page, from 1. A page's chunks in `seq` order are the page. 0 in the docs of a release whose pack predates positions |

Keyword search uses the full-text index on `text`:

```sql test
SELECT page, heading_path
FROM sprig_docs.chunks
WHERE MATCH(text) AGAINST ('suspend tenant')
LIMIT 3;
```

Search by meaning with `sprig_docs.embed('<text>')`, which gives a question's embedding from the same model, and the vector index:

```sql
SELECT page, heading_path
FROM sprig_docs.chunks
ORDER BY VEC_DISTANCE_COSINE(embedding, sprig_docs.embed('why is replication slow'))
LIMIT 5;
```

Filter by audience as with any `SET` column:

```sql test
SELECT COUNT(*) > 0 AS has_operator_pages
FROM sprig_docs.chunks
WHERE FIND_IN_SET('operator', audience);
```

```text output
has_operator_pages
1
```

The rules:

- Reads work: `SELECT` and `WITH`, and `DESCRIBE` and `SHOW COLUMNS` of `sprig_docs.chunks`. A statement may read one table once: a join of the docs with themselves, also through a CTE, a derived table or a subquery, is refused with 1041. It may read at most 1,000,000 rows and return 32 MiB (1041 past either), and it may run for at most 30 seconds (1969). `KILL QUERY` stops it (1317).
- Writes and DDL fail with 1044, as writes to `information_schema` do on MySQL.
- `USE sprig_docs` fails with 39051: name the table as `sprig_docs.chunks`.
- One statement can't join the docs with your own tables: your tables aren't there (1146).
- `sprig_docs.embed` takes a string literal only. It needs the model on the gateway (`docs.embed_queries` on, and the model in `docs.model_dir`); without it, it fails with 39989.
- `sprig_docs` is a reserved name: no tenant can create a database with it, on any protocol.
- Prepared statements, MongoDB and Redis can't read `sprig_docs`; use SQL text or REST.

```sql test-error
DELETE FROM sprig_docs.chunks;   -- 1044: the docs are read-only
```

## Over REST: POST /v1/docs:search

`POST /v1/docs:search` runs the hybrid search: the 40 best chunks by keywords and the 40 nearest by meaning, merged by reciprocal-rank fusion, at most two chunks per page. A question that names an exact identifier (an error code or name, a SQLSTATE, a setting key) gives keyword matches three times the weight, since the embedding model doesn't know those names. Any signed-in user can call it: Basic authentication, a Studio session or an API key (see [REST and gRPC](../connect/rest-and-grpc.md)).

| Field | Required | What |
|---|---|---|
| `query` | yes | The question or words to search for |
| `audience` | no | `user`, `dba` or `operator`: only chunks for that audience |
| `limit` | no | How many hits, at most 50. Default `docs.search_limit` (8) |
| `version` | no | The release to search. A node serves its own release's docs; another release fails with 503 and 39989 |

```sh
curl -s -u acme.app:'<password>' -X POST https://sprig.example.com/v1/docs:search \
  -H 'content-type: application/json' \
  -d '{"query": "how do I suspend a tenant", "limit": 2}'
```

```json
{
  "version": "0.9.0",
  "mode": "hybrid",
  "hits": [
    {
      "chunk_id": "5c7bd6d31707d76ff6de015634a48081",
      "page": "security/tenant-administration.md",
      "title": "Tenant administration",
      "heading_path": "Tenant administration > Suspension",
      "audience": ["dba", "operator"],
      "text": "…",
      "score": 0.0328,
      "bm25_rank": 1,
      "vector_rank": 1
    }
  ]
}
```

- `mode` is `hybrid`, or `keyword` when the question wasn't embedded: `docs.embed_queries` is off, or the gateway couldn't load the model (counted in `sprig_docs_search_degraded_total`). Keyword search still answers.
- `bm25_rank` and `vector_rank` are the hit's place in each list, or `null` when it wasn't in that list. `score` is the fused score.
- An empty `query`, an unknown audience, or a `limit` that isn't a positive integer fails with 400. A `version` that isn't loaded fails with 503 and 39989.

## Over REST: GET /v1/docs/pages/{page}

`GET /v1/docs/pages/{page}` returns one page of the docs, rebuilt from the pack: its chunks in page order and the page's Markdown. `{page}` is the page's path under the docs, slashes included, such as `tools/studio.md`. The `.md` may be left out, and a directory means its `README.md`. Add `?version=` for another loaded release. The same callers can use it as search. Studio's Docs panel shows pages this way.

```sh
curl -s -u acme.app:'<password>' https://sprig.example.com/v1/docs/pages/tools/docs-search.md
```

```json
{
  "version": "0.9.0",
  "page": "tools/docs-search.md",
  "title": "Searching the docs",
  "audience": ["user", "dba", "operator"],
  "ordered": true,
  "markdown": "# Searching the docs\n\nEvery SprigDB release carries these pages…",
  "chunks": [
    {"chunk_id": "…", "heading_path": "Searching the docs", "text": "Every SprigDB release…", "seq": 1}
  ]
}
```

- `markdown` is the page without its front matter and HTML comments. The headings that chunks keep only in their `heading_path` are written back, and a long section's pieces are joined again.
- `ordered` is `true` when the chunks came in their `seq` order. A pack from before chunk positions has none: then the chunks are ordered by their headings, siblings by name, `ordered` is `false`, and the page's sections may be out of order.
- A path that isn't a page path fails with 400, and so does a page the release doesn't have. Docs that aren't loaded fail with 503 and 39989.

### The pack's format

The pack has format 2 since chunk positions (`seq`) were added. A format 1 pack still reads, with every `seq` 0.

## Settings

All are operator settings, live, at data-centre or global scope. See [Settings](../reference/settings.md).

| Setting | Default | What |
|---|---|---|
| `docs.enabled` | `true` | Load the docs and serve docs search |
| `docs.pack_path` | empty | The pack each node reads; empty: `sprig-docs-<version>.pack` next to the `sprigdb` binary |
| `docs.search_limit` | `8` | Hits returned when a request gives no `limit` |
| `docs.embed_queries` | `true` | Embed questions for hybrid search; off: keyword search only, and the model is never loaded |
| `docs.embed_threads` | `2` | CPU threads one query's embedding may use |
| `docs.model_dir` | empty | The model's directory (`config.json`, `tokenizer.json`, `model.safetensors`); empty: `models/bge-small-en-v1.5` next to the `sprigdb` binary |

The gateway loads the model on the first search that needs it.

## Monitoring

| Metric | What |
|---|---|
| `sprig_docs_pack_info{version,sha}` | The pack the node read and serves |
| `sprig_docs_load_seconds` | Time to read the pack into memory |
| `sprig_docs_search_total{result}` | Searches by result: `ok`, `empty`, `invalid`, `refused`, `unavailable`, `error` |
| `sprig_docs_search_seconds{mode}` | Search time: `hybrid`, `keyword` or `sql` |
| `sprig_docs_search_degraded_total` | Searches answered by keywords only because the model couldn't run |

See [Metrics](../reference/metrics.md).

## Troubleshooting

| Symptom | What to do |
|---|---|
| 39989 on every search | The node has no pack. Check `docs.enabled`, that the pack is next to the binary or at `docs.pack_path`, and the gateway's `docs.pack.unavailable` event |
| 39989 from `sprig_docs.embed` only | The gateway has no model: check `docs.embed_queries` and `docs.model_dir` |
| `mode` is always `keyword` | Same cause; `sprig_docs_search_degraded_total` rises |
| Results from an older release | The pack beside the binary is an older release's (the gateway logs a warning when the pack's release isn't its own); install the release's pack |
