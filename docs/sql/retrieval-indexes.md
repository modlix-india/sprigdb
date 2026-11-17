---
title: Retrieval indexes
audience: [user, dba]
sidebar:
  order: 13
---

# Retrieval indexes

A retrieval index makes a table's text searchable by meaning and by keywords at once. SprigDB cuts each row's text into chunks, embeds every chunk with a model, indexes the chunks for full-text search, and keeps all of it in step with the table as rows are inserted, changed and deleted. You search with the `RETRIEVE()` table function in SQL or with one REST call, and you get back the best chunks with the keys of their rows. Use it to answer questions from your own documents, tickets or articles (retrieval-augmented generation), or for a search box that understands what people mean.

Every search applies the caller's privileges and [row-level security](row-level-security.md): a user only ever gets chunks of rows they could `SELECT`.

```sql test
CREATE TABLE articles (
  id      BIGINT PRIMARY KEY,
  title   VARCHAR(200),
  body    MEDIUMTEXT,
  product VARCHAR(20),
  lang    CHAR(2)
);

INSERT INTO articles VALUES
  (1, 'Reset your password', 'Open Settings, choose Account, then Reset password. We email you a link that works for one hour.', 'app', 'en'),
  (2, 'Change your plan', 'Plans can be changed at any time from Billing. The new price applies from the next invoice.', 'app', 'en'),
  (3, 'Export your data', 'Admins can export every table as CSV from the Data page.', 'web', 'en');

CREATE RETRIEVAL INDEX kb ON articles (body)
  TITLE title
  FILTERS (product, lang);
```

The worker indexes the rows in the background, within a few seconds:

```sql test
SELECT SLEEP(15) INTO @waited;
SHOW RETRIEVAL INDEXES;
```

```text output
Index	Table	Column	Model	State	Rows	Shards	Error
kb	articles	body	local:bge-small	ready	3	1	NULL
```

```sql test
SELECT id, title, `rank`
FROM RETRIEVE(kb, 'how do I reset my password', 2, 'product = ''app''')
ORDER BY `rank`;
```

```text output
id	title	rank
1	Reset your password	1
2	Change your plan	2
```

## CREATE RETRIEVAL INDEX

```sql
CREATE RETRIEVAL INDEX [IF NOT EXISTS] index_name
  ON [database.]table (text_column)
  [TITLE title_column]
  [FILTERS (column [, column] ...)]
  [CHUNK BY HEADINGS | CHUNK BY TOKENS n]
  [EMBED WITH MODEL 'model_alias'];
```

| Part | Meaning |
|---|---|
| `index_name` | Up to 64 characters. The index belongs to the table's database; names are unique within a database. |
| `text_column` | The text to search: a `CHAR`, `VARCHAR` or `TEXT` column. A row whose text is `NULL` or blank has no chunks. |
| `TITLE` | A string column embedded and searched with every chunk of its row, and returned with each hit. |
| `FILTERS` | Up to 16 columns you can filter searches on: numbers, dates, short strings, `ENUM` and `SET` (not `TEXT`, `BLOB`, `JSON`, binary, spatial or vector columns). |
| `CHUNK BY TOKENS n` | The default, with `n` = 400: cut the text into pieces of at most about `n` tokens (32 to 480), between paragraphs, then sentences, then words. |
| `CHUNK BY HEADINGS` | For Markdown: cut at `#`, `##` and `###` headings, long sections again between paragraphs; code blocks and table rows stay whole. Each chunk's title is the row's title followed by the headings above it, such as `Account help > Reset`. |
| `EMBED WITH MODEL` | An alias from `ai.embedding_models`; the default is `local:bge-small`, the bundled model (BAAI's bge-small-en-v1.5, English, 384 dimensions). It runs inside SprigDB, so no row leaves it. A provider's model is refused for now (39997). |

Rules:

- **Privileges:** `INDEX` on the table.
- The table needs a primary key: hits are returned with it, and searches check each hit's row through it.
- The statement returns at once. The index starts in state `building`: a worker cuts and embeds the existing rows, at most `ai.embedding.backfill_rows_per_s` rows a second (500 by default), and follows new changes meanwhile. Searches during the build find the rows indexed so far. `SHOW RETRIEVAL INDEXES` shows the progress.
- A row keeps at most `ai.retrieval.max_chunks_per_row` chunks (64); the rest of a longer text isn't indexed. Each chunk is embedded with its title, cut to the model's 512 tokens.

## Keeping in step

A worker follows the table's [change feed](../concepts/change-feed.md):

- **Inserts and updates** are indexed within a second or two. An update that doesn't change the text, the title, a filter column or the key costs nothing.
- **Deletes** remove the row's chunks. A deleted row never shows up in a search even before that, because every hit is checked against the table.
- **Exactly once.** The worker records how far it has read the feed only after its writes commit, and replaces a row's chunks only if the row still holds the text it embedded. A worker killed in the middle is replaced, reads the last changes again and passes over the rows it had already done.
- **Shards.** Each shard of the table keeps the chunks of its own rows, next to them, and each chunk carries its row's routing hash. When a shard is split, merged or moved, its chunks move with its rows, so searches find them in the new shards as soon as the move is done. The worker then checks the new shards: it reads each row's hash and embeds again only a row whose chunks are missing or out of date, and the index stays `ready`. Moving a table into a table group moves its chunks with it too. One case makes a shard indexed again from scratch (state `rebuilding`; meanwhile searches miss that shard's rows): a re-key, whose new routing can't be computed from a chunk. A shard that arrives without its chunks for any other reason is filled the same way.

| State | Meaning |
|---|---|
| `building` | Existing rows are being indexed |
| `ready` | Every shard is indexed; changes are followed |
| `rebuilding` | A shard is being indexed again from scratch: after a re-key, or when a shard arrived without its chunks |
| `failed` | Stopped on an error, shown in `SHOW RETRIEVAL INDEXES`: for example, a column the index reads was dropped, or the model isn't available. The worker tries again every 30 to 60 seconds. |

## RETRIEVE()

```sql
SELECT ... FROM RETRIEVE([database.]index_name, 'query', limit [, 'filter']) [AS alias] ...
```

A table function: write it where a table goes in `FROM`, alone or joined to other tables. It returns up to `limit` rows, best first:

| Column | Meaning |
|---|---|
| the key columns | The row's primary key, named as in the table |
| `chunk_no` | The chunk's place in its row, from 0 |
| `title` | The row's title (with `CHUNK BY HEADINGS`, and the headings above the chunk) |
| `chunk` | The chunk's text |
| `score` | The fused score: higher is better |
| `rank` | 1 for the best hit, then 2, … |
| `bm25_rank` | The chunk's rank among the full-text matches, or `NULL` |
| `vector_rank` | The chunk's rank among the nearest embeddings, or `NULL` |

```sql
-- The best chunks, with columns of their rows
SELECT a.id, a.title, r.chunk, r.score
FROM RETRIEVE(kb, 'refund for a cancelled order', 8) AS r
JOIN articles AS a ON a.id = r.id
ORDER BY r.`rank`;

-- Only English articles of one product
SELECT id, chunk
FROM RETRIEVE(support.kb, 'two-factor login', 5, 'product = ''app'' AND lang = ''en''')
ORDER BY `rank`;
```

Order by `rank` (or `score`) when the order matters: like any derived table, the rows come in no guaranteed order otherwise.

How a search works:

1. The query is embedded with the index's model, in the gateway.
2. Each shard finds its 40 best chunks by full-text relevance and its 40 nearest chunks by cosine distance (`ai.retrieval.candidates`), with the filter applied.
3. The candidates' rows are read through your session: rows you can't `SELECT`, rows a row-level security policy hides from you, and rows that no longer exist are dropped. You also need `SELECT` on the text column (and the title column).
4. The two lists are fused with reciprocal-rank fusion (each chunk scores `1 / (60 + rank)` in each list it is in), keeping at most two chunks of a row. A query that names an exact identifier, such as an error code or a setting key, weighs the full-text list three times, since the model doesn't know such names.
5. When a policy hid so many candidates that fewer than `limit` hits are left, the search runs again with four times the candidates, up to 1,000 per list and shard.

**Ranking for a user that row-level security restricts.** MariaDB's full-text relevance weighs each word by how many chunks of the whole index contain it, so a plain full-text ranking would depend on rows you can't see. When a row-level security policy on the table applies to you, the full-text list is ranked over the rows you may see alone:

- Every chunk that contains a word of the query is a candidate, at most 1,000 per shard.
- The candidates whose rows you can read are ranked by BM25, with word counts and lengths taken from those chunks only. Equal scores go to the lower key.
- Both lists are cut at the same depth (40 per shard), counted in chunks you can see, so how many hidden rows there are changes nothing.

Your hits and ranks are then the same whether or not the hidden rows exist. A query whose words match more than 1,000 chunks in one shard is ranked by the vector list alone for you. Users no policy restricts keep the full-text relevance of the whole index. The nearest-chunk search uses MariaDB's approximate vector index, whose answers can still vary slightly with the rows in the index.

Arguments:

- **The index:** its name, or `database.name`.
- **The query:** a string literal of at most 2,000 characters.
- **The limit:** a number from 1 to 100.
- **The filter:** a string literal with a condition on the index's `FILTERS` columns: comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`), `IN`, `BETWEEN`, `LIKE`, `IS [NOT] NULL`, `AND`, `OR`, `NOT`, parentheses and literals. Another column, a function, a subquery or a variable is refused (1054). `NULL` or `''` means no filter.

The arguments must be literals, so `RETRIEVE()` can't be used in a prepared statement (39051), and a call anywhere but a `FROM` list isn't the table function (a stored function named `retrieve` keeps working there).

## REST

```http
POST /v1/databases/{database}/retrieval/{index}:search
Content-Type: application/json

{"query": "how do I reset my password", "limit": 5, "filter": "product = 'app'"}
```

`limit` is 8 when absent. Any signed-in user or API key may call it; the search runs as that caller, exactly as `RETRIEVE()` does. The answer, best first:

```json
{
  "database": "support",
  "index": "kb",
  "hits": [
    {"key": {"id": 42}, "chunk_no": 0, "title": "Account help",
     "text": "To reset your password, open Settings ...", "score": 0.0325,
     "bm25_rank": 1, "vector_rank": 2}
  ]
}
```

`key` holds the row's primary key columns; binary key columns are given in hexadecimal.

## DROP, SHOW CREATE and SHOW

```sql
DROP RETRIEVAL INDEX [IF EXISTS] [database.]index_name;
SHOW CREATE RETRIEVAL INDEX [database.]index_name;
SHOW RETRIEVAL INDEXES [FROM database];
```

- `DROP RETRIEVAL INDEX` needs `INDEX` on the table. Searches stop at once; the worker removes the chunks shortly after. Dropping the table or its database drops its indexes too.
- `SHOW CREATE RETRIEVAL INDEX` prints the index's statement with every clause, defaults included, so it can be run again elsewhere.
- `SHOW RETRIEVAL INDEXES` lists the database's indexes over tables you can read: `Index`, `Table`, `Column`, `Model`, `State`, `Rows` (rows indexed so far), `Shards` and `Error`.

Changing an index means dropping it and creating it again. A column the index reads can be dropped from the table, but the index then fails until it is dropped.

## Settings

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `ai.retrieval.candidates` | `40` | global | Hits a search takes from each shard's full-text list and vector list before fusing them |
| `ai.retrieval.max_chunks_per_row` | `64` | global | Chunks an index keeps of one row at most |
| `ai.embedding_models` | the bundled model | global | Model aliases offered to `EMBED WITH MODEL` |
| `ai.embedding.backfill_rows_per_s` | `500` | tenant, global | Existing rows an index's build reads per second at most |
| `ai.embedding.rows_per_s` | `2000` | tenant, global | Rows one index handles per second at most, changes and build together |
| `ai.embedding.batch_rows` | `64` | global | Chunks sent to the model at once |
| `ai.embedding.max_input_bytes` | `65536` | global | Text bytes of a row indexed at most |

The model is found in `docs.model_dir`, as for [embedded columns](../data-models/vectors.md#embedded-columns) and the docs search.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sprig_retrieval_searches_total` | counter | `result` | Searches: `ok`, `empty` or `error` |
| `sprig_retrieval_search_seconds` | histogram | `mode` | Time of one search: `hybrid`, or `keyword` when the query couldn't be embedded |
| `sprig_retrieval_rows_total` | counter | `result` | Rows a worker handled: `indexed`, `cleared` (chunks removed), `skipped` (already current), `superseded` (changed again first) |
| `sprig_retrieval_chunks_total` | counter | none | Chunks written |
| `sprig_retrieval_backlog` | gauge | none | Changed rows a worker has read and not yet indexed |
| `sprig_ai_embedding_seconds` | histogram | `model` | Time of one model batch, shared with embedded columns |

## Errors

| Code | Name | When |
|---|---|---|
| 39997 | `ER_SPRIG_EMBEDDING_FAILED` | The index's declaration refused (a provider's model, an unknown alias, a text or filter column of the wrong type, no primary key), or the model failed |
| 1061 | | The database has a retrieval index of that name |
| 1146 | | No such table, or no such retrieval index |
| 1054 | | An unknown column in the statement, or a filter naming something else than a `FILTERS` column |
| 1142 | | No `INDEX` on the table (create, drop), or no `SELECT` on it or its text column (search) |
| 1210, 1582 | | `RETRIEVE()` with arguments of the wrong kind or number |
| 39051 | `ER_SPRIG_NAME_REWRITE_UNSUPPORTED` | `RETRIEVE()` in a prepared statement |

## Related

- [Vectors](../data-models/vectors.md)
- [Search and vectors in SQL](search-and-vectors.md)
- [AI functions in SQL](ai-functions.md)
- [Row-level security](row-level-security.md)
- [Docs search](../tools/docs-search.md): the same search, over SprigDB's own documentation
