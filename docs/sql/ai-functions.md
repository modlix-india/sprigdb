---
title: AI functions in SQL
audience: [user, dba]
sidebar:
  order: 12
---

# AI functions in SQL

Five functions call a language model or an embedding model from a statement: embed text, classify it, pull structured data out of it, summarize it, or complete a prompt. SprigDB answers them in the gateway, so MariaDB never sees them. They follow the same rules as the AI assistants: providers that operators configure, daily token budgets, an audit trail, and no row value leaves SprigDB unless the tenant allows it.

| Function | Returns |
|---|---|
| `AI_EMBED(text [, model])` | The text's embedding, a `VECTOR(384)` value with the bundled model |
| `AI_CLASSIFY(text, label1, label2, … [, model])` | One of the labels, as written in the call |
| `AI_EXTRACT(text, json_schema [, model])` | `JSON` that fits the schema |
| `AI_SUMMARIZE(text [, max_words [, model]])` | Text of at most `max_words` words (default 100, at most 2000) |
| `AI_COMPLETE(prompt [, model])` | The model's answer, as text |

```sql
-- Nearest products to a phrase, with the bundled model (nothing leaves SprigDB).
SELECT id, name FROM products
ORDER BY VEC_DISTANCE_COSINE(embedding, AI_EMBED('red running shoes'))
LIMIT 10;

-- Label and summarize support tickets.
SELECT id,
       AI_CLASSIFY(body, 'bug', 'feature request', 'question') AS kind,
       AI_SUMMARIZE(body, 25) AS gist
FROM tickets WHERE created_at > NOW() - INTERVAL 1 DAY;

-- Pull fields out of free text.
SELECT id, AI_EXTRACT(note, '{"type":"object","properties":{"city":{"type":"string"},"date":{"type":"string"}},"required":["city"]}') AS facts
FROM deliveries LIMIT 50;
```

## Turning them on

A tenant administrator turns the functions on:

```sql
ALTER TENANT SPRIG SET (ai.functions.enabled = TRUE);
```

Without it, any call fails with 39991 `ER_SPRIG_AI_DISABLED`.

```sql test-error
SELECT AI_EMBED('red running shoes');  -- 39991 while ai.functions.enabled is off
``` It is separate from `ai.enabled`, which turns on the assistants.

- **`AI_EMBED` with the bundled model** (`local:bge-small`, the default) runs inside the gateway. Nothing is sent anywhere, no budget is spent, and `ai.share_data` doesn't matter.
- **Every other call goes to a provider**, so the values in it leave SprigDB. That needs `ai.share_data` on for every database the statement names, and for the session's database (the tenant's value when it names none). Otherwise the statement fails with 39995 `ER_SPRIG_AI_EGRESS_DENIED` before anything is sent.
- **The model.** Without a model argument, `AI_CLASSIFY`, `AI_EXTRACT`, `AI_SUMMARIZE` and `AI_COMPLETE` use `ai.functions.model`, or `ai.model.data` when that is empty. A model argument (`'anthropic/claude-haiku-5-5'`) must be that model or one listed in `ai.functions.models`; operators set both. With no model at all, or one not offered, the call fails with 39990 `ER_SPRIG_AI_NOT_CONFIGURED`. For `AI_EMBED`, the model is an alias of `ai.embedding_models` (the same aliases as [embedded columns](../data-models/vectors.md)); `local:bge-small` is the default. Only aliases of the bundled model run in `AI_EMBED` so far: a provider's embedding model fails with 39990, and an alias that isn't listed with 39997 `ER_SPRIG_EMBEDDING_FAILED`.

## Where they can stand

- **With a literal input** (a string, a number or `NULL`), a call can stand anywhere: in `WHERE`, `ORDER BY`, `INSERT … VALUES`, `UPDATE … SET`. SprigDB answers it before the statement runs and puts the answer in its place.
- **Over row values** (a column, or an expression of columns), a call must be a whole item of the outermost `SELECT` list, with or without an alias. SprigDB runs the statement with the call's input in that column, reads the rows, answers all their inputs at once, and returns the rows with the answers in that column. Everything else about the statement applies as usual: privileges, row-level security, sharding and the transaction. So a function only ever sees values you can read.

These are refused with 1235, since the statement would act on the inputs rather than the answers:

- a call over row values anywhere else: in `WHERE`, inside another expression, in a subquery, in `INSERT … SELECT` or `UPDATE … SET`;
- with `SELECT DISTINCT`, `UNION`, `EXCEPT`, `INTERSECT` or `SELECT … INTO`;
- when `GROUP BY`, `HAVING`, `ORDER BY` or `WINDOW` names the call's column, by alias or by position.

To filter or sort on an answer, select it and filter in your application, or store it first (for embeddings, use an [embedded column](search-and-vectors.md)).

Other rules:

- The arguments after the input must be literals (1210 otherwise). Calls don't nest (1210).
- A wrong number of arguments fails with 1582. `AI_CLASSIFY` needs at least two labels, all different (1210). For `AI_CLASSIFY`, a last argument that names a model offered to the functions is the model; any other is a label.
- They work in plain statements only, not in prepared ones (39051): run them with the text protocol.
- A statement with calls over its rows may return at most 100,000 rows (39601); add a `LIMIT` or a narrower `WHERE`.
- The five names are taken: an unqualified call is always the AI function, never a stored function of the same name. A qualified call (`db.ai_embed(…)`) is the stored function.

## Values

- **`NULL` or blank input** gives `NULL`, and nothing is sent.
- **Equal inputs** in one statement are answered once.
- **`AI_EMBED`** returns the vector's bytes as MariaDB stores a `VECTOR`: you can compare it with `VEC_DISTANCE_*`, insert it into a `VECTOR(384)` column, or read it with `VEC_ToText`. Text is embedded as a passage, with no instruction in front, so `AI_EMBED(name)` equals what an embedded column of `name` holds.
- **`AI_CLASSIFY`** returns the label as written in the call. An answer that isn't one of the labels gives `NULL`.
- **`AI_EXTRACT`** returns compact JSON. Its top-level type and an object's `required` properties are checked; an answer that doesn't fit gives `NULL`. The column's type is `JSON`.
- **`AI_SUMMARIZE`** and **`AI_COMPLETE`** return text. An answer cut off at `ai.functions.max_output_tokens` is returned as it is.
- An input longer than 64 KiB is cut at that length before it is sent; the bundled model reads at most 512 tokens.

Inputs are treated as data. The prompts put each value inside tags and tell the model not to follow instructions in it. Whatever comes back is only a value: nothing runs it.

## Cache

Answers from a provider are kept in the tenant's catalog, keyed by the SHA-256 of the function, the model, the other arguments and the input. The same call with the same input within `ai.functions.cache_days` (30) is answered from the cache and sends nothing. The cache holds at most `ai.functions.cache_max_entries` answers (100,000); past that, the least recently used go first. Answers that weren't valid, or were cut off, aren't kept. The bundled model's vectors aren't cached: computing them again is cheaper.

## Budgets and limits

The functions share the assistants' daily token budgets, `ai.budget.tenant_tokens_day` and `ai.budget.user_tokens_day`; their tokens are counted under the role `functions`. The budgets are checked before each wave of up to four provider calls. When one is spent:

- with `ai.functions.on_budget = error` (the default), the statement fails with 39992 `ER_SPRIG_AI_BUDGET_EXCEEDED`;
- with `ai.functions.on_budget = null`, the rows not yet answered get `NULL`, and the statement ends with warning 39992 (`SHOW WARNINGS`).

One statement may send at most `ai.functions.max_inputs` distinct inputs (10,000) to the provider; more fail with 39992 before anything is sent. Cache hits and the bundled model don't count.

`AI_CLASSIFY` sends up to 32 inputs in one call; the other functions send one input a call, up to four calls at a time.

## Errors

| Code | When |
|---|---|
| 39991 `ER_SPRIG_AI_DISABLED` | `ai.functions.enabled` is off |
| 39990 `ER_SPRIG_AI_NOT_CONFIGURED` | No model, or a model that isn't offered |
| 39995 `ER_SPRIG_AI_EGRESS_DENIED` | A provider model with `ai.share_data` off |
| 39992 `ER_SPRIG_AI_BUDGET_EXCEEDED` | A daily budget is spent, or too many inputs |
| 39993 `ER_SPRIG_AI_PROVIDER_ERROR` | The provider failed after retries (retryable) |
| 39994 `ER_SPRIG_AI_REFUSED` | The model declined |
| 39997 `ER_SPRIG_EMBEDDING_FAILED` | `AI_EMBED` names an alias `ai.embedding_models` doesn't list, or the bundled model couldn't be loaded on the gateway |
| 1210, 1582, 1235 | Wrong arguments, a wrong count, or a call where it can't stand |

## Audit and metrics

Each statement that calls a model records an `ai.function` audit event: the user, the function, the model, the counts (rows, distinct inputs, cache hits, answers, `NULL`s for the budget, invalid answers), the tokens, and the SHA-256 of each request sent. The values themselves aren't kept.

`sprig_ai_fn_calls_total{fn,model,cached}` counts the distinct inputs answered, and `sprig_ai_tokens_total{role="functions"}` the tokens.

## Settings

| Key | Default | Scope | Meaning |
|---|---|---|---|
| `ai.functions.enabled` | `false` | tenant | Turn the functions on |
| `ai.functions.model` | empty | global (operators) | The default model; empty uses `ai.model.data` |
| `ai.functions.models` | empty | global (operators) | Other models a call may name |
| `ai.functions.cache_days` | `30` | tenant | Cache life; 0 turns the cache off |
| `ai.functions.cache_max_entries` | `100000` | tenant (operators) | Cache size |
| `ai.functions.on_budget` | `error` | tenant | `error` or `null` |
| `ai.functions.max_output_tokens` | `1024` | global (operators) | Output tokens per provider call |
| `ai.functions.max_inputs` | `10000` | tenant (operators) | Distinct inputs a statement may send |

## MongoDB

The same functions are aggregation operators: `$aiEmbed`, `$aiClassify`, `$aiExtract`, `$aiSummarize` and `$aiComplete`. See [Documents](../data-models/documents.md#ai-operators).
