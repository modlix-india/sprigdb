---
title: AI assistants
audience: [user, dba, operator]
sidebar:
  order: 5
---

# AI assistants

SprigDB has two assistants for a tenant's users, reached through one chat API:

- **The data specialist** (`data`) explains your data, writes and runs read-only queries, and proposes charts and glossary terms.
- **The DBA assistant** (`dba`) helps with plans, indexes, workload, settings, shards, row-level security and logic.

Operators also have an **administration assistant** (`admin`), which sees no tenant data. It also advises on SprigDB's own errors, through the approvals inbox: see [AI incident advice](../operate/ai-incident-advice.md).

Every assistant follows the same rules:

- Its tools run **as you**, with exactly your rights: grants, row-level security and caps apply unchanged.
- It **never changes anything**. A statement that would change data, schema or settings comes back as a proposal, which you review and run yourself.
- What leaves SprigDB passes **one gate**: by default, schema and statistics only, never rows.
- Every call is **audited and budgeted**, and you can see exactly what was sent.

Nothing is sent anywhere until an operator configures a provider and a tenant administrator turns the assistants on. SprigDB doesn't host models: you use a provider you choose. Studio's chat panel isn't part of this release; the assistants are reached through the API described here, the [`ASK` statement](#ask-in-sql) and [`sprig ask`](sprig.md#sprig-ask).

Outside AI agents, such as Claude Code, can use the same tools as you through the [MCP server](mcp.md).

## Setting up

### 1. An operator adds a provider

SprigDB speaks two provider APIs:

- **anthropic**: the Anthropic Messages API;
- **openai**: OpenAI-compatible Chat Completions, which also covers Azure OpenAI, DeepSeek (`--base-url https://api.deepseek.com`), Ollama, vLLM, Mistral and Groq.

```sh
# A hosted provider: the key is read without echo, or from a file
sprigctl ai provider add anthropic --kind anthropic --key-file ~/.secrets/anthropic-key

# A local server that takes no key
sprigctl ai provider add ollama --kind openai \
  --base-url http://127.0.0.1:11434/v1 --no-key

sprigctl ai provider list
sprigctl ai provider test anthropic --model claude-opus-5-5
sprigctl ai provider remove ollama
```

- **The key** is read from `--key-file` (`-` reads standard input), or asked for on the terminal without echo. The controller seals it with the master key file (`[controller] workload_key_file`) and stores it in the global tier. No command or API ever shows it again: listings show only whether a key is set. Replacing a provider with `--keep-key` keeps its key.
- **Gateways** open the key with the same master key file (`[gateway] workload_key_file`), only when they call the provider.
- **`test`** makes one tiny call (at most 16 output tokens) and prints the answer, or the error that says why the call failed.
- **Other options:** `--key-header api_key` for Azure OpenAI, `--query api-version=…` for a query string on every call, and `--provider-ca-file` for a TLS-inspecting proxy.
- **The output limit:** for OpenAI itself, `add` names it `max_completion_tokens`, because `api.openai.com` refuses `max_tokens` on its current models; every other `openai` server gets `max_tokens`. `--max-tokens-field` chooses either.
- **Who:** changing providers needs a platform operator. Every change is audited on the platform audit stream, without the key.

Then choose a model for each assistant, as `provider/model`:

```sh
sprigctl settings set ai.model.data anthropic/claude-opus-5-5 --scope global --reason "data assistant"
sprigctl settings set ai.model.dba anthropic/claude-opus-5-5 --scope global --reason "DBA assistant"
```

`ai.effort.<role>` sets how hard the model thinks, for models that support it.

### 2. A tenant administrator turns them on

```sql
ALTER TENANT SPRIG SET (ai.enabled = TRUE);
```

Decide at the same time whether rows may leave SprigDB:

```sql
-- Off by default. On for one database only:
ALTER DATABASE shop SPRIG SET (ai.share_data = TRUE);
```

## What leaves SprigDB

| `ai.share_data` | Sent to the provider | Never sent |
|---|---|---|
| off (default) | your messages, names and types of tables and columns, plans, counts, null counts and distinct estimates, the glossary | rows and query results (the model gets the columns and the row count instead), minimum, maximum and common values |
| on | all of the above, plus results: at most 200 rows and 64 KiB per tool result | secrets |

- **Secrets** are always masked, in your messages and in every tool result: strings after `IDENTIFIED BY`, `PASSWORD`, `TOKEN` and similar words, words that look like API keys, and `Bearer` credentials.
- **Tool results are marked untrusted**, and the model is told that text inside them is data, never instructions. Text stored in your tables can't steer the assistant into doing anything, because the assistant can't change anything anyway.
- **The administration assistant** never receives tenant data: names in its input are replaced by stable placeholders such as `table#kqzmfa`. SprigDB's own documentation reaches it as written, except for names the scrubber knows.

## The chat API

`POST /v1/ai/chat` runs one turn and streams it as NDJSON, one JSON object per line. Sign in as for any other API call (a Studio session token, Basic, or an API key).

```sh
curl -sN http://127.0.0.1:8080/v1/ai/chat \
  -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"role": "data", "database": "shop", "message": "Which tables hold orders?"}'
```

The request:

| Field | Meaning |
|---|---|
| `role` | `data`, `dba`, or `admin` (platform operators only) |
| `message` | Your question, up to 32 KiB |
| `conversation` | Continue this conversation; a new one starts without it |
| `database` | The database the question is about: `ai.share_data` and the other settings are read at its scope |
| `context` | Up to ten items `{kind, ref}` added to the message: `query` (a statement), `table`, `note`, or `result` (rows you have on screen, refused with 39995 when `ai.share_data` is off) |

The stream:

| `type` | Fields | Meaning |
|---|---|---|
| `start` | `conversation`, `role`, `model`, `share_data` | The turn started; keep `conversation` to continue it |
| `text` | `text` | More of the answer. When a turn calls tools, the text of each later model call starts with a blank line (`\n\n`), so joining the `text` lines gives readable paragraphs |
| `thinking` | `text` | The model's reasoning, when the provider shows it |
| `tool_start` | `id`, `name`, `input` | A tool call started |
| `tool_end` | `id`, `name`, `ok`, `reduced` | It ended; `reduced` means rows or values were withheld or cut |
| `proposal` | `proposal: {id, kind, tool_use_id, body, checks?, generated?}` | Something for you to review: `sql`, `tile`, `glossary`, `recommendation` or `generate_rows`. It has not run. `checks` is what SprigDB found when it tried the proposal (see below); `generated` holds generated test rows, which only you get |
| `usage` | `usage` | Tokens of one model call |
| `sources` | `sources: [{kind, title, reference}]` | What the answer drew on: docs pages, tables, queries |
| `error` | `error: {code, name, message}` | The turn failed, or the model refused (39994) |
| `end` | `stop` | The turn ended: `end_turn`, `max_tokens`, `refusal` or `other` (`detail` says `max_tool_rounds` or `stopped`) |

Closing the connection stops the turn.

### Proposals

A proposal is never run by the assistant. For an SQL proposal, the stream carries the statement, why, and what it will do. You decide what happens to it:

- **Accept it** with `POST /v1/ai/proposals/{id}:accept`. It runs as you, with your own rights, exactly as if you had typed it.
- **Dismiss it** with `POST /v1/ai/proposals/{id}:dismiss`.
- **Run it yourself** through your usual client, and report the outcome through the feedback route.

```sh
curl -s http://127.0.0.1:8080/v1/ai/proposals/0193…:accept \
  -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"confirm": true, "note": "ran it in the quiet hour"}'
```

| Kind | Accepting it |
|---|---|
| `sql` | Runs the statement (or statements, in order, stopping at the first error) in the database the proposal names. The reply holds each statement's results: `columns`, `rows` (at most 1,000), `affected_rows`, `last_insert_id`, `warnings`. A plain read runs at once. Anything else needs `"confirm": true`, which a client sends only after showing the statement to you and getting your yes; without it the call fails with 39792 and nothing runs |
| `generate_rows` | Inserts the generated test rows, with `"confirm": true` |
| `recommendation` | Files an `ai_dba_advice` item in the approvals inbox, at your tenant's level. It is advice only: approving it later acknowledges it and changes nothing |
| `glossary` | Accepts the proposed term (tenant administrators, as for `POST /v1/ai/glossary/{term}:accept`) |
| `tile` | Not yet: tiles come with dashboards |

You can act only on proposals in your own tenant conversations, MCP calls included. A proposal that was accepted can't be accepted again; one that failed can. The optional `note` (up to 500 characters) is kept with the outcome. Each outcome, `accepted`, `failed` or `dismissed`, is recorded as feedback, so the assistants learn which proposals your team takes, and is counted in `sprig_ai_proposals_total` and written to the tenant's audit stream as an `ai.proposal` event.

**Checked before you see it.** A proposed schema change (`CREATE`, `ALTER`, `RENAME`) must pass the same rules as the DBA assistant's `check_ddl` tool before it is shown. One that fails goes back to the assistant to fix, so a proposed `CREATE TABLE` always parses on SprigDB. Row-level security policies are tried as test users, logic is compiled, and test rows are generated before they are shown, as the next sections describe.

### What the assistants can read

The tools each assistant may call, all as you:

- **Both:** search these docs; list databases and tables; describe a table (columns, indexes, definition); plans (`EXPLAIN FORMAT=JSON`, inside a read-only transaction for reads); column statistics over at most 100,000 rows; the approvals inbox.
- **Data specialist:** run one `SELECT` in a read-only transaction that is rolled back, capped at its limit (1 to 1,000 rows) and at `studio.statement_timeout_ms`.
- **DBA assistant:** workload fingerprints, indexes and automatic indexes, settings as they resolve for the tenant, a database or a table (`SHOW SPRIG SETTINGS … FOR`), shards and advisor runs, the row-level security tester (counts only), logic compiles (dry runs), SprigDB's decisions about your tenant, two schema checks, and the sensitive-data scanner (below).

A tool call outside the assistant's role, or with invalid input, is refused; three refusals in one turn stop it with 39996.

When `ai.share_data` is off, a query's rows are withheld and the assistant gets only its columns and row count. It then offers you the query as a proposal, so you can run it and see the rows yourself. An MCP client that isn't offered `propose_sql` is told to give you the query instead.

### Schema design and migrations

The DBA assistant checks schemas with SprigDB's own rules, not the model's guesses:

- **`check_ddl`** reads schema statements without running them. Each one goes through the same name rules every statement meets, then SprigDB's DDL rules: names over 64 characters, storage engines, `DEFINER` clauses, `FULLTEXT` keys, generated columns, and the statement parser. The assistant runs it on every schema change before proposing one. Ask it to design tables from a description, and it writes plain MySQL DDL, checks it, and proposes what passed.
- **`migration_check`** reads a schema you're bringing to SprigDB. For MySQL or MariaDB, paste the DDL from `mysqldump --no-data` (up to 512 KiB at a time). For MongoDB, paste the collections as `db.getCollectionInfos()` and `getIndexes()` print them, as a JSON array of `{name, options, indexes}`. The report is the same every time for the same schema:

| Part | Holds |
|---|---|
| `findings` | Each one with a level: `error` (won't run as written, with the error code), `change` (runs, but SprigDB does something different, such as `ENGINE=MyISAM` becoming InnoDB or a `FULLTEXT` key becoming a search index), or `note` |
| `groups`, `reference`, `unsharded`, `pins` | Table groups along your foreign keys, small code tables to pin `REFERENCE`, and the `SPRIG PIN` statements for them. Pins are optional: without them the sharding advisor designs groups from your workload |
| `load_order` | The tables, parents first |
| `plan` | The import steps: dump options, the port to load through, pins, loading, checks |

For MongoDB, sample documents are never read: only collection options, validators and indexes. The checks follow the [MySQL and MariaDB](../connect/mysql-and-mariadb.md) and [MongoDB](../connect/mongodb.md) pages: views, time-series and clustered collections, and `2d`, `geoHaystack` and wildcard indexes are errors (39662); sparse, partial and hidden index options are kept but don't change what the index holds.

### Finding sensitive data

Ask the DBA assistant which columns hold personal or sensitive data. Its `sensitive_columns` tool scans a database, or one table, and decides by fixed rules, so the same schema gives the same answer:

- **Names, types, comments and constraints.** Column names are split into words (`customerEmail`, `date_of_birth`), and each category has its words: email, phone, birth date or age, national, tax, passport and licence numbers, card numbers and security codes, bank accounts and IBANs, addresses and postcodes, people's names, IP addresses (and `INET4`/`INET6` columns), locations (and spatial columns), passwords, keys and tokens, special categories (health, sex, ethnicity, religion and the like), salaries and credit scores, device ids, and long free text such as `notes`. A comment that says `PII`, `GDPR` or `sensitive`, or a `CHECK` that makes a column look like an email, counts too. Flags, counts and kinds of the data aren't the data: `email_verified`, `phone_type` and `TINYINT(1)` columns are left alone.
- **Values, only with `ai.share_data` on.** Up to 200 values of each text column (50 columns a table, 20 tables a call) are read in a read-only transaction and matched against patterns: email addresses, phone numbers, card numbers that pass the Luhn check, US social security numbers, IBANs with valid check digits, IP addresses, and tokens or keys. Only the counts leave the gateway ("188 of 200 sampled values look like an email"), never a value. Matches raise a finding's confidence or find a column whose name hides its data; a large sample with no match lowers it.

Each finding has a category, a confidence (`high`, `medium`, `low`) and its reasons, and the result says which tables have row-level security on. The assistant then suggests what SprigDB has: row-level security for whose rows a user sees, column-level grants or a view for which columns, and residency, which operators set for a whole tenant. SprigDB has no column masking.

### Row-level security from a sentence

Describe who should see what, such as "sales reps see only the orders of their region", and the DBA assistant writes the policy, binding it to a value per user:

| Session function | When |
|---|---|
| `SPRIG_USER_ATTR('key')` | The value is stored on each account: `ALTER USER bob ATTRIBUTE '{"region": "EU"}'` |
| `SPRIG_CLAIM('key')` | The value is a claim of the identity provider token the session logged in with |
| `CURRENT_USER()` | A column holds account names (`name@host`) |

Before it proposes a policy, the assistant tries it. `test_policy` takes a `CREATE POLICY` that doesn't exist yet: SprigDB parses it and checks its expressions as `CREATE POLICY` would, binds its `USING` to the test user's own values (account, id, tenant, attributes, and claims you supply), and counts as that user, through the row-level security tester:

- `rows_user_sees_now`: the rows of the table the user sees today;
- `rows_new_policy_admits`: how many of those the new policy's `USING` lets through;
- whether the policy's `TO` list applies to the user, and the attributes and claims it reads that the user lacks, with how to set them (`ALTER USER … ATTRIBUTE`, or a token claim).

Only counts come back, never rows, and nothing is created. The tester's rules apply: you need `SPRIG_TENANT_ADMIN`, `studio.view_as` must be on, and administrators of your tenant can't be tested as.

The proposal itself comes from `propose_policy`, with the statements (`CREATE POLICY`, and `ALTER TABLE … ENABLE ROW LEVEL SECURITY` when it's off) and the users to try them as. Every policy is tried again as each of those users before you see it, and the proposal's `checks` carries the counts and warnings, such as "enabling row-level security hides every row from users no policy admits". A statement that doesn't parse, or an expression a policy can't use, goes back to the assistant to fix. Accepting it runs it as any SQL proposal.

### Writing logic

Ask for a function, procedure, validation or after-commit trigger, in KIRun or JavaScript, and the DBA assistant writes the whole `CREATE … LANGUAGE … AS …` statement and checks it with `compile_logic`, a dry run of `POST /v1/databases/{db}/logic:compile` that records and deploys nothing:

- functions and procedures compile and are placed in a tier, as before;
- validations and `AFTER COMMIT` triggers get the deploy's own checks against their table: the table exists, the code compiles into MariaDB, its parameters name the table's columns, and how a validation would be enforced (a `CHECK` constraint or `BEFORE` triggers);
- a KIRun definition on its own is compiled as a function.

The assistant fixes what the compile reports and compiles again, then proposes the statement with `propose_logic`. SprigDB compiles it once more before you see it: one that doesn't compile goes back to the assistant, and the proposal's `checks` says what compiled (kind, tier, how it is enforced). Stream and queue triggers and schedules can't be dry-run; their proposals say they weren't compiled.

### Test data

`generate_rows` proposes up to 10,000 test rows for a table. SprigDB generates them in the gateway, from the table's schema, as you:

| Column | Gets |
|---|---|
| `auto_increment` or generated | Left out |
| A foreign key | Keys that exist in the parent table (up to 10,000 read), a whole tuple for a composite key. A `NOT NULL` key whose parent table is empty is refused: generate the parent's rows first |
| `ENUM`, `SET`, a `CHECK … IN (…)` | Their values |
| A unique integer | A sequence after the column's largest value |
| A unique string | Values with a run tag and a sequence, so they don't collide with existing rows |
| Text, by its name | Emails (on `example.com`), phone numbers in the 555 test range, names, companies, addresses, cities, postcodes, ISO country and currency codes, URLs, documentation IP addresses, UUIDs, payment networks' test card numbers, obviously fake ids (`TEST-…`), short sentences or words; cut to the column's length |
| Numbers and dates, by type | Ranges that fit the type and the name (ages, years, quantities, prices), narrowed by `CHECK` bounds (`>`, `>=`, `<`, `<=`, `BETWEEN`, `AND`); dates in the last two years, birth dates between 1950 and 2006 |

The assistant may add a hint per column, as a line such as `status: one_of, values=paid|paid|new`, `total: decimal, min=5, max=500` or `email: email, domain=test.example` (also `nulls=20` for a share of NULLs in a nullable column). Composite unique keys are kept distinct among the generated rows. A `CHECK` that SprigDB doesn't understand is reported, and rows that fail it are refused when the statements run.

The rows go to you only: the proposal's `generated` holds the `INSERT` statements (500 rows each, at most 4 MiB in all), and the assistant gets a summary of how each column was filled, with counts but no value. Over MCP, `generated` is returned only when `ai.share_data` is on, because it may hold existing keys. Accepting the proposal runs the inserts as you.

## ASK in SQL

`ASK` puts a question to an assistant from any SQL client, on the MySQL and MariaDB ports or through `Sql.Query` over REST and gRPC. It returns the proposed query and an explanation, and **never runs the query**.

```sql
ASK 'question' [IN DATABASE db_name] [ROLE data | dba]
```

- The data specialist answers unless you say `ROLE dba`.
- The assistant works with the session's current database, or the one `IN DATABASE` names.
- `IN DATABASE` and `ROLE` may come in either order. Adjacent string literals are joined.
- `ASK` is recognized only when a string literal follows the word, so a table, column or variable named `ask` works as before.
- Run `ASK` as a plain statement: a server-side prepared statement can't carry it.

```sql
ASK 'top 10 customers by revenue this year';
ASK 'why is SELECT * FROM orders WHERE total > 100 slow?' IN DATABASE shop ROLE dba;
```

The answer has one row for each proposed statement:

| Column | Holds |
|---|---|
| `query` | One statement, without a trailing `;`. `NULL` when the assistant answered in words |
| `database` | The database the assistant meant it for, or `NULL` |
| `explanation` | Why, in a few sentences; the whole answer when there is no query |
| `notes` | `Reads only.`, or a reminder to review a statement that changes things; the estimate; `Statement 1 of 2 in one proposal.` when one proposal held several statements; a note when the answer was cut off at `ai.max_output_tokens` |
| `sources` | What the answer drew on, such as `table: shop.orders` or a docs page |
| `conversation_id` | The stored conversation |

`ASK` is one chat turn as you, with the same checks in the same order as the chat API: `ai.enabled` (39991), a tenant login (39792), the assistant's model (39990), and the budgets (39992). It uses the same tools, egress rules, budgets and audit. The conversation is kept like a chat's, so `GET /v1/ai/conversations/{conversation_id}` shows what was sent. A refusal is 39994, and a provider failure is 39993.

The query comes from the assistant's SQL proposals. When it made none, it comes from the SQL code blocks in its answer. A proposal holding several statements becomes several rows, so `query` always holds exactly one statement. Tile and glossary proposals aren't shown by `ASK`.

To run an answer, copy the query, or use [`sprig ask --run`](sprig.md#sprig-ask). In the TUI, press Ctrl-K and type `?` and your question: the `ASK` statement goes into the editor, and you run it like any other statement.

## Conversations and what was sent

`dba` and `data` conversations are kept in your tenant's catalog, and only you can read yours:

- `GET /v1/ai/conversations` lists them, newest first (`?limit=`, 50 by default).
- `GET /v1/ai/conversations/{id}` returns one with every message: your messages, answers, tool calls, tool results as the model got them, proposals, and a note for each model call with the SHA-256 of the exact payload sent and, while it is kept, the payload itself. Add `?payloads=false` to leave the payloads out.

**Retention.** Payloads are kept for `ai.audit_payload_days` (30); every hour the controller drops older ones and keeps their hashes. With 0, only hashes are kept. Each turn also writes one `ai.turn` event to the tenant's audit stream: the user, the role, the tools called and the payload hashes.

### Administration conversations

`admin` conversations belong to no tenant, so they are kept at the system level: in the `__sprig_ai` schema on the data center's system cluster, which is its lowest-numbered cluster that is active, draining or drained. The controller creates the schema. No tenant account can read it.

- **Who sees them.** Only the operator who had them, and only while their login is a platform operator (in `studio.operator_logins`, with operator mode on). Operators are told apart by login (`tenant.user`).
- **The same routes.** For an operator, `GET /v1/ai/conversations` lists their tenant conversations and their administration conversations together, newest first. Administration ids start with `a` (`a12`), as do their message ids, so `GET /v1/ai/conversations/a12` opens one with what was sent, `"conversation": "a12"` continues it, and `{"message_id": "a40", "vote": "up"}` rates an answer in it.
- **Retention.** The global `ai.audit_payload_days`; the controller purges them hourly like the tenants' payloads. Each turn also writes its `ai.turn` event to the platform audit stream.
- **The grant.** `sprig_ctl` needs ``GRANT ALL ON `__sprig_ai`.* TO 'sprig_ctl'@'%'`` on the system cluster; registration lists it as an advisory when it is missing. Until it is given, or while the system cluster can't be reached, `admin` turns still run but aren't stored, and the listing says why in `admin_unavailable`.
- **Moving.** If the system cluster is retired, the next one in service takes over with an empty history.

## Budgets

| Setting | Default | Meaning |
|---|---|---|
| `ai.budget.tenant_tokens_day` | 2,000,000 | Tokens all of a tenant's turns may use per UTC day |
| `ai.budget.user_tokens_day` | 200,000 | Tokens one user's turns may use per UTC day |
| `ai.max_output_tokens` | 16,000 | Output per model call, also capped by the budget left |
| `ai.max_tool_rounds` | 12 | Tool rounds per turn |
| `ai.timeout_seconds` | 120 | Wall time per turn |

Budgets count input read or written uncached, and output; cached input isn't counted. They are checked before every model call: a spent budget stops the turn with 39992 before anything is sent.

`admin` turns use the budgets' global values: `ai.budget.tenant_tokens_day` caps all administration turns in the data center, and `ai.budget.user_tokens_day` each operator.

## Glossary

The glossary holds your tenant's business terms ("an active customer ordered in the last 90 days"), optionally with an SQL expression and the tables they apply to. Every `dba` and `data` turn's prompt carries the accepted terms, so the assistants use your meanings.

| Route | Who | Does |
|---|---|---|
| `GET /v1/ai/glossary?status=accepted\|proposed\|all` | everyone in the tenant | The terms, and whether you may edit them |
| `POST /v1/ai/glossary` | tenant administrators | Adds or replaces an accepted term: `{term, definition, sql?, tables?}` |
| `POST /v1/ai/glossary/{term}:accept` | tenant administrators | Accepts a proposed term |
| `POST /v1/ai/glossary/{term}:reject` | tenant administrators | Drops a proposal |
| `POST /v1/ai/glossary/{term}:delete` | tenant administrators | Deletes a term |

When an assistant proposes a term, it is filed as a proposal; it reaches prompts only once an administrator accepts it. Glossary terms are text your administrators wrote, so they are sent like schema, whatever `ai.share_data` says. Changes are audited as `ai.glossary` events.

## Feedback

```json
POST /v1/ai/feedback
{"message_id": "42", "vote": "down", "note": "use net revenue"}
{"proposal_id": "0193…", "outcome": "accepted"}
```

Rate an answer up or down with an optional note (up to 500 characters), or report what became of a proposal: `accepted`, `dismissed` or `failed`. You can rate only your own conversations, administration ones included. The latest 20 outcomes and notes in the tenant are added to every `dba` and `data` prompt: the proposal's kind and a short label, and your note, never an answer's text. Feedback changes prompts, never a model.

## Errors

| Code | Name | When |
|---|---|---|
| 39990 | `ER_SPRIG_AI_NOT_CONFIGURED` | No model for the assistant (`ai.model.<role>` empty or naming no provider), or the provider's key doesn't open |
| 39991 | `ER_SPRIG_AI_DISABLED` | `ai.enabled` is off for the tenant |
| 39992 | `ER_SPRIG_AI_BUDGET_EXCEEDED` | A daily token budget is used up |
| 39993 | `ER_SPRIG_AI_PROVIDER_ERROR` | The provider failed after two retries, or the turn ran past `ai.timeout_seconds` (retryable) |
| 39994 | `ER_SPRIG_AI_REFUSED` | The model declined; its explanation is in the message |
| 39995 | `ER_SPRIG_AI_EGRESS_DENIED` | A `result` in the context while `ai.share_data` is off |
| 39996 | `ER_SPRIG_AI_TOOL_DENIED` | Three refused tool calls in one turn |
| 1227 | | The administration assistant asked for by someone who isn't a platform operator |

Errors found before a turn starts (39990, 39991, 39992, 39995, the role) are answered as HTTP errors; once the stream has started they arrive as an `error` line. See the [error reference](../reference/errors.md) for what to do about each.

## Metrics

`sprig_ai_requests_total`, `sprig_ai_tokens_total`, `sprig_ai_turn_seconds`, `sprig_ai_tool_calls_total`, `sprig_ai_egress_reduced_total`, `sprig_ai_proposals_total`, `sprig_ai_feedback_total`, `sprig_ai_budget_stops_total` and `sprig_ai_payloads_purged_total`. See the [metrics reference](../reference/metrics.md).
