---
title: AI health checks, write-ups and upgrade checks
audience: [operator]
sidebar:
  order: 13
---

# AI health checks, write-ups and upgrade checks

Besides [incident advice](ai-incident-advice.md), the administration agent:

- keeps a small **metric history** on the data center's leading controller;
- runs **health checks** over it, and files what they find as `ai_health_warning` items in the approvals inbox;
- drafts **write-ups** of failovers and failed data moves;
- answers **why SprigDB decided** something from its decision log;
- checks **upgrade readiness** on demand.

In every case SprigDB's own rules decide, and the model only words what they found. Everything it files is advice: nothing is ever changed by it. The model is sent ids, counts and numbers, never data, and every name SprigDB knows is replaced with a placeholder first, as for incident advice.

## Turning it on

Health checks and write-ups run while `ai.admin.enabled` is on, the same setting as incident advice (see [AI incident advice](ai-incident-advice.md#turning-it-on)):

```sh
sprigctl settings set ai.admin.enabled true --scope global --reason "administration agent"
sprigctl settings set ai.admin.check_interval 1h --scope global --reason "health checks"   # the default
```

`ai.model.admin` words the warnings and drafts the write-ups. Without it, health warnings are still filed with the rule's own words, and write-ups wait (see below).

## The metric history

Every 5 minutes the leading controller samples these series. They are written to etcd (`ai/metrics/` in the data center's tier) every 15 minutes and at each new hour, as hourly points (count, mean, minimum, maximum, last) kept 14 days, and daily points kept 30 days. A data center keeps at most 512 series; one that isn't updated for 14 days is dropped.

| Series | Labels | What it is |
|---|---|---|
| `sprig_shard_split_ratio` | `tenant` (`t<id>`), `group` | The largest shard of a table group over its split size (`sharding.split_size_gb`), as the sharding loop last measured it |
| `sprig_balancer_cluster_full_ratio` | `cluster`, `dimension` | How full a cluster is, 0 to 1, by its bottleneck, from the balancer's last run |
| `sprig_backend_replica_lag_seconds` | `cluster` | The largest replication lag among the cluster's replicas, from the controller's probes |
| `sprig_cluster_health_state` | `cluster` | 0 healthy, 1 degraded, 2 no writer, 3 unknown |
| `sprig_gateway_connections` | `gateway` | Open client sessions, as the gateway reports them |
| `sprig_backup_last_success_timestamp_seconds` | `cluster`, `kind` | When the newest successful backup finished (see [Backups](backups-and-moves.md)) |
| `sprig_mover_jobs_waiting` | - | Data-move jobs waiting for a worker |
| `sprig_mover_oldest_waiting_seconds` | - | How long the oldest of them has waited |

```sh
sprigctl ai metrics sprig_backend_replica_lag_seconds --label cluster=fra1-c1 --days 7
```

prints each matching series' daily mean, minimum and maximum. The history starts when `ai.admin.enabled` is turned on, and a new leading controller starts the current hour over.

## Health checks

Every `ai.admin.check_interval` (15m, 30m, 1h, 2h, 3h, 6h, 12h or 24h; `1h` by default) the rules run over the history:

| Rule | Finds |
|---|---|
| `shard_split` | A table group's largest shard above its split size for 24 hours, without splitting (medium); or projected to reach its split size within 7 days, after which it splits by itself (low) |
| `cluster_full` | A cluster 90% full or more (high); or projected to reach 90% within 7 days (medium) |
| `replica_lag` | Replication lag of 60 s or more in each of the last 3 hours (high); or a daily peak that rose over the last 3 whole days to 10 s or more (medium) |
| `cluster_health` | A cluster not healthy in any of the last 3 hours (high) |
| `connections` | A gateway that peaked at 80% of `gateway.max_connections` in the last day (medium); or is projected to reach 90% within 14 days (low) |
| `backup_age` | A cluster's newest backup of a kind older than 26 hours (medium) or 72 hours (high) |
| `mover_backlog` | A data move waiting 6 hours or more for a worker (medium) |

A projection fits a straight line through the hourly means of the last 72 hours, and needs at least 24 of them and a rising slope. A series with nothing in the last 3 hours is skipped.

Each finding is one `ai_health_warning` item: data-center level, advisory, no timers, expiring after 24 hours like any warning.

- **New finding.** The administration model is sent the finding (below), with `search_docs`, `metrics_trend` and `decision_log` to look further, and writes the severity, the finding and steps. The item holds the rule's own sentence first, then the model's words.
- **Still found while its item waits** (open or snoozed): the item's numbers are refreshed, and nothing is sent.
- **After the item is decided** (acknowledged, ignored or expired): it is warned about again after a quiet day, or at once if its severity rises.
- **Gone for a day:** forgotten; if it comes back, it is new.
- **Limits:** at most 3 warnings a check, and `ai.admin.max_advice_hour` an hour (counted apart from incident advice). The rest wait for the next check.
- **Without a model** (or with a spent budget, or a provider that fails), the warning is filed with the rule's words and the reason, such as `Not worded by a model: ai.model.admin is empty`.

What is sent for a finding:

```text
Health check finding at 2026-11-15T09:00Z (rule cluster_full, severity medium):
Cluster host#kqzmfa (storage is its bottleneck) is 85% full and filling; at this rate it reaches 90% in about 2 days.
Series: sprig_balancer_cluster_full_ratio{cluster=host#kqzmfa,dimension=storage} (metrics_trend serves its history).
Projected to reach the threshold in 1.6 days.
Topology: 1 DC; this DC has 2 gateways, 1 worker and 2 clusters (2 galera of 3 nodes).
Version: SprigDB 0.9.0.
```

The operators' copy in the inbox shows the real cluster id where the model wrote its placeholder.

```sh
sprigctl ai health list            # findings, newest first
sprigctl ai health show <key>      # one: the rule's text, what was sent, the warning
sprigctl ai health check           # run the rules now and print what they find; files nothing
sprigctl recommendations list --kind ai_health_warning
```

## Write-ups

A write-up is drafted 15 minutes after:

- a **cluster failover** (a promoted replica or a bootstrapped Galera node), id `cf-<8 hex digits of the cluster>-<writer epoch>`;
- a **DC failover** of this data center, or one that promoted tenants into it, id `df-<failover id>`;
- a **failed data move** (rolled back with an error), id `mv-<job id>`.

Events older than a day aren't written up on their own. The evidence covers 2 hours before the event to 1 hour after: what SprigDB recorded about the event (detection, decision, promotion, recovery time, expected loss, the former writer's fate), the errors from the error intake seen in the window, and the hourly points of the history's series for that cluster. The administration model gets it with `search_docs`, `metrics_trend` and `decision_log`, and returns a summary, timeline, cause, effects and follow-ups. The write-up is filed as an `ai_incident_advice` item (target `writeup/<id>`) and kept in etcd (`ai/writeups/<id>`). Without a model it is held, with the evidence kept, and can be drafted later.

```sh
sprigctl ai incident writeups                    # newest first
sprigctl ai incident writeup cf-1a2b3c4d-7       # the write-up, what was sent, the payload hashes
sprigctl ai incident writeup cf-1a2b3c4d-7 --draft   # draft it (again) now
sprigctl ai incident writeup 3f09a1c2b7de --draft    # a write-up of an incident from the error intake
```

`--draft` takes event ids from the last 90 days, or an incident fingerprint. It needs `ai.model.admin`, not `ai.admin.enabled`.

## Why SprigDB decided something

```sh
sprigctl ai decisions "fra1-c1 failover" --days 30
```

lists the data center's decision log, newest first: cluster failovers (and a cluster the failover loop holds blocked or suppressed), DC failovers, data moves (each naming the tenant's decision that started it), incidents, health findings and write-ups, each with an id to cite. The words are matched against each entry's id, kind, subject and summary; the entries matching most words come first. When nothing matches, the newest entries come back with a note saying so.

The assistants answer "why" questions from the same log with their `decision_log` tool:

- the **administration assistant** (operators) reads the data center's log above, and its `metrics_trend` tool reads the history;
- the **DBA assistant** reads only its own tenant's decision log (shard splits, merges and moves, index changes, placement), by decision id or by words, through `GET /admin/v1/tenants/{name}/decisions?subject=…&days=…`. `sprigctl explain decision <id> --tenant <name>` shows one entry.

## Upgrade readiness

```sh
sprigctl ai upgrade-check --to 1.5.0 --pack sprig-docs-1.5.0.pack [--explain]
```

compares the next version's settings and error codes with this one's, and matches the changes against how this deployment is used:

- **The next version's catalog** comes from its docs pack: the file given with `--pack` (read by `sprigctl`); else a pack file of that version next to the controller's binary or next to `docs.pack_path`; else the pack of that version loaded into the docs index on the system cluster.
- **Compared:** settings added, removed and renamed (a removed key and an added one with the same type and default, and the same last part or the same description), and their default, type, scopes and apply changes; error codes added, removed and renamed.
- **This deployment's use:** settings set away from their defaults at global, data-center and cluster scope (never their values), and the statement kinds, front ends and features of the workload fingerprints of up to 200 tenants homed in the data center. Tenant-scope settings aren't read yet.

Each change is ranked:

| Impact | When |
|---|---|
| `breaks` | A key set here is removed or renamed (after the upgrade its value is ignored, and the controller logs it as a bad setting record), or no longer accepts the scope it is set at |
| `changes` | A default changes for a key used at its default in a group in use here; a key set here changes type; an error code is renamed or no longer raised |
| `info` | The rest, and the new settings of groups in use |

With `--explain`, the administration model explains the report (the report text and counts are sent: keys, codes and numbers). The report itself never needs a model. The Admin API serves it as `POST /admin/v1/ai:upgradeCheck` with `{"to": "1.5.0", "catalog": {...}, "explain": false}`. The report covers the operators' side; a report for each tenant's own settings comes later.

## Admin API

| Route | Does |
|---|---|
| `GET /admin/v1/ai/metrics?name=&label=k=v&days=` | A series of the metric history |
| `GET /admin/v1/ai/health` | Findings, newest first |
| `GET /admin/v1/ai/health/{key}` | One finding |
| `POST /admin/v1/ai/health:check` | Runs the rules now and returns their findings; nothing is filed |
| `GET /admin/v1/ai/decisions?subject=&days=` | The data center's decision log |
| `GET /admin/v1/ai/writeups[/{id}]` | Write-ups |
| `POST /admin/v1/ai/writeups/{id}:draft` | Drafts the write-up of an event id or incident fingerprint |
| `POST /admin/v1/ai:upgradeCheck` | The upgrade readiness report |

All need an operator.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `ai.admin.enabled` | `false` | The metric history, health checks and write-ups run only while it is on |
| `ai.admin.check_interval` | `1h` | How often the health rules run |
| `ai.admin.max_advice_hour` | 10 | Health warnings, and write-ups, filed per hour in each data center (each counted apart from incident advice) |
| `ai.model.admin` | none | Words the warnings, drafts the write-ups, explains upgrade reports |

## Metrics

| Metric | Meaning |
|---|---|
| `sprig_ai_history_samples_total{result}` | Sampling passes of the metric history |
| `sprig_ai_health_findings_total{rule}` | Findings of the health rules, one per finding per check |
| `sprig_ai_health_warnings_total{worded}` | Health warnings filed: `model` or `rules` (without a model) |
| `sprig_ai_writeups_total{result}` | Write-ups: `drafted`, `held` (no model) or `failed` |
| `sprig_ai_upgrade_checks_total{result}` | Upgrade readiness checks: `ok` or `error` |
