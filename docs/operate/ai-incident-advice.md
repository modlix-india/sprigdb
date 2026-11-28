---
title: AI incident advice
audience: [operator]
sidebar:
  order: 12
---

# AI incident advice

The administration agent watches the errors SprigDB reports. When an error is new, rises well above its usual rate, or is one of the critical codes, a cheap triage model decides whether it is noise; if it isn't, the administration model writes advice (likely cause, steps, the docs it used), and the advice lands in the approvals inbox for operators. It is advice only: nothing is ever changed by it.

It sends the provider the **problem, never data**: error codes, where they happened, counts, versions and a summary of the topology. It is **off by default**.

The same agent also runs health checks, drafts write-ups of failovers and checks upgrade readiness: see [AI health checks, write-ups and upgrade checks](ai-health-checks.md).

## Turning it on

1. Add a provider and set the models (see [AI assistants](../tools/assistants.md#setting-up)):

   ```sh
   sprigctl ai provider add anthropic --kind anthropic --key-file ~/.anthropic-key
   sprigctl settings set ai.model.admin anthropic/claude-opus-5-5 --scope global --reason "incident advice"
   sprigctl settings set ai.model.triage anthropic/claude-haiku-4-5 --scope global --reason "incident triage"   # optional
   ```

   Without `ai.model.triage`, the administration model does the triage too.

2. Turn the agent on:

   ```sh
   sprigctl settings set ai.admin.enabled true --scope global --reason "incident advice"
   ```

3. For email, set `approvals.operator_emails` and the SMTP settings of the approvals inbox; each new advice item is emailed like any new item at the data-center level.

The first minute after it is turned on, the controller takes what processes had already counted as a baseline; only errors after that are new.

## How it works

```text
every gateway, controller and worker: counts its errors by code and place
    -> written to etcd every minute (the DC tier, under the process's lease)
the data center's leading controller, every minute:
    adds up what each process counted since the last minute
    new, rising (3x its 7-day hourly baseline, at least 3 an hour) or critical:
        triage model: noise or needs_advice      (critical codes skip triage)
        administration model: searches the operator docs, writes the advice
        -> an ai_incident_advice item in the approvals inbox
    the same incident again: the item's counts are refreshed, nothing is sent
```

- **What is counted.** The gateway counts every request that fails, on every protocol; the controller every pass of its loops that fails; the worker every job and data-move step that fails. Each count is an error number (SprigDB's code, or the MariaDB error number) and a place such as `mysql.query`, `loop.balancer` or `job.logic_trigger`. No message text, no names, no values. A process keeps at most 256 different (code, place) pairs.
- **Incidents.** One per error number, component and place: its fingerprint is 12 hex digits. The controller keeps its hourly counts for seven days.
- **Critical codes** always get advice, without triage: 39032 `ER_SPRIG_OUTCOME_UNKNOWN`, 39441 `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN`, 39442 `ER_SPRIG_PARTIAL_COMMIT` and 39999 `ER_SPRIG_INTERNAL`.
- **Noise.** Triage screens out what applications cause at their usual rate: syntax errors, duplicate keys, failed logins, lock waits.
- **Quiet periods.** An incident that went to triage waits six hours before it can go again, and never while its advice item is still waiting.
- **Limits.** At most `ai.admin.max_triage_hour` incidents go to triage and `ai.admin.max_advice_hour` advice items are filed per hour in each data center, and three per minute. The rest wait; an incident still waiting after a day is dropped until it rises again.

## What is sent

Exactly this, as one message, for triage and then for advice:

```text
Incident fingerprint 3f09a1c2b7de: code 39100 (ER_SPRIG_CLUSTER_UNAVAILABLE), component controller, span loop.balancer.
Count: 12 in the last hour; 7-day baseline 0.2 per hour. 40 since first seen 3 hours ago, from 2 processes.
New fingerprint.
Topology: 1 DC; this DC has 2 gateways, 1 worker and 2 clusters (1 galera of 3 nodes, 1 primary_replica of 3 nodes).
Versions: SprigDB 0.9.0; MariaDB 11.8.2.
```

with the assistants' system prompt, and, for advice, the results of the docs search it asks for (operator pages of this release's docs).

**Never sent:** tenant, database, table, column or user names, statements, values, cluster, node, host or process names, and addresses. The text above holds none of them. As a second line of defence, every name SprigDB knows that could appear (data centers, clusters, nodes and their addresses, gateways, workers, processes and tenants) is registered with the egress gate before anything leaves, so a name that found its way in would be replaced by a placeholder such as `host#kqzmfa`. Counts of a million or more are written with separators.

## Where advice appears

Advice items have the kind `ai_incident_advice`, sit at the `dc` level and are advisory: there are no timers and nothing is applied.

```sh
sprigctl recommendations list --kind ai
sprigctl recommendations show dc.eu1.17
```

- The summary is the error and where it happened, with the likely cause; the action holds the whole advice.
- Each new item is emailed to `approvals.operator_emails`, and sent to `approvals.webhook_url` when one is set.
- **Approve** acknowledges it: it becomes `applied` with "acknowledged", and nothing changes. **Ignore** and **snooze** work as for any item.
- While the incident goes on, its item's counts are refreshed. After a week without the error it expires.

## Seeing what was sent

```sh
sprigctl ai incident list
sprigctl ai incident show 3f09a1c2b7de
sprigctl ai incident show 3f09a1c2b7de --payload
```

`show` prints the counts, the intake text exactly as sent, triage's verdict, the advice and the SHA-256 of every payload. `--payload` adds each exact payload, from the incident's conversation in the system store (the data center's system catalog records, kept for the global `ai.audit_payload_days`; see [AI assistants](../tools/assistants.md#administration-conversations)). The Admin API serves the same as `GET /admin/v1/ai/incidents` and `GET /admin/v1/ai/incidents/{fingerprint}?payloads=true`.

## Budgets

The agent's turns are budgeted like administration turns, as one more operator in the system store: the global `ai.budget.user_tokens_day` caps the agent, and `ai.budget.tenant_tokens_day` all administration turns of the data center together. When a budget is spent, incidents wait until 00:00 UTC.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `ai.admin.enabled` | `false` | Turns the agent on: processes write their error counts, and the controller triages and advises |
| `ai.admin.max_advice_hour` | 10 | Advice items filed per hour in each data center |
| `ai.admin.max_triage_hour` | 60 | Incidents sent to triage per hour in each data center |
| `ai.model.admin` | none | `provider/model` that writes the advice |
| `ai.model.triage` | none | `provider/model` that screens noise; the administration model when empty |
| `ai.effort.admin`, `ai.effort.triage` | `medium`, `low` | Effort, for models that support it |

All are global and for platform operators.

## Metrics

| Metric | Meaning |
|---|---|
| `sprig_ai_incidents_total{severity}` | Advice items filed |
| `sprig_ai_incident_triage_total{verdict}` | Incidents triaged: `noise`, `needs_advice`, or `critical` (triage skipped) |
| `sprig_ai_incidents_held_total{reason}` | Incidents that had to wait: `advice_cap`, `triage_cap`, `budget`, `no_model`, `error`, or `expired` |
| `sprig_ai_intake_flushes_total{result}` | A process's writes of its error counts |

Model calls also count in `sprig_ai_requests_total` and `sprig_ai_tokens_total` with `role` `triage` or `admin`.

## When it doesn't advise

| What you see | Why |
|---|---|
| `sprigctl ai incident list` is empty | `ai.admin.enabled` is off, or nothing failed since it was turned on |
| An incident `pending`, held `no_model` | `ai.model.admin` is empty or names no provider |
| Held `budget` | A daily token budget is spent; it resumes at 00:00 UTC |
| Held `advice_cap` or `triage_cap` | The hourly limits; it goes in a later hour |
| Status `failed` with an error | The provider failed or refused; the incident can go again after six hours |
| The conversation is `unavailable` | The engine node holding the system records couldn't be reached; advice still works, but the payloads weren't kept |
