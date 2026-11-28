// Automatic indexing as the gateway's /v1/indexing routes return it: the
// controller's index records, the loop's events and operators' pending
// requests (GET /admin/v1/tenants/{t}/indexes).

/** One key column of an index. */
export interface KeyCol {
  column: string;
  desc?: boolean;
}

/** An automatic index's record. */
export interface IndexRecord {
  /** `sprig_auto_<8 hex>`. */
  name: string;
  spec: { db: string; table: string; cols: KeyCol[] };
  origin: string;
  /** recommended, building, observing, active, dropped, rejected, failed. */
  state: string;
  /** The query patterns (fingerprint ids) it serves. */
  serves: number[];
  est_before: number;
  est_after: number;
  benefit_ms_per_hour: number;
  /** Why it was decided: slow_p95, top_total_time, large_scan. */
  reason: string;
  created_us: number;
  changed_us: number;
  built_us?: number | null;
  build?: { states: string[]; rows: number; ms: number; batches: number } | null;
  last_used_us?: number | null;
  reads_total?: number;
  /** Its request in the approvals inbox (mode recommend). */
  approval?: string | null;
  /** Why it ended. */
  end_reason?: string | null;
  /** Pinned by an operator: the watch never drops it. */
  pinned?: boolean;
}

/** One decision of the loop. */
export interface IndexEvent {
  at_us: number;
  /** candidate, rejected_candidate, recommended, approved, built, build_failed, observed, dropped, deferred. */
  kind: string;
  index: string;
  def: string;
  /** `db.table`. */
  table: string;
  fingerprints: number[];
  reason: string;
  est?: [number, number] | null;
}

/** An operator's request the node hasn't applied yet. */
export interface IndexRequest {
  index: string;
  action: 'drop' | 'pin' | 'unpin' | 'restore';
  actor: string;
  reason: string;
  at_us: number;
}

export interface IndexListing {
  tenant: string;
  indexes: IndexRecord[];
  events: IndexEvent[];
  requests: IndexRequest[];
}

export interface IndexDetail {
  index: IndexRecord;
  events: IndexEvent[];
  requests: IndexRequest[];
}

/** States an index ends in. */
export const ENDED = ['dropped', 'rejected', 'failed'];

export type IndexAction = 'drop' | 'pin' | 'unpin' | 'restore';

/** What an operator may ask of `r` now, as the controller allows it. */
export function actionsFor(r: IndexRecord): IndexAction[] {
  const ended = ENDED.includes(r.state);
  const out: IndexAction[] = [];
  if (!ended && !r.pinned) out.push('pin');
  if (r.pinned) out.push('unpin');
  if (!ended) out.push('drop');
  if (r.state === 'dropped' || r.state === 'failed') out.push('restore');
  return out;
}

/** The button text of an action. */
export const ACTION_LABEL: Record<IndexAction, string> = { pin: 'Pin', unpin: 'Unpin', drop: 'Drop', restore: 'Restore' };

/** `col, col DESC`. */
export function columnsText(r: IndexRecord): string {
  return r.spec.cols.map((c) => (c.desc ? `${c.column} DESC` : c.column)).join(', ');
}

/** `db.table`. */
export const tableOf = (r: IndexRecord) => `${r.spec.db}.${r.spec.table}`;

/** µs since the epoch as local `YYYY-MM-DD HH:MM`. */
export function usText(us: number | null | undefined): string {
  if (!us) return '';
  const d = new Date(us / 1000);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

/** The pending request on index `name`, if any. */
export function pendingFor(requests: readonly IndexRequest[], name: string): IndexRequest | undefined {
  return requests.find((q) => q.index.toLowerCase() === name.toLowerCase());
}

/** Events newest first. */
export function newestFirst(events: readonly IndexEvent[]): IndexEvent[] {
  return [...events].sort((a, b) => b.at_us - a.at_us);
}
