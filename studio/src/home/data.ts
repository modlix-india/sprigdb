// What the home pages show, from calls Studio already makes elsewhere (the
// same routes and, where the shape is the same, the same cache keys, so a
// home page and the page it summarises share one fetch). No home page has
// a route of its own.

import { useQueries, useQuery } from '@tanstack/react-query';
import { json } from '../api/client';
import { objects } from '../api/sql';
import { fetchSnapshot, useDatabases, type DbSnapshot } from '../catalog/snapshot';
import { deadLetters, listModules, type ModuleRow } from '../logic/api';
import type { FingerprintRow } from '../indexing/Fingerprints';
import { showStatement } from '../settings/settings';
import type { IndexListing } from '../indexing/records';

/** At most this many databases are read for a home page. */
export const HOME_MAX_DATABASES = 12;
/** At most this many logic modules are asked for dead letters. */
export const HOME_MAX_MODULES = 12;

/** The catalog snapshots of the first databases (each shared with the
 * explorer tree's cache). */
export function useSnapshots(): { dbs: string[]; snaps: (DbSnapshot | undefined)[]; loading: boolean; error: unknown } {
  const dbs = useDatabases();
  const names = (dbs.data ?? []).slice(0, HOME_MAX_DATABASES);
  const qs = useQueries({
    queries: names.map((db) => ({ queryKey: ['catalog', 'db', db], queryFn: () => fetchSnapshot(db) })),
  });
  return {
    dbs: names,
    snaps: qs.map((q) => q.data),
    loading: dbs.isLoading || qs.some((q) => q.isLoading),
    error: dbs.error,
  };
}

export interface ModelCounts {
  db: string;
  tables: number;
  views: number;
  graphs: number;
  routines: number;
}

/** Objects of each kind per database, from the snapshots. */
export function modelCounts(dbs: string[], snaps: (Pick<DbSnapshot, 'objects'> | undefined)[]): ModelCounts[] {
  return dbs.map((db, i) => {
    const objs = snaps[i]?.objects ?? [];
    const n = (k: string[]) => objs.filter((o) => k.includes(o.kind)).length;
    return { db, tables: n(['table']), views: n(['view']), graphs: n(['graph']), routines: n(['procedure', 'function']) };
  });
}

export interface Found {
  kind: 'object' | 'saved';
  name: string;
  hint: string;
  /** Tenant-relative route, for objects. */
  rel?: string;
  /** The saved query's id, for saved queries. */
  id?: string;
}

/** The analyst's search: readable tables, views and graphs whose names
 * match, then saved queries whose name or text matches. */
export function searchHome(
  q: string,
  dbs: string[],
  snaps: (Pick<DbSnapshot, 'objects'> | undefined)[],
  saved: readonly { id: string; name: string; sql: string; db: string | null }[],
  limit = 20,
): Found[] {
  const t = q.trim().toLowerCase();
  if (!t) return [];
  const out: Found[] = [];
  dbs.forEach((db, i) => {
    for (const o of snaps[i]?.objects ?? []) {
      if (!['table', 'view', 'graph'].includes(o.kind) || !o.name.toLowerCase().includes(t)) continue;
      out.push({
        kind: 'object',
        name: o.name,
        hint: `${o.kind} · ${db}`,
        rel: `db/${encodeURIComponent(db)}/obj/${o.kind}/${encodeURIComponent(o.name)}`,
      });
    }
  });
  for (const s of saved) {
    if (s.name.toLowerCase().includes(t) || s.sql.toLowerCase().includes(t)) {
      out.push({ kind: 'saved', name: s.name, hint: s.db ?? '', id: s.id });
    }
  }
  return out.slice(0, limit);
}

/** Workload fingerprints of the last 24 hours (the automatic indexing
 * page's Fingerprints view, same cache). */
export function useFingerprints() {
  return useQuery({
    queryKey: ['workload', 'fingerprints', 24, 'all'],
    queryFn: () => json<{ fingerprints: FingerprintRow[] }>('/v1/workload/fingerprints?hours=24&limit=100&source=all'),
    retry: false,
  });
}

/** The heaviest fingerprints by total time, and the slowest by p95. */
export function loadSummary(rows: readonly FingerprintRow[], n = 5): { heaviest: FingerprintRow[]; slowest: FingerprintRow[] } {
  return {
    heaviest: [...rows].sort((a, b) => b.total_ms - a.total_ms).slice(0, n),
    slowest: [...rows].sort((a, b) => b.p95_ms - a.p95_ms).slice(0, n),
  };
}

/** Recommended automatic indexes (the automatic indexing page's
 * Recommendations view, same cache): the controller's index records. */
export function useIndexAdvice() {
  return useQuery({
    queryKey: ['indexing', 'recommendations', '', '', false],
    queryFn: () => json<IndexListing>('/v1/indexing/recommendations'),
    retry: false,
  });
}

export interface HomeUser {
  user: string;
  host: string;
  kind: string;
  account_locked: boolean;
  password_expired: boolean;
  /** Newer gateways: the last successful and failed logins (ms, as text),
   * failures in a row, and a temporary lock's end. */
  last_login_ms?: string | null;
  last_failed_login_ms?: string | null;
  failed_logins?: number;
  locked_until_ms?: string | null;
}

/** Accounts with failed logins in a row, most first, then by latest failure. */
export function failedLogins(users: readonly HomeUser[]): HomeUser[] {
  return users
    .filter((u) => (u.failed_logins ?? 0) > 0)
    .sort((a, b) => (b.failed_logins ?? 0) - (a.failed_logins ?? 0) || Number(b.last_failed_login_ms ?? 0) - Number(a.last_failed_login_ms ?? 0));
}

/** Storage per database as `information_schema.TABLES` reports it to the
 * caller (data and index bytes), largest first. */
export function useStorage() {
  return useQuery({
    queryKey: ['home', 'storage'],
    queryFn: async () => {
      const r = await objects(
        null,
        'SELECT TABLE_SCHEMA AS db, COUNT(*) AS tables, SUM(DATA_LENGTH + INDEX_LENGTH) AS bytes FROM information_schema.TABLES GROUP BY TABLE_SCHEMA',
      );
      return storageRows(r);
    },
    retry: false,
  });
}

export interface StorageRow {
  db: string;
  tables: number;
  bytes: number;
}

export function storageRows(rows: readonly Record<string, string | null>[]): StorageRow[] {
  return rows
    .map((x) => ({ db: x.db ?? '', tables: Number(x.tables ?? 0), bytes: Number(x.bytes ?? 0) }))
    .filter((x) => x.db && !['information_schema', 'performance_schema', 'mysql', 'sys'].includes(x.db.toLowerCase()))
    .sort((a, b) => b.bytes - a.bytes);
}

/** Bytes in words: 1.5 MB. */
export function bytesText(n: number): string {
  const units = ['bytes', 'KB', 'MB', 'GB', 'TB'];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return i === 0 ? `${v} bytes` : `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

export interface RunningStatement {
  id: string;
  user: string;
  db: string | null;
  seconds: number;
  text: string;
}

/** Statements running at least `min` seconds on this gateway, from SHOW FULL
 * PROCESSLIST as the caller sees it, longest first. */
export function longRunning(rows: readonly Record<string, string | null>[], min = 5): RunningStatement[] {
  return rows
    .filter((x) => x.Command !== 'Sleep' && Number(x.Time ?? 0) >= min && x.Info && !/^\s*SHOW\s+FULL\s+PROCESSLIST/i.test(x.Info))
    .map((x) => ({ id: x.Id ?? '', user: x.User ?? '', db: x.db ?? null, seconds: Number(x.Time ?? 0), text: x.Info ?? '' }))
    .sort((a, b) => b.seconds - a.seconds);
}

export function useLongRunning() {
  return useQuery({
    queryKey: ['home', 'processlist'],
    queryFn: async () => longRunning(await objects(null, 'SHOW FULL PROCESSLIST')),
    refetchInterval: 15_000,
    retry: false,
  });
}

export interface SchemaChange {
  database: string;
  object: string;
  kind: string;
  version: number;
  statement: string;
  at_ms: number;
  by: string | null;
}

/** Recent schema changes of the objects the caller can see (newer gateways). */
export function useSchemaChanges(limit = 10) {
  return useQuery({
    queryKey: ['home', 'schema-changes', limit],
    queryFn: () => json<{ changes: SchemaChange[] }>(`/v1/activity/schema?limit=${limit}`),
    retry: false,
  });
}

export interface RecentGrant {
  grantee_kind: string;
  grantee: string | null;
  level: string;
  database: string | null;
  object: string | null;
  column: string | null;
  privileges: string[];
  restriction: boolean;
  at_ms: number;
  by: string | null;
}

/** Recent grants, for user administrators (newer gateways). */
export function useRecentGrants(limit = 10) {
  return useQuery({
    queryKey: ['home', 'grants', limit],
    queryFn: () => json<{ grants: RecentGrant[] }>(`/v1/activity/grants?limit=${limit}`),
    retry: false,
  });
}

/** Where a grant applies, as GRANT writes it: `*.*`, `shop.*`, `shop.orders`. */
export function grantTarget(g: Pick<RecentGrant, 'level' | 'database' | 'object' | 'column'>): string {
  if (g.level === 'tenant') return '*.*';
  const db = g.database ?? '?';
  if (g.level === 'database') return `${db}.*`;
  const obj = `${db}.${g.object ?? '?'}`;
  return g.column ? `${obj} (${g.column})` : obj;
}

/** The tenant's accounts (the users page, same cache). */
export function useUsers() {
  return useQuery({
    queryKey: ['users'],
    queryFn: () => json<{ users: HomeUser[]; all: boolean }>('/v1/users'),
    retry: false,
  });
}

export interface FailingModule {
  db: string;
  module: ModuleRow;
  dead: number;
}

/** Triggers and schedules with dead letters waiting, in the first
 * databases; at most HOME_MAX_MODULES modules are asked. */
export function useFailingLogic(dbs: string[]): { rows: FailingModule[]; loading: boolean; checked: number } {
  const lists = useQueries({
    queries: dbs.map((db) => ({ queryKey: ['logic', db], queryFn: () => listModules(db), retry: false })),
  });
  const bound = lists
    .flatMap((q, i) => (q.data?.modules ?? []).map((m) => ({ db: dbs[i]!, m })))
    .filter((x) => x.m.kind === 'trigger' || x.m.kind === 'schedule')
    .slice(0, HOME_MAX_MODULES);
  const dead = useQueries({
    queries: bound.map(({ db, m }) => ({
      queryKey: ['logic-dead', db, m.kind, m.name, 'dead'],
      queryFn: () => deadLetters(db, m.kind, m.name, 'dead'),
      retry: false,
    })),
  });
  const rows = bound
    .map(({ db, m }, i) => ({ db, module: m, dead: dead[i]?.data?.dead_letters.length ?? 0 }))
    .filter((r) => r.dead > 0);
  return { rows, loading: lists.some((q) => q.isLoading) || dead.some((q) => q.isLoading), checked: bound.length };
}

export interface TenantSetting {
  key: string;
  value: string;
}

/** Settings whose value is set on this tenant itself (not inherited or
 * the default), from SHOW SPRIG SETTINGS. */
export function useTenantSettings(tenant: string, home: boolean) {
  return useQuery({
    queryKey: ['home', 'settings', tenant],
    queryFn: async (): Promise<TenantSetting[]> => {
      const r = await objects(null, showStatement({ kind: 'tenant', tenant: home ? null : tenant }));
      return setOnTenant(r);
    },
    retry: false,
  });
}

/** The rows of SHOW SPRIG SETTINGS whose source is the tenant itself. */
export function setOnTenant(rows: readonly Record<string, string | null>[]): TenantSetting[] {
  return rows
    .filter((x) => (x.SOURCE_SCOPE ?? '').toLowerCase() === 'tenant')
    .map((x) => ({ key: x.SETTING_NAME ?? '', value: x.VALUE ?? '' }));
}
