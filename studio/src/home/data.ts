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

export interface IndexAdvice {
  index_id: number;
  db: string;
  table: string;
  name: string;
  columns: string[];
  state: string;
  est_net_ms_h: number;
}

/** Recommended indexes (the automatic indexing page's Recommendations
 * view, same cache). */
export function useIndexAdvice() {
  return useQuery({
    queryKey: ['indexing', 'recommendations', '', '', false],
    queryFn: () => json<{ indexes: IndexAdvice[] }>('/v1/indexing/recommendations'),
    retry: false,
  });
}

export interface HomeUser {
  user: string;
  host: string;
  kind: string;
  account_locked: boolean;
  password_expired: boolean;
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
