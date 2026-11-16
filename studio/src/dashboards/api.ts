// The dashboards routes (/v1/dashboards...) and the queries Studio keeps
// of them.

import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import type { Schemas } from '../api/types';
import type { ChartSpec, ColumnInfo } from './spec';

export interface Pos {
  x: number;
  y: number;
  w: number;
  h: number;
}

export type FilterKind = 'text' | 'number' | 'date' | 'list';

export interface DashFilter {
  name: string;
  label: string;
  kind: FilterKind;
  value: unknown;
}

export interface Tile {
  id: string;
  title: string;
  database: string | null;
  sql: string;
  chart: ChartSpec;
  refresh_seconds: number;
  pos: Pos;
}

export interface DashboardHeader {
  id: string;
  name: string;
  owner: string;
  owner_id?: number;
  shared: boolean;
  mine: boolean;
  can_edit: boolean;
  tiles: number;
  updated_ms: number;
}

export interface Dashboard extends Omit<DashboardHeader, 'tiles'> {
  filters: DashFilter[];
  layout?: Record<string, unknown>;
  tiles: Tile[];
}

export interface TileRun {
  columns: ColumnInfo[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  ms: number;
}

export interface TileInput {
  title: string;
  database: string | null;
  sql: string;
  chart: ChartSpec;
  refresh_seconds?: number;
  pos?: Pos;
}

// The hand-written shapes must stay assignable to the generated ones.
type Assignable<A, B> = A extends B ? true : false;
export const DASHBOARD_TYPES_MATCH: [
  Assignable<Pick<DashboardHeader, 'id' | 'name' | 'shared' | 'mine' | 'can_edit'>, Pick<Schemas['DashboardHeader'], 'id' | 'name' | 'shared' | 'mine' | 'can_edit'>>,
  Assignable<Pick<TileRun, 'row_count' | 'truncated' | 'ms'>, Pick<Schemas['TileRun'], 'row_count' | 'truncated' | 'ms'>>,
] = [true, true];

const enc = encodeURIComponent;

export const listDashboards = () => json<{ mine: DashboardHeader[]; shared: DashboardHeader[] }>('/v1/dashboards');
export const getDashboard = (id: string) => json<Dashboard>(`/v1/dashboards/${enc(id)}`);
export const createDashboard = (name: string, shared = false) => json<Dashboard>('/v1/dashboards', { body: { name, shared } });
export const updateDashboard = (id: string, body: { name?: string; filters?: DashFilter[]; positions?: { tile: string; pos: Pos }[] }) =>
  json<Dashboard>(`/v1/dashboards/${enc(id)}`, { body });
export const shareDashboard = (id: string, shared: boolean) => json<Dashboard>(`/v1/dashboards/${enc(id)}:share`, { body: { shared } });
export const deleteDashboard = (id: string) => json<{ deleted: boolean }>(`/v1/dashboards/${enc(id)}:delete`, { body: {} });
export const addTile = (id: string, t: TileInput) => json<{ tile: string; dashboard: Dashboard }>(`/v1/dashboards/${enc(id)}/tiles`, { body: t });
export const updateTile = (id: string, tile: string, t: TileInput) =>
  json<{ tile: string; dashboard: Dashboard }>(`/v1/dashboards/${enc(id)}/tiles/${enc(tile)}`, { body: t });
export const deleteTile = (id: string, tile: string) => json<{ deleted: boolean }>(`/v1/dashboards/${enc(id)}/tiles/${enc(tile)}:delete`, { body: {} });
export const runTile = (id: string, tile: string, params: Record<string, unknown>, signal?: AbortSignal) =>
  json<TileRun>(`/v1/dashboards/${enc(id)}/tiles/${enc(tile)}:run`, { body: { params }, signal });
export const runQuery = (database: string | null, sql: string, filters: DashFilter[] = [], params: Record<string, unknown> = {}, signal?: AbortSignal) =>
  json<TileRun>('/v1/dashboards:run', { body: { database: database ?? '', sql, filters, params }, signal });

/** Accepts the assistant's tile proposal into a dashboard. */
export const acceptTile = (proposal: string, choice: { dashboard?: string; new_dashboard?: string; title?: string; chart?: ChartSpec; sql?: string }) =>
  json<{ outcome: string; dashboard?: string; tile?: string }>(`/v1/ai/proposals/${enc(proposal)}:accept`, { body: choice });
export const dismissProposal = (proposal: string) => json<unknown>(`/v1/ai/proposals/${enc(proposal)}:dismiss`, { body: {} });

export const KEYS = {
  list: ['dashboards'] as const,
  one: (id: string) => ['dashboards', id] as const,
};

export function useDashboards() {
  return useQuery({ queryKey: KEYS.list, queryFn: listDashboards });
}

export function useDashboard(id: string | undefined) {
  return useQuery({ queryKey: KEYS.one(id ?? ''), queryFn: () => getDashboard(id!), enabled: !!id });
}

/** Puts a dashboard the server answered into the cache, and refreshes the list. */
export function useStoreDashboard() {
  const qc = useQueryClient();
  return (d: Dashboard) => {
    qc.setQueryData(KEYS.one(d.id), d);
    void qc.invalidateQueries({ queryKey: KEYS.list, exact: true });
  };
}

/** The filter values a run binds: the viewer's own over the saved ones. */
export function paramsOf(filters: DashFilter[], local: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const f of filters) if (f.name in local) out[f.name] = local[f.name];
  return out;
}

/** `{{name}}` filters a query uses, outside quotes (as the gateway reads them). */
export function placeholders(sql: string): string[] {
  const out: string[] = [];
  const re = /'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*"|`[^`]*`|--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/|\{\{\s*([A-Za-z_][A-Za-z0-9_]{0,63})\s*\}\}/g;
  for (let m = re.exec(sql); m; m = re.exec(sql)) if (m[1] && !out.includes(m[1])) out.push(m[1]);
  return out;
}
