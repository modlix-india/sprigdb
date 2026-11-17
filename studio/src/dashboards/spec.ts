// The chart spec a tile stores (the same one the data assistant's
// propose_tile writes), field roles from a result's column types, and
// Show Me: which marks fit the shelves. The rules are chart-rules.json,
// shared with the gateway's tile check (a test keeps this copy equal).

import RULES from './chart-rules.json';

export type Agg = 'sum' | 'avg' | 'count' | 'countd' | 'min' | 'max' | 'median' | 'none';
export type DatePart = 'year' | 'quarter' | 'month' | 'week' | 'day' | 'hour' | 'exact';
export type Mark =
  | 'bar'
  | 'line'
  | 'area'
  | 'point'
  | 'pie'
  | 'donut'
  | 'waterfall'
  | 'histogram'
  | 'box'
  | 'heatmap'
  | 'highlight-table'
  | 'treemap'
  | 'funnel'
  | 'radar'
  | 'sankey'
  | 'kpi'
  | 'gauge'
  | 'bullet'
  | 'gantt'
  | 'text-table'
  | 'pivot'
  | 'map-point'
  | 'map-filled';

export interface Field {
  field: string;
  agg?: Agg;
  datePart?: DatePart;
  /** A second measure on its own axis (a combo). */
  axis?: 'left' | 'right';
  /** This measure's own mark (a line over bars). */
  mark?: Mark;
}

export type Shelf = 'columns' | 'rows' | 'color' | 'size' | 'label' | 'detail' | 'tooltip' | 'facet';

export interface ChartSpec {
  mark: Mark;
  columns?: (Field | string)[];
  rows?: (Field | string)[];
  color?: Field | string | null;
  size?: Field | string | null;
  label?: Field | string | null;
  detail?: (Field | string)[];
  tooltip?: (Field | string)[];
  filters?: { field: string; in: unknown[] }[];
  sort?: { by: string; dir?: 'asc' | 'desc' } | null;
  top?: number | null;
  facet?: Field | string | null;
  stack?: 'none' | 'stack' | 'percent' | null;
  format?: Record<string, string> | string;
}

/** A spec with every shelf entry as an object. */
export interface Spec {
  mark: Mark;
  columns: Field[];
  rows: Field[];
  color: Field | null;
  size: Field | null;
  label: Field | null;
  detail: Field[];
  tooltip: Field[];
  filters: { field: string; in: unknown[] }[];
  sort: { by: string; dir: 'asc' | 'desc' } | null;
  top: number | null;
  facet: Field | null;
  stack: 'none' | 'stack' | 'percent';
  format: Record<string, string>;
}

export const MARKS = Object.keys(RULES.marks) as Mark[];
export const AGGS = RULES.aggs as Agg[];
export const DATE_PARTS = RULES.dateParts as DatePart[];

export interface ColumnInfo {
  name: string;
  type: string;
}

export type Role = 'measure' | 'dimension' | 'date';

/** Field names read as a latitude, a longitude or a country (any case):
 * chart-rules.json's lists, which the gateway's tile check reads too. */
export const LAT_NAMES: readonly string[] = RULES.latNames;
export const LON_NAMES: readonly string[] = RULES.lonNames;
export const COUNTRY_NAMES: readonly string[] = RULES.countryNames;
export const isLat = (name: string) => LAT_NAMES.includes(name.toLowerCase());
export const isLon = (name: string) => LON_NAMES.includes(name.toLowerCase());
export const isCountry = (name: string) => COUNTRY_NAMES.includes(name.toLowerCase());

const NUMERIC = ['DECIMAL', 'TINYINT', 'SMALLINT', 'INT', 'FLOAT', 'DOUBLE', 'BIGINT', 'MEDIUMINT', 'INTEGER', 'NUMBER'];
const DATES = ['DATE', 'DATETIME', 'TIMESTAMP'];

/** What a column is on the shelves, from its type: numbers are measures,
 * dates are dates, the rest dimensions. Ids, flags, latitudes, longitudes
 * and countries (a numeric code) stay dimensions (a location is never
 * summed). */
export function roleOf(c: ColumnInfo): Role {
  const t = c.type.toUpperCase().replace(/\(.*$/, '').trim();
  if (DATES.includes(t)) return 'date';
  if (isLat(c.name) || isLon(c.name) || isCountry(c.name)) return 'dimension';
  const n = c.name.toLowerCase();
  if (NUMERIC.includes(t)) {
    const idLike = n === 'id' || n.endsWith('_id') || /[a-z]Id$/.test(c.name);
    const flag = t === 'TINYINT' && /^(is|has)_/.test(n);
    if (idLike || flag || t === 'YEAR') return 'dimension';
    return 'measure';
  }
  return 'dimension';
}

const one = (v: Field | string | null | undefined): Field | null =>
  v === null || v === undefined ? null : typeof v === 'string' ? (v.trim() ? { field: v } : null) : v.field ? { ...v } : null;
const many = (v: (Field | string)[] | undefined): Field[] => (v ?? []).map(one).filter((f): f is Field => f !== null);

/**
 * The spec with objects on every shelf. With the result's columns, a bare
 * measure (a grouped query's number) gets `sum`, so the counts are exact.
 */
export function normalize(s: ChartSpec, columns: ColumnInfo[] = []): Spec {
  const role = (f: string) => {
    const c = columns.find((x) => x.name === f) ?? columns.find((x) => x.name.toLowerCase() === f.toLowerCase());
    return c ? roleOf(c) : null;
  };
  const measureIfNumber = (f: Field): Field => (f.agg === undefined && !f.datePart && role(f.field) === 'measure' ? { ...f, agg: 'sum' } : f);
  const dateIfDate = (f: Field): Field => (f.agg === undefined && !f.datePart && role(f.field) === 'date' ? { ...f, datePart: 'exact' } : f);
  const fix = (f: Field) => dateIfDate(measureIfNumber(f));
  return {
    mark: MARKS.includes(s.mark) ? s.mark : 'bar',
    columns: many(s.columns).map(fix),
    rows: many(s.rows).map(fix),
    color: one(s.color),
    size: one(s.size),
    label: one(s.label),
    detail: many(s.detail),
    tooltip: many(s.tooltip),
    filters: (s.filters ?? []).filter((f) => f && typeof f.field === 'string'),
    sort: s.sort?.by ? { by: s.sort.by, dir: s.sort.dir === 'asc' ? 'asc' : 'desc' } : null,
    top: typeof s.top === 'number' && s.top > 0 ? Math.floor(s.top) : null,
    facet: one(s.facet),
    stack: s.stack === 'stack' || s.stack === 'percent' ? s.stack : 'none',
    format: typeof s.format === 'object' && s.format ? s.format : {},
  };
}

/** The stored form: empty shelves left out. */
export function toStored(s: Spec): ChartSpec {
  const out: ChartSpec = { mark: s.mark };
  if (s.columns.length) out.columns = s.columns;
  if (s.rows.length) out.rows = s.rows;
  if (s.color) out.color = s.color;
  if (s.size) out.size = s.size;
  if (s.label) out.label = s.label;
  if (s.detail.length) out.detail = s.detail;
  if (s.tooltip.length) out.tooltip = s.tooltip;
  if (s.filters.length) out.filters = s.filters;
  if (s.sort) out.sort = s.sort;
  if (s.top) out.top = s.top;
  if (s.facet) out.facet = s.facet;
  if (s.stack !== 'none') out.stack = s.stack;
  if (Object.keys(s.format).length) out.format = s.format;
  return out;
}

export const isMeasure = (f: Field) => f.agg !== undefined;

// ---- Show Me (the gateway's rules, chart-rules.json) ----

export interface Counts {
  dims: number;
  measures: number;
  dates: number;
  geo: boolean;
  /** A latitude and a longitude field are on the shelves (a point map
   * needs them, as the gateway's tile check does). */
  latLon: boolean;
  /** A country field is on the shelves (a filled map needs one). */
  country: boolean;
  /** Bare entries that might be measures (only for specs without column types). */
  unknown: number;
}

type Range = [number, number | null];
interface MarkRule {
  dims: Range;
  measures: Range;
  dates?: Range;
  dimsAreDates?: boolean;
  geo?: boolean;
}

const MARK_RULES = RULES.marks as unknown as Record<Mark, MarkRule>;
const GEO = new Set(RULES.geoNames);

/** Counts a spec's fields the way the gateway's tile check does. */
export function counts(s: ChartSpec): Counts {
  const c: Counts = { dims: 0, measures: 0, dates: 0, geo: false, latLon: false, country: false, unknown: 0 };
  let lat = false;
  let lon = false;
  const place = (f: Field) => {
    if (GEO.has(f.field.toLowerCase())) c.geo = true;
    if (isCountry(f.field)) c.country = true;
    lat ||= isLat(f.field);
    lon ||= isLon(f.field);
  };
  const shelf = (name: 'columns' | 'rows' | 'color' | 'size') => {
    const v = s[name];
    const list = Array.isArray(v) ? v : v === null || v === undefined ? [] : [v];
    for (const e of list) {
      const f = one(e);
      if (!f) continue;
      place(f);
      if (f.agg !== undefined || name === 'size') c.measures++;
      else {
        c.dims++;
        if (f.datePart) c.dates++;
        // A bare location is never a measure.
        else if ((name === 'columns' || name === 'rows') && !isLat(f.field) && !isLon(f.field) && !isCountry(f.field)) c.unknown++;
      }
    }
  };
  shelf('columns');
  shelf('rows');
  shelf('color');
  shelf('size');
  for (const name of ['detail', 'tooltip', 'label'] as const) {
    const v = s[name];
    const list = Array.isArray(v) ? v : v ? [v] : [];
    for (const e of list) {
      const f = one(e);
      if (f) place(f);
    }
  }
  c.latLon = lat && lon;
  return c;
}

const within = (n: number, r: Range) => n >= r[0] && (r[1] === null || n <= r[1]);

function fitsExactly(r: MarkRule, c: Counts): boolean {
  return (
    within(c.dims, r.dims) &&
    within(c.measures, r.measures) &&
    (!r.dates || within(c.dates, r.dates)) &&
    (!r.dimsAreDates || c.dates === c.dims) &&
    (!r.geo || c.geo)
  );
}

/** Whether `mark` fits, taking any bare entries as measures if that helps,
 * and a map has its place fields (as the gateway does). */
export function fits(mark: Mark, c: Counts): boolean {
  const r = MARK_RULES[mark];
  if (!r || !placed(mark, c)) return false;
  for (let k = 0; k <= c.unknown; k++) {
    if (fitsExactly(r, { ...c, dims: c.dims - k, measures: c.measures + k, unknown: 0 })) return true;
  }
  return false;
}

/** Whether a map mark has the place fields it draws by: a point map a
 * latitude and a longitude, a filled map a country. Other marks have. */
export function placed(mark: Mark, c: Counts): boolean {
  if (mark === 'map-point') return c.latLon;
  if (mark === 'map-filled') return c.country;
  return true;
}

/** The best mark for the shelves, and why (the first rule that holds),
 * skipping a point map without a latitude and a longitude and a filled
 * map without a country. */
export function best(c: Counts): { mark: Mark; why: string } {
  for (const b of RULES.best) {
    const w = b.when as Record<string, unknown>;
    const ok = Object.entries(w).every(([k, v]) => {
      if (k === 'geo') return v === c.geo;
      const r = v as Range;
      if (k === 'dims') return within(c.dims, r);
      if (k === 'measures') return within(c.measures, r);
      if (k === 'dates') return within(c.dates, r);
      return true;
    });
    if (ok && placed(b.mark as Mark, c)) return { mark: b.mark as Mark, why: b.why };
  }
  return { mark: 'text-table', why: '' };
}

/** A Show Me gallery entry: a mark and the shelf options it sets. */
export interface Preset {
  id: string;
  label: string;
  mark: Mark;
  stack?: 'none' | 'stack' | 'percent';
  /** Bars along the rows (horizontal). */
  horizontal?: boolean;
  /** The second measure as a line on the right axis. */
  combo?: boolean;
  /** Needs more than the mark's own rule. */
  needs?: (c: Counts) => boolean;
}

export const PRESETS: Preset[] = [
  { id: 'text-table', label: 'Text table', mark: 'text-table' },
  { id: 'highlight-table', label: 'Highlight table', mark: 'highlight-table' },
  { id: 'heatmap', label: 'Heatmap', mark: 'heatmap' },
  { id: 'pivot', label: 'Pivot table', mark: 'pivot' },
  { id: 'bar', label: 'Bar', mark: 'bar' },
  { id: 'hbar', label: 'Horizontal bar', mark: 'bar', horizontal: true },
  { id: 'stacked', label: 'Stacked bar', mark: 'bar', stack: 'stack', needs: (c) => c.dims >= 2 || c.measures >= 2 },
  { id: 'percent', label: '100% stacked bar', mark: 'bar', stack: 'percent', needs: (c) => c.dims >= 2 || c.measures >= 2 },
  { id: 'line', label: 'Line', mark: 'line' },
  { id: 'area', label: 'Area', mark: 'area' },
  { id: 'stacked-area', label: 'Stacked area', mark: 'area', stack: 'stack', needs: (c) => c.dims >= 2 || c.measures >= 2 },
  { id: 'combo', label: 'Combo, two axes', mark: 'bar', combo: true, needs: (c) => c.measures === 2 && c.dims >= 1 },
  { id: 'waterfall', label: 'Waterfall', mark: 'waterfall' },
  { id: 'point', label: 'Scatter', mark: 'point' },
  { id: 'bubble', label: 'Bubble', mark: 'point', needs: (c) => c.measures >= 3 },
  { id: 'pie', label: 'Pie', mark: 'pie' },
  { id: 'donut', label: 'Donut', mark: 'donut' },
  { id: 'histogram', label: 'Histogram', mark: 'histogram' },
  { id: 'box', label: 'Box plot', mark: 'box' },
  { id: 'treemap', label: 'Treemap', mark: 'treemap' },
  { id: 'funnel', label: 'Funnel', mark: 'funnel' },
  { id: 'radar', label: 'Radar', mark: 'radar' },
  { id: 'sankey', label: 'Sankey', mark: 'sankey' },
  { id: 'kpi', label: 'KPI', mark: 'kpi' },
  { id: 'gauge', label: 'Gauge', mark: 'gauge' },
  { id: 'bullet', label: 'Bullet', mark: 'bullet' },
  { id: 'gantt', label: 'Gantt', mark: 'gantt' },
  { id: 'map-point', label: 'Point map', mark: 'map-point', needs: (c) => c.latLon },
  { id: 'map-filled', label: 'Filled map', mark: 'map-filled', needs: (c) => c.country },
];

/** Whether a preset fits (its mark's rule, then its own needs). */
export function presetFits(p: Preset, c: Counts): boolean {
  return fits(p.mark, c) && (!p.needs || p.needs(c));
}

/** Applies a Show Me preset to a spec: the mark, the stack, the
 * orientation, and for a combo the second measure on the right axis as a
 * line. Fields on the shelves are moved, never dropped. */
export function applyPreset(s: Spec, p: Preset): Spec {
  let columns = s.columns.map((f) => ({ ...f }));
  let rows = s.rows.map((f) => ({ ...f }));
  const measuresOnColumns = columns.some(isMeasure) && !rows.some(isMeasure);
  // Horizontal bars hold the measures on columns; vertical ones on rows.
  if (p.mark === 'bar' && !!p.horizontal !== measuresOnColumns) [columns, rows] = [rows, columns];
  const strip = (f: Field): Field => {
    const { axis: _a, mark: _m, ...rest } = f;
    void _a;
    void _m;
    return rest;
  };
  rows = rows.map(strip);
  columns = columns.map(strip);
  if (p.combo) {
    const ms = rows.filter(isMeasure);
    if (ms.length >= 2) {
      const second = ms[1]!;
      rows = rows.map((f) => (f === second ? { ...f, axis: 'right', mark: 'line' } : f));
    }
  }
  return { ...s, mark: p.mark, columns, rows, stack: p.stack ?? 'none' };
}

/** The gallery entry that describes a spec (for highlighting it). */
export function presetOf(s: Spec): string {
  if (s.mark === 'bar') {
    if (s.rows.some((f) => f.axis === 'right')) return 'combo';
    if (s.stack === 'percent') return 'percent';
    if (s.stack === 'stack') return 'stacked';
    if (s.columns.some(isMeasure) && !s.rows.some(isMeasure)) return 'hbar';
    return 'bar';
  }
  if (s.mark === 'area' && s.stack !== 'none') return 'stacked-area';
  if (s.mark === 'point' && s.size) return 'bubble';
  return s.mark;
}

/** A first chart for a result (Add to dashboard): Show Me's pick for its
 * first date or dimension and its first measures. */
export function suggest(columns: ColumnInfo[]): Spec {
  const roles = columns.map((c) => ({ c, r: roleOf(c) }));
  const date = roles.find((x) => x.r === 'date');
  const dim = roles.find((x) => x.r === 'dimension');
  const measures = roles.filter((x) => x.r === 'measure').slice(0, 2);
  const cols: Field[] = [];
  if (date) cols.push({ field: date.c.name, datePart: 'exact' });
  else if (dim) cols.push({ field: dim.c.name });
  const rows: Field[] = measures.map((m) => ({ field: m.c.name, agg: 'sum' }));
  let s: Spec = normalize({ mark: 'text-table', columns: cols, rows }, columns);
  if (!rows.length) return normalize({ mark: 'text-table' }, columns);
  const b = best(counts(s));
  s = { ...s, mark: b.mark };
  if (b.mark === 'point') s = { ...s, columns: rows.slice(0, 1), rows: rows.slice(1) };
  return s;
}
