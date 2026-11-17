// A tile's rows shaped for its chart: Studio applies the spec in the
// browser (the query returns row-level or grouped rows). Filters, date
// parts, aggregation, series from colour, sorting, top-N, 100% stacks,
// and the shapes the special marks need (histogram bins, box statistics,
// waterfall steps, a pivot with subtotals, heatmap cells, sankey links).

import type { ColumnInfo, Field, Spec } from './spec';
import { isMeasure } from './spec';

export type Cell = string | number | boolean | null;
export type Rec = Record<string, Cell>;

/** A field's name in shaped records and on axes: SUM(revenue), MONTH(day). */
export function labelOf(f: Field): string {
  if (f.agg && f.agg !== 'none') return `${f.agg === 'countd' ? 'CNTD' : f.agg.toUpperCase()}(${f.field})`;
  if (f.datePart && f.datePart !== 'exact') return `${f.datePart.toUpperCase()}(${f.field})`;
  return f.field;
}

// ---- values ----

export function num(v: unknown): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'boolean') return v ? 1 : 0;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** A date or datetime cell as UTC milliseconds. */
export function toTime(v: unknown): number | null {
  if (typeof v === 'number') return v > 1e11 ? v : v * 1000;
  if (typeof v !== 'string') return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?/.exec(v.trim());
  if (!m) return null;
  return Date.UTC(+m[1]!, +m[2]! - 1, +m[3]!, +(m[4] ?? 0), +(m[5] ?? 0), +(m[6] ?? 0));
}

const pad = (n: number) => String(n).padStart(2, '0');

/** A date bucket's key, which sorts in time order and reads well. */
export function datePart(v: unknown, part: Field['datePart']): Cell {
  if (!part || part === 'exact') return v === undefined ? null : (v as Cell);
  const t = toTime(v);
  if (t === null) return null;
  const d = new Date(t);
  const y = d.getUTCFullYear();
  const mo = d.getUTCMonth() + 1;
  switch (part) {
    case 'year':
      return String(y);
    case 'quarter':
      return `${y} Q${Math.floor((mo - 1) / 3) + 1}`;
    case 'month':
      return `${y}-${pad(mo)}`;
    case 'week': {
      // The Monday the ISO week starts on.
      const day = (d.getUTCDay() + 6) % 7;
      const m = new Date(Date.UTC(y, mo - 1, d.getUTCDate() - day));
      return `${m.getUTCFullYear()}-${pad(m.getUTCMonth() + 1)}-${pad(m.getUTCDate())}`;
    }
    case 'day':
      return `${y}-${pad(mo)}-${pad(d.getUTCDate())}`;
    case 'hour':
      return `${y}-${pad(mo)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:00`;
  }
}

export function median(xs: number[]): number | null {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m]! : (s[m - 1]! + s[m]!) / 2;
}

function quantile(sorted: number[], q: number): number {
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (pos - lo);
}

export function aggregate(values: unknown[], agg: Field['agg']): number | null {
  if (agg === 'count') return values.filter((v) => v !== null && v !== undefined).length;
  if (agg === 'countd') return new Set(values.filter((v) => v !== null && v !== undefined).map(String)).size;
  const ns = values.map(num).filter((n): n is number => n !== null);
  if (!ns.length) return null;
  switch (agg) {
    case 'avg':
      return ns.reduce((a, b) => a + b, 0) / ns.length;
    case 'min':
      return Math.min(...ns);
    case 'max':
      return Math.max(...ns);
    case 'median':
      return median(ns);
    case 'none':
      return ns[0]!;
    default:
      return ns.reduce((a, b) => a + b, 0);
  }
}

// ---- the table ----

/** Rows as records by column name. */
export function records(columns: ColumnInfo[], rows: unknown[][]): Rec[] {
  return rows.map((r) => {
    const o: Rec = {};
    columns.forEach((c, i) => {
      o[c.name] = (r[i] ?? null) as Cell;
    });
    return o;
  });
}

function getter(columns: ColumnInfo[]) {
  const names = columns.map((c) => c.name);
  return (rec: Rec, field: string): Cell => {
    if (field in rec) return rec[field]!;
    const n = names.find((x) => x.toLowerCase() === field.toLowerCase());
    return n ? rec[n]! : null;
  };
}

/** The spec's dimensions (in shelf order: columns, rows, colour, detail,
 * facet) and measures (columns, rows, colour, size, label, tooltip). */
export function fieldsOf(s: Spec): { dims: Field[]; measures: Field[] } {
  const dims: Field[] = [];
  const measures: Field[] = [];
  const add = (f: Field | null, asMeasure = false) => {
    if (!f) return;
    const list = isMeasure(f) || asMeasure ? measures : dims;
    const m = isMeasure(f) || asMeasure ? (f.agg ? f : { ...f, agg: 'sum' as const }) : f;
    if (!list.some((x) => labelOf(x) === labelOf(m))) list.push(m);
  };
  [...s.columns, ...s.rows].forEach((f) => add(f));
  add(s.color);
  s.detail.forEach((f) => add(f));
  add(s.facet);
  add(s.size, true);
  if (s.label && isMeasure(s.label)) add(s.label);
  s.tooltip.filter(isMeasure).forEach((f) => add(f));
  return { dims, measures };
}

/** The rows as records, with the spec's filters applied, and a field getter. */
export function filtered(s: Spec, columns: ColumnInfo[], rows: unknown[][]): { recs: Rec[]; get: (rec: Rec, field: string) => Cell } {
  const get = getter(columns);
  let recs = records(columns, rows);
  for (const f of s.filters) {
    const allowed = new Set(f.in.map(String));
    recs = recs.filter((r) => allowed.has(String(get(r, f.field))));
  }
  return { recs, get };
}

/**
 * Filtered, then grouped by every dimension with each measure aggregated
 * (row level when a measure's aggregation is `none`). Keys are labelOf().
 */
export function grouped(s: Spec, columns: ColumnInfo[], rows: unknown[][]): Rec[] {
  const { recs, get } = filtered(s, columns, rows);
  const { dims, measures } = fieldsOf(s);
  const rowLevel = measures.some((m) => m.agg === 'none');
  if (rowLevel) {
    return recs.map((r) => {
      const o: Rec = {};
      for (const d of dims) o[labelOf(d)] = datePart(get(r, d.field), d.datePart);
      for (const m of measures) o[labelOf(m)] = m.agg === 'none' ? num(get(r, m.field)) : aggregate([get(r, m.field)], m.agg);
      return o;
    });
  }
  const groups = new Map<string, { key: Rec; vals: unknown[][] }>();
  for (const r of recs) {
    const key: Rec = {};
    for (const d of dims) key[labelOf(d)] = datePart(get(r, d.field), d.datePart);
    const k = JSON.stringify(dims.map((d) => key[labelOf(d)]));
    let g = groups.get(k);
    if (!g) {
      g = { key, vals: measures.map(() => []) };
      groups.set(k, g);
    }
    measures.forEach((m, i) => g.vals[i]!.push(get(r, m.field)));
  }
  return [...groups.values()].map((g) => {
    const o: Rec = { ...g.key };
    measures.forEach((m, i) => {
      o[labelOf(m)] = aggregate(g.vals[i]!, m.agg);
    });
    return o;
  });
}

const cmp = (a: Cell, b: Cell): number => {
  if (a === b) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const na = num(a);
  const nb = num(b);
  if (na !== null && nb !== null) return na - nb;
  return String(a).localeCompare(String(b));
};

// ---- series charts (bar, line, area, combo, radar, funnel, pie) ----

export interface Series {
  key: string;
  label: string;
  measure: Field;
  /** Colour slot, by the series' first appearance (stable across filters). */
  slot: number;
  axis: 'left' | 'right';
  mark: 'bar' | 'line' | 'area';
}

export interface SeriesData {
  /** The category axis (a dimension label), or null for one bar per measure. */
  x: string | null;
  xIsDate: boolean;
  data: Rec[];
  series: Series[];
  /** Measures run along the category axis's other direction horizontally. */
  horizontal: boolean;
  /** Categories cut by top-N or the drawing cap. */
  cut: number;
}

/** The most categories one chart draws. */
export const MAX_CATEGORIES = 500;

export function seriesData(s: Spec, columns: ColumnInfo[], rows: unknown[][]): SeriesData {
  const horizontal = s.columns.some(isMeasure) && !s.rows.some(isMeasure);
  const axisShelf = horizontal ? s.rows : s.columns;
  const valueShelf = horizontal ? s.columns : s.rows;
  const axisDims = axisShelf.filter((f) => !isMeasure(f));
  const measures = [...valueShelf, ...axisShelf].filter(isMeasure);
  const xField = axisDims[0] ?? null;
  // Series: colour (a dimension), else a second dimension on the axis
  // shelf, else the other shelf's dimension.
  const otherDims = [...axisDims.slice(1), ...valueShelf.filter((f) => !isMeasure(f))];
  const seriesDim = s.color && !isMeasure(s.color) ? s.color : (otherDims[0] ?? null);
  const recs = grouped(s, columns, rows);
  const x = xField ? labelOf(xField) : null;
  const sd = seriesDim ? labelOf(seriesDim) : null;
  const seriesValues: Cell[] = [];
  if (sd) for (const r of recs) if (!seriesValues.some((v) => v === r[sd])) seriesValues.push(r[sd] ?? null);
  const series: Series[] = [];
  measures.forEach((m) => {
    const mark: Series['mark'] = m.mark === 'line' ? 'line' : m.mark === 'area' ? 'area' : s.mark === 'line' ? 'line' : s.mark === 'area' ? 'area' : 'bar';
    const axis = m.axis === 'right' ? 'right' : 'left';
    if (sd && seriesValues.length) {
      seriesValues.forEach((v) => {
        const lbl = measures.length > 1 ? `${labelOf(m)} · ${String(v)}` : String(v ?? 'NULL');
        series.push({ key: `${labelOf(m)}|${String(v)}`, label: lbl, measure: m, slot: series.length, axis, mark });
      });
    } else {
      series.push({ key: labelOf(m), label: labelOf(m), measure: m, slot: series.length, axis, mark });
    }
  });
  // One record per category.
  const byX = new Map<string, Rec>();
  const order: string[] = [];
  for (const r of recs) {
    const xv = x ? (r[x] ?? null) : '';
    const k = JSON.stringify(xv);
    let o = byX.get(k);
    if (!o) {
      o = x ? { [x]: xv } : { '': '' };
      byX.set(k, o);
      order.push(k);
    }
    for (const m of measures) {
      const key = sd ? `${labelOf(m)}|${String(r[sd])}` : labelOf(m);
      const prev = num(o[key]);
      const v = num(r[labelOf(m)]);
      o[key] = v === null ? (prev ?? null) : (prev ?? 0) + v;
    }
  }
  let data = order.map((k) => byX.get(k)!);
  const xIsDate = !!xField?.datePart;
  // Sort: the spec's, else dates in time order, else as the query returned.
  const firstKey = series[0]?.key;
  const total = (r: Rec) => series.reduce((a, se) => a + (num(r[se.key]) ?? 0), 0);
  if (s.sort) {
    const by = s.sort.by;
    const dir = s.sort.dir === 'asc' ? 1 : -1;
    const isX = x && (by === x || by === xField?.field);
    const m = measures.find((mm) => by === labelOf(mm) || by === mm.field);
    data = [...data].sort((a, b) => {
      if (isX) return dir * cmp(a[x] ?? null, b[x] ?? null);
      if (m) {
        const keys = series.filter((se) => se.measure === m).map((se) => se.key);
        const sum = (r: Rec) => keys.reduce((acc, k) => acc + (num(r[k]) ?? 0), 0);
        return dir * (sum(a) - sum(b));
      }
      return 0;
    });
  } else if (xIsDate && x) {
    data = [...data].sort((a, b) => cmp(a[x] ?? null, b[x] ?? null));
  }
  let cut = 0;
  if (s.top && data.length > s.top) {
    const ranked = s.sort ? data : [...data].sort((a, b) => total(b) - total(a));
    const keep = new Set(ranked.slice(0, s.top));
    cut = data.length - s.top;
    data = data.filter((r) => keep.has(r));
  }
  if (data.length > MAX_CATEGORIES) {
    cut += data.length - MAX_CATEGORIES;
    data = data.slice(0, MAX_CATEGORIES);
  }
  if (s.stack === 'percent') {
    data = data.map((r) => {
      const t = total(r);
      const o: Rec = { ...r };
      for (const se of series) o[se.key] = t ? ((num(r[se.key]) ?? 0) / t) * 100 : 0;
      return o;
    });
  }
  void firstKey;
  return { x, xIsDate, data, series, horizontal, cut };
}

// ---- special shapes ----

/** Histogram bins of the first measure's row values (Sturges' rule, 5 to 30 bins). */
export function histogram(s: Spec, columns: ColumnInfo[], rows: unknown[][]): { bin: string; from: number; to: number; count: number }[] {
  const m = fieldsOf(s).measures[0];
  if (!m) return [];
  const get = getter(columns);
  const xs = records(columns, rows)
    .map((r) => num(get(r, m.field)))
    .filter((n): n is number => n !== null);
  if (!xs.length) return [];
  const lo = Math.min(...xs);
  const hi = Math.max(...xs);
  const n = Math.max(5, Math.min(30, Math.ceil(Math.log2(xs.length) + 1)));
  const w = hi > lo ? (hi - lo) / n : 1;
  const bins = Array.from({ length: n }, (_, i) => ({ from: lo + i * w, to: lo + (i + 1) * w, count: 0, bin: '' }));
  for (const x of xs) bins[Math.min(n - 1, Math.floor((x - lo) / w))]!.count++;
  for (const b of bins) b.bin = `${fmt(b.from)}–${fmt(b.to)}`;
  return bins;
}

export interface BoxStats {
  name: string;
  min: number;
  q1: number;
  median: number;
  q3: number;
  max: number;
  n: number;
}

/** Box statistics of the first measure's row values, per category of the first dimension. */
export function boxes(s: Spec, columns: ColumnInfo[], rows: unknown[][]): BoxStats[] {
  const { dims, measures } = fieldsOf(s);
  const m = measures[0];
  if (!m) return [];
  const d = dims[0];
  const get = getter(columns);
  const groups = new Map<string, number[]>();
  for (const r of records(columns, rows)) {
    const v = num(get(r, m.field));
    if (v === null) continue;
    const k = d ? String(datePart(get(r, d.field), d.datePart) ?? 'NULL') : m.field;
    const g = groups.get(k) ?? [];
    g.push(v);
    groups.set(k, g);
  }
  return [...groups.entries()].map(([name, xs]) => {
    const sorted = xs.sort((a, b) => a - b);
    return {
      name,
      min: sorted[0]!,
      q1: quantile(sorted, 0.25),
      median: quantile(sorted, 0.5),
      q3: quantile(sorted, 0.75),
      max: sorted[sorted.length - 1]!,
      n: sorted.length,
    };
  });
}

/** Waterfall steps: each category's change as a floating bar from the running total, then the total. */
export function waterfall(sd: SeriesData): { name: string; range: [number, number]; delta: number; total?: boolean }[] {
  const key = sd.series[0]?.key;
  if (!key) return [];
  let run = 0;
  const out = sd.data.map((r) => {
    const v = num(r[key]) ?? 0;
    const from = run;
    run += v;
    return { name: String(sd.x ? r[sd.x] : key), range: [from, run] as [number, number], delta: v };
  });
  out.push({ name: 'Total', range: [0, run], delta: run, total: true } as (typeof out)[number] & { total: boolean });
  return out;
}

export interface Matrix {
  xs: string[];
  ys: string[];
  cell: (x: string, y: string) => number | null;
  min: number;
  max: number;
  measure: string;
  xLabel: string;
  yLabel: string;
}

/** Cells for a heatmap or highlight table: the first two dimensions and the first measure. */
export function matrix(s: Spec, columns: ColumnInfo[], rows: unknown[][]): Matrix | null {
  const { dims, measures } = fieldsOf(s);
  const m = measures[0];
  if (!m || !dims.length) return null;
  const dx = dims[0]!;
  const dy = dims[1];
  const recs = grouped(s, columns, rows);
  const xl = labelOf(dx);
  const yl = dy ? labelOf(dy) : '';
  const ml = labelOf(m);
  const xs: string[] = [];
  const ys: string[] = [];
  const cells = new Map<string, number>();
  for (const r of recs) {
    const x = String(r[xl] ?? 'NULL');
    const y = dy ? String(r[yl] ?? 'NULL') : ml;
    if (!xs.includes(x)) xs.push(x);
    if (!ys.includes(y)) ys.push(y);
    const v = num(r[ml]);
    if (v !== null) cells.set(`${x}\u0000${y}`, (cells.get(`${x}\u0000${y}`) ?? 0) + v);
  }
  const sortKeys = (a: string, b: string) => cmp(a, b);
  if (dx.datePart) xs.sort(sortKeys);
  if (dy?.datePart) ys.sort(sortKeys);
  const vals = [...cells.values()];
  return {
    xs: xs.slice(0, 100),
    ys: ys.slice(0, 100),
    cell: (x, y) => cells.get(`${x}\u0000${y}`) ?? null,
    min: vals.length ? Math.min(...vals) : 0,
    max: vals.length ? Math.max(...vals) : 0,
    measure: ml,
    xLabel: xl,
    yLabel: yl,
  };
}

export interface PivotRow {
  /** The row dimensions' values; a subtotal row has its last ones empty. */
  keys: string[];
  values: (number | null)[];
  kind: 'row' | 'subtotal' | 'total';
}

export interface Pivot {
  rowDims: string[];
  /** One header per column: the column dimensions' values, then the measure. */
  heads: string[];
  rows: PivotRow[];
}

/**
 * A pivot table: dimensions on rows down the side, dimensions on columns
 * across, every measure in each column group; subtotals for each value of
 * the first row dimension (when there are two or more), and a grand total.
 *
 * Every cell, subtotal and total is aggregated from the underlying rows of
 * its group, so an average, minimum, maximum, median or distinct count is
 * right at every level (a subtotal of averages is the average of the
 * group's rows, not the sum of the averages shown). A row-level measure
 * (`none`) adds up.
 */
export function pivot(s: Spec, columns: ColumnInfo[], rows: unknown[][]): Pivot {
  const rowDims = s.rows.filter((f) => !isMeasure(f));
  const colDims = s.columns.filter((f) => !isMeasure(f));
  const { measures } = fieldsOf(s);
  const { recs, get } = filtered(s, columns, rows);
  const keysOf = (r: Rec, ds: Field[]) => ds.map((d) => String(datePart(get(r, d.field), d.datePart) ?? 'NULL'));
  // A group's raw values: by column key, then by measure.
  type Bucket = { keys: string[]; cells: Map<string, unknown[][]> };
  const bucket = (keys: string[]): Bucket => ({ keys, cells: new Map() });
  const put = (b: Bucket, ck: string, r: Rec) => {
    let cell = b.cells.get(ck);
    if (!cell) {
      cell = measures.map(() => []);
      b.cells.set(ck, cell);
    }
    measures.forEach((m, mi) => cell[mi]!.push(get(r, m.field)));
  };
  const colKeys: string[] = [];
  const leaves = new Map<string, Bucket>();
  const firsts = new Map<string, Bucket>();
  const total = bucket([]);
  for (const r of recs) {
    const rk = keysOf(r, rowDims);
    const ck = keysOf(r, colDims).join(' · ');
    if (!colKeys.includes(ck)) colKeys.push(ck);
    const k = JSON.stringify(rk);
    let leaf = leaves.get(k);
    if (!leaf) {
      leaf = bucket(rk);
      leaves.set(k, leaf);
    }
    put(leaf, ck, r);
    if (rowDims.length >= 2) {
      let first = firsts.get(rk[0]!);
      if (!first) {
        first = bucket([rk[0]!]);
        firsts.set(rk[0]!, first);
      }
      put(first, ck, r);
    }
    put(total, ck, r);
  }
  if (colDims.some((d) => d.datePart)) colKeys.sort();
  const heads = colKeys.flatMap((c) => measures.map((m) => (c ? `${c} · ${labelOf(m)}` : labelOf(m))));
  const values = (b: Bucket): (number | null)[] =>
    colKeys.flatMap((c) =>
      measures.map((m, mi) => {
        const vals = b.cells.get(c)?.[mi];
        return vals ? aggregate(vals, m.agg === 'none' ? 'sum' : m.agg) : null;
      }),
    );
  const body = [...leaves.values()];
  if (rowDims.length) body.sort((a, b) => a.keys.join('\u0000').localeCompare(b.keys.join('\u0000'), undefined, { numeric: true }));
  const out: PivotRow[] = [];
  if (rowDims.length >= 2) {
    let group: Bucket[] = [];
    const flush = () => {
      if (!group.length) return;
      out.push(...group.map((r) => ({ keys: r.keys, values: values(r), kind: 'row' as const })));
      const first = group[0]!.keys[0]!;
      out.push({ keys: [first, ...rowDims.slice(1).map(() => '')], values: values(firsts.get(first)!), kind: 'subtotal' });
      group = [];
    };
    for (const r of body) {
      if (group.length && group[0]!.keys[0] !== r.keys[0]) flush();
      group.push(r);
    }
    flush();
  } else {
    out.push(...body.map((r) => ({ keys: r.keys, values: values(r), kind: 'row' as const })));
  }
  if (body.length > 1) out.push({ keys: rowDims.map((_, i) => (i === 0 ? 'Total' : '')), values: values(total), kind: 'total' });
  return { rowDims: rowDims.map(labelOf), heads, rows: out };
}

/** Sankey nodes and links: the first dimension flows to the second, sized by the first measure. */
export function sankey(s: Spec, columns: ColumnInfo[], rows: unknown[][]): { nodes: { name: string }[]; links: { source: number; target: number; value: number }[] } {
  const { dims, measures } = fieldsOf(s);
  const [a, b] = dims;
  const m = measures[0];
  if (!a || !b || !m) return { nodes: [], links: [] };
  const nodes: { name: string }[] = [];
  const idx = (name: string) => {
    let i = nodes.findIndex((n) => n.name === name);
    if (i < 0) {
      nodes.push({ name });
      i = nodes.length - 1;
    }
    return i;
  };
  const links: { source: number; target: number; value: number }[] = [];
  for (const r of grouped(s, columns, rows)) {
    const v = num(r[labelOf(m)]);
    if (!v || v <= 0) continue;
    // The two sides keep their own nodes even when a value repeats.
    const source = idx(`${String(r[labelOf(a)])}`);
    const target = idx(`${String(r[labelOf(b)])} `);
    links.push({ source, target, value: v });
  }
  return { nodes: nodes.map((n) => ({ name: n.name.trimEnd() })), links };
}

export interface GanttBar {
  task: string;
  range: [number, number];
}

/** Gantt bars: a task dimension, a start date, and an end date or a duration measure (days). */
export function gantt(s: Spec, columns: ColumnInfo[], rows: unknown[][]): GanttBar[] {
  const all = [...s.columns, ...s.rows, ...(s.color ? [s.color] : [])];
  const dates = all.filter((f) => !isMeasure(f) && f.datePart);
  const task = all.find((f) => !isMeasure(f) && !f.datePart);
  const dur = all.find(isMeasure) ?? (s.size ? s.size : null);
  const start = dates[0];
  if (!task || !start) return [];
  const end = dates[1];
  const get = getter(columns);
  const out: GanttBar[] = [];
  for (const r of records(columns, rows)) {
    const t0 = toTime(get(r, start.field));
    if (t0 === null) continue;
    let t1 = end ? toTime(get(r, end.field)) : null;
    if (t1 === null && dur) t1 = t0 + (num(get(r, dur.field)) ?? 1) * 86_400_000;
    if (t1 === null) t1 = t0 + 86_400_000;
    out.push({ task: String(get(r, task.field) ?? 'NULL'), range: [t0, Math.max(t0, t1)] });
  }
  return out.slice(0, MAX_CATEGORIES);
}

/** The KPI: the latest period's value with the change from the one before
 * (when a date is on a shelf), else the whole total; the series for a sparkline. */
export function kpi(s: Spec, columns: ColumnInfo[], rows: unknown[][]): { value: number | null; previous: number | null; spark: { x: Cell; v: number | null }[]; label: string } {
  const { dims, measures } = fieldsOf(s);
  const m = measures[0];
  if (!m) return { value: null, previous: null, spark: [], label: '' };
  const ml = labelOf(m);
  const d = dims.find((f) => f.datePart) ?? dims[0];
  if (!d) {
    const r = grouped(s, columns, rows)[0];
    return { value: r ? num(r[ml]) : null, previous: null, spark: [], label: ml };
  }
  const recs = grouped({ ...s, color: null, detail: [] }, columns, rows).sort((a, b) => cmp(a[labelOf(d)] ?? null, b[labelOf(d)] ?? null));
  const spark = recs.map((r) => ({ x: r[labelOf(d)] ?? null, v: num(r[ml]) }));
  return {
    value: spark.at(-1)?.v ?? null,
    previous: spark.length > 1 ? (spark.at(-2)?.v ?? null) : null,
    spark,
    label: ml,
  };
}

/** Small multiples: the rows split by the facet field's values (at most 12). */
export function facets(s: Spec, columns: ColumnInfo[], rows: unknown[][]): { value: string; rows: unknown[][] }[] {
  if (!s.facet) return [];
  const i = columns.findIndex((c) => c.name.toLowerCase() === s.facet!.field.toLowerCase());
  if (i < 0) return [];
  const out = new Map<string, unknown[][]>();
  for (const r of rows) {
    const v = String(datePart(r[i], s.facet.datePart) ?? 'NULL');
    const list = out.get(v) ?? [];
    list.push(r);
    out.set(v, list);
  }
  return [...out.entries()].slice(0, 12).map(([value, rs]) => ({ value, rows: rs }));
}

// ---- numbers ----

/** A number for an axis or label: compact above ten thousand. */
export function fmt(v: unknown, format?: string): string {
  const n = num(v);
  if (n === null) return v === null || v === undefined ? '' : typeof v === 'string' ? v : JSON.stringify(v);
  if (format === 'percent') return `${(n * 100).toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  if (format && /^[A-Z]{3}$/.test(format)) {
    return n.toLocaleString(undefined, { style: 'currency', currency: format, notation: Math.abs(n) >= 1e5 ? 'compact' : 'standard', maximumFractionDigits: 2 });
  }
  if (format === 'currency') return n.toLocaleString(undefined, { style: 'currency', currency: 'USD', notation: Math.abs(n) >= 1e5 ? 'compact' : 'standard' });
  if (Math.abs(n) >= 1e4) return n.toLocaleString(undefined, { notation: 'compact', maximumFractionDigits: 1 });
  return n.toLocaleString(undefined, { maximumFractionDigits: Math.abs(n) < 10 ? 2 : 1 });
}
