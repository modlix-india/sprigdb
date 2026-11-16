// How a spec is drawn: which Recharts chart and series (or which of
// Studio's own drawings: tables, heatmaps, KPIs), worked out without
// React so the mapping can be tested.

import type { ColumnInfo, Spec } from './spec';
import {
  boxes,
  fmt,
  gantt,
  histogram,
  kpi,
  matrix,
  num,
  pivot,
  sankey,
  seriesData,
  waterfall,
  grouped,
  fieldsOf,
  labelOf,
  type BoxStats,
  type GanttBar,
  type Matrix,
  type Pivot,
  type Rec,
  type SeriesData,
} from './data';

/** One drawn series: a Recharts component with its props. */
export interface DrawnSeries {
  component: 'Bar' | 'Line' | 'Area';
  dataKey: string;
  name: string;
  slot: number;
  yAxisId: 'left' | 'right';
  stackId?: string;
}

export type Plan =
  | {
      kind: 'cartesian';
      chart: 'BarChart' | 'LineChart' | 'AreaChart' | 'ComposedChart';
      layout: 'horizontal' | 'vertical';
      xKey: string;
      data: Rec[];
      series: DrawnSeries[];
      rightAxis: boolean;
      percent: boolean;
      labels: boolean;
      cut: number;
    }
  | { kind: 'scatter'; xKey: string; yKey: string; zKey: string | null; groups: { name: string; slot: number; data: Rec[] }[] }
  | { kind: 'pie'; donut: boolean; data: { name: string; value: number; slot: number }[]; cut: number }
  | { kind: 'funnel'; data: { name: string; value: number; slot: number }[] }
  | { kind: 'radar'; angleKey: string; data: Rec[]; series: DrawnSeries[] }
  | { kind: 'treemap'; data: { name: string; size?: number; children?: { name: string; size: number }[] }[] }
  | { kind: 'sankey'; data: ReturnType<typeof sankey> }
  | { kind: 'histogram'; data: ReturnType<typeof histogram>; measure: string }
  | { kind: 'box'; data: BoxStats[]; measure: string }
  | { kind: 'waterfall'; data: ReturnType<typeof waterfall>; measure: string }
  | { kind: 'gantt'; data: GanttBar[] }
  | { kind: 'kpi'; value: number | null; previous: number | null; spark: { x: unknown; v: number | null }[]; label: string }
  | { kind: 'gauge'; value: number; max: number; label: string }
  | { kind: 'bullet'; data: { name: string; value: number; target: number | null }[]; measure: string }
  | { kind: 'matrix'; numbers: boolean; m: Matrix }
  | { kind: 'pivot'; p: Pivot }
  | { kind: 'table'; heads: string[]; rows: Rec[] }
  | { kind: 'unavailable'; why: string }
  | { kind: 'empty'; why: string };

function cartesian(s: Spec, sd: SeriesData): Plan {
  const combo = sd.series.some((x) => x.axis === 'right' || x.mark !== sd.series[0]?.mark);
  const stacked = s.stack !== 'none';
  const series: DrawnSeries[] = sd.series.map((x) => ({
    component: x.mark === 'line' ? 'Line' : x.mark === 'area' ? 'Area' : 'Bar',
    dataKey: x.key,
    name: x.label,
    slot: x.slot,
    yAxisId: x.axis,
    // Bars and areas stack per axis; a line never does.
    ...(stacked && x.mark !== 'line' ? { stackId: x.axis } : {}),
  }));
  const chart = combo ? 'ComposedChart' : s.mark === 'line' ? 'LineChart' : s.mark === 'area' ? 'AreaChart' : 'BarChart';
  return {
    kind: 'cartesian',
    chart,
    // Recharts' "vertical" layout puts categories down the side.
    layout: sd.horizontal ? 'vertical' : 'horizontal',
    xKey: sd.x ?? '',
    data: sd.data,
    series,
    rightAxis: series.some((x) => x.yAxisId === 'right'),
    percent: s.stack === 'percent',
    labels: !!s.label,
    cut: sd.cut,
  };
}

/** The drawing for a spec over a result. */
export function plan(s: Spec, columns: ColumnInfo[], rows: unknown[][]): Plan {
  const { dims, measures } = fieldsOf(s);
  if (!rows.length && s.mark !== 'text-table') return { kind: 'empty', why: 'The query returned no rows.' };
  switch (s.mark) {
    case 'bar':
    case 'line':
    case 'area': {
      if (!measures.length) return { kind: 'empty', why: 'Put a measure on Rows or Columns.' };
      return cartesian(s, seriesData(s, columns, rows));
    }
    case 'point': {
      const xs = s.columns.filter((f) => f.agg !== undefined);
      const ys = s.rows.filter((f) => f.agg !== undefined);
      const [mx, my] = xs.length && ys.length ? [xs[0]!, ys[0]!] : [measures[0], measures[1]];
      if (!mx || !my) return { kind: 'empty', why: 'A scatter plot needs two measures, one on Columns and one on Rows.' };
      const recs = grouped(s, columns, rows);
      const colour = s.color && s.color.agg === undefined ? labelOf(s.color) : null;
      const groups = new Map<string, Rec[]>();
      for (const r of recs) {
        const g = colour ? String(r[colour] ?? 'NULL') : labelOf(my);
        groups.set(g, [...(groups.get(g) ?? []), r]);
      }
      return {
        kind: 'scatter',
        xKey: labelOf(mx),
        yKey: labelOf(my),
        zKey: s.size ? labelOf({ ...s.size, agg: s.size.agg ?? 'sum' }) : null,
        groups: [...groups.entries()].slice(0, 8).map(([name, data], slot) => ({ name, slot, data })),
      };
    }
    case 'pie':
    case 'donut':
    case 'funnel': {
      const d = (s.color && s.color.agg === undefined ? s.color : null) ?? dims[0];
      const m = measures[0];
      if (!d || !m) return { kind: 'empty', why: 'Put a dimension on Colour and a measure on Rows.' };
      const recs = grouped({ ...s, columns: [], rows: [m], color: d, detail: [], facet: null }, columns, rows);
      let data = recs
        .map((r) => ({ name: String(r[labelOf(d)] ?? 'NULL'), value: num(r[labelOf(m)]) ?? 0 }))
        .filter((x) => x.value > 0)
        .sort((a, b) => b.value - a.value);
      let cut = 0;
      // Slices past the eighth fold into "Other": a ninth colour is never made up.
      if (s.mark !== 'funnel' && data.length > 8) {
        const rest = data.slice(7).reduce((a, b) => a + b.value, 0);
        cut = data.length - 7;
        data = [...data.slice(0, 7), { name: 'Other', value: rest }];
      }
      const withSlots = data.map((x, slot) => ({ ...x, slot }));
      return s.mark === 'funnel' ? { kind: 'funnel', data: withSlots.slice(0, 12) } : { kind: 'pie', donut: s.mark === 'donut', data: withSlots, cut };
    }
    case 'radar': {
      const sd = seriesData(s, columns, rows);
      if (!sd.x) return { kind: 'empty', why: 'A radar needs a dimension for its spokes.' };
      return { kind: 'radar', angleKey: sd.x, data: sd.data, series: (cartesian(s, sd) as Extract<Plan, { kind: 'cartesian' }>).series };
    }
    case 'treemap': {
      const m = measures[0];
      const [a, b] = dims;
      if (!m || !a) return { kind: 'empty', why: 'A treemap needs a dimension and a measure.' };
      const recs = grouped(s, columns, rows);
      const size = (r: Rec) => num(r[labelOf(m)]) ?? 0;
      if (!b) return { kind: 'treemap', data: recs.map((r) => ({ name: String(r[labelOf(a)] ?? 'NULL'), size: size(r) })).filter((x) => (x.size ?? 0) > 0) };
      const parents = new Map<string, { name: string; size: number }[]>();
      for (const r of recs) {
        const p = String(r[labelOf(a)] ?? 'NULL');
        if (size(r) > 0) parents.set(p, [...(parents.get(p) ?? []), { name: String(r[labelOf(b)] ?? 'NULL'), size: size(r) }]);
      }
      return { kind: 'treemap', data: [...parents.entries()].map(([name, children]) => ({ name, children })) };
    }
    case 'sankey': {
      const d = sankey(s, columns, rows);
      return d.links.length ? { kind: 'sankey', data: d } : { kind: 'empty', why: 'A sankey needs two dimensions and a positive measure.' };
    }
    case 'histogram':
      return { kind: 'histogram', data: histogram(s, columns, rows), measure: measures[0]?.field ?? '' };
    case 'box':
      return { kind: 'box', data: boxes(s, columns, rows), measure: measures[0]?.field ?? '' };
    case 'waterfall': {
      const sd = seriesData(s, columns, rows);
      return { kind: 'waterfall', data: waterfall(sd), measure: sd.series[0]?.label ?? '' };
    }
    case 'gantt': {
      const g = gantt(s, columns, rows);
      return g.length ? { kind: 'gantt', data: g } : { kind: 'empty', why: 'A Gantt chart needs a task, a start date, and an end date or a duration in days.' };
    }
    case 'kpi':
      return { kind: 'kpi', ...kpi(s, columns, rows) };
    case 'gauge': {
      const r = grouped({ ...s, columns: [], rows: measures, color: null, detail: [] }, columns, rows)[0] ?? {};
      const value = num(r[labelOf(measures[0]!)]) ?? 0;
      const target = measures[1] ? num(r[labelOf(measures[1])]) : null;
      const max = target && target > 0 ? target : niceMax(value);
      return { kind: 'gauge', value, max, label: labelOf(measures[0]!) };
    }
    case 'bullet': {
      const [m, t] = measures;
      if (!m) return { kind: 'empty', why: 'A bullet chart needs a measure, and a target as a second one.' };
      const d = dims[0];
      const recs = grouped(s, columns, rows);
      return {
        kind: 'bullet',
        measure: labelOf(m),
        data: recs.slice(0, 20).map((r) => ({
          name: d ? String(r[labelOf(d)] ?? 'NULL') : labelOf(m),
          value: num(r[labelOf(m)]) ?? 0,
          target: t ? num(r[labelOf(t)]) : null,
        })),
      };
    }
    case 'heatmap':
    case 'highlight-table': {
      const m = matrix(s, columns, rows);
      return m ? { kind: 'matrix', numbers: s.mark === 'highlight-table', m } : { kind: 'empty', why: 'Put dimensions on Columns and Rows, and a measure on Colour.' };
    }
    case 'pivot':
      return { kind: 'pivot', p: pivot(s, columns, rows) };
    case 'map-point':
    case 'map-filled':
      return { kind: 'unavailable', why: 'Maps are not drawn in Studio yet. Show Me suggests another chart; the tile keeps its spec.' };
    case 'text-table':
    default: {
      if (!dims.length && !measures.length) {
        return { kind: 'table', heads: columns.map((c) => c.name), rows: rows.slice(0, 500).map((r) => Object.fromEntries(columns.map((c, i) => [c.name, (r[i] ?? null) as Rec[string]]))) };
      }
      const heads = [...dims, ...measures].map(labelOf);
      return { kind: 'table', heads, rows: grouped(s, columns, rows).slice(0, 500) };
    }
  }
}

/** A round maximum above `v` (a gauge without a target). */
export function niceMax(v: number): number {
  if (v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  const m = v / p;
  return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
}

/** A sentence describing the drawing, for screen readers. */
export function describePlan(p: Plan, title: string): string {
  switch (p.kind) {
    case 'cartesian':
      return `${title}: ${p.chart.replace('Chart', '').toLowerCase()} chart of ${p.series.map((x) => x.name).join(', ')} by ${p.xKey || 'measure'}, ${p.data.length} categories`;
    case 'kpi':
      return `${title}: ${p.label} is ${fmt(p.value)}`;
    case 'pie':
      return `${title}: ${p.donut ? 'donut' : 'pie'} of ${p.data.length} slices`;
    case 'empty':
    case 'unavailable':
      return `${title}: ${p.why}`;
    default:
      return `${title}: ${p.kind} chart`;
  }
}
