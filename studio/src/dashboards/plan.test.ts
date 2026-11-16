import { describe, expect, it } from 'vitest';
import { describePlan, plan, type Plan } from './plan';
import { bounds, radius, tileTemplate } from './map';
import { normalize, type ChartSpec } from './spec';
import { datePart, fmt } from './data';

const COLS = [
  { name: 'week', type: 'DATE' },
  { name: 'region', type: 'VARCHAR' },
  { name: 'status', type: 'VARCHAR' },
  { name: 'revenue', type: 'DECIMAL' },
  { name: 'orders', type: 'BIGINT' },
];
// Two weeks, three regions; numbers arrive as text or numbers, as the API sends them.
const ROWS: unknown[][] = [
  ['2026-07-06', 'North', 'paid', '100', 2],
  ['2026-07-06', 'South', 'paid', 50, 1],
  ['2026-07-06', 'North', 'refunded', '10', 1],
  ['2026-07-13', 'North', 'paid', 120, 3],
  ['2026-07-13', 'West', 'paid', '30', 1],
  ['2026-07-14', 'South', 'shipped', 70, 2],
];

const p = (s: ChartSpec, rows = ROWS): Plan => plan(normalize(s, COLS), COLS, rows);
const cart = (x: Plan) => {
  if (x.kind !== 'cartesian') throw new Error(`not cartesian: ${x.kind}`);
  return x;
};

describe('spec to Recharts', () => {
  it('draws one bar per category with the measure summed', () => {
    const x = cart(p({ mark: 'bar', columns: [{ field: 'region' }], rows: [{ field: 'revenue', agg: 'sum' }] }));
    expect(x.chart).toBe('BarChart');
    expect(x.layout).toBe('horizontal');
    expect(x.xKey).toBe('region');
    expect(x.series).toEqual([{ component: 'Bar', dataKey: 'SUM(revenue)', name: 'SUM(revenue)', slot: 0, yAxisId: 'left' }]);
    expect(x.data).toEqual([
      { region: 'North', 'SUM(revenue)': 230 },
      { region: 'South', 'SUM(revenue)': 120 },
      { region: 'West', 'SUM(revenue)': 30 },
    ]);
  });

  it('makes a series per colour value, stacked and 100% stacked', () => {
    const s: ChartSpec = { mark: 'bar', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }], color: { field: 'region' }, stack: 'stack' };
    const x = cart(p(s));
    expect(x.series.map((v) => [v.name, v.slot, v.stackId])).toEqual([
      ['North', 0, 'left'],
      ['South', 1, 'left'],
      ['West', 2, 'left'],
    ]);
    expect(x.data.map((r) => r['WEEK(week)'])).toEqual(['2026-07-06', '2026-07-13']);
    expect(x.data[1]).toEqual({ 'WEEK(week)': '2026-07-13', 'SUM(revenue)|North': 120, 'SUM(revenue)|South': 70, 'SUM(revenue)|West': 30 });
    const pc = cart(p({ ...s, stack: 'percent' }));
    expect(pc.percent).toBe(true);
    const r = pc.data[1]!;
    expect(Math.round(Number(r['SUM(revenue)|North']) + Number(r['SUM(revenue)|South']) + Number(r['SUM(revenue)|West']))).toBe(100);
  });

  it('draws a combo with the second measure as a line on the right axis', () => {
    const x = cart(
      p({
        mark: 'bar',
        columns: [{ field: 'week', datePart: 'week' }],
        rows: [{ field: 'revenue', agg: 'sum' }, { field: 'orders', agg: 'count', axis: 'right', mark: 'line' }],
      }),
    );
    expect(x.chart).toBe('ComposedChart');
    expect(x.rightAxis).toBe(true);
    expect(x.series.map((v) => [v.component, v.yAxisId])).toEqual([
      ['Bar', 'left'],
      ['Line', 'right'],
    ]);
    expect(x.data[0]!['COUNT(orders)']).toBe(3);
  });

  it('lays bars on their side when the measure is on Columns, with sorting and top-N', () => {
    const x = cart(p({ mark: 'bar', rows: [{ field: 'region' }], columns: [{ field: 'revenue', agg: 'sum' }], sort: { by: 'SUM(revenue)', dir: 'asc' }, top: 2 }));
    expect(x.layout).toBe('vertical');
    expect(x.data.map((r) => r.region)).toEqual(['West', 'South']);
    expect(x.cut).toBe(1);
  });

  it('draws lines and areas', () => {
    expect(cart(p({ mark: 'line', columns: [{ field: 'week', datePart: 'day' }], rows: [{ field: 'orders', agg: 'sum' }] })).chart).toBe('LineChart');
    const a = cart(p({ mark: 'area', columns: [{ field: 'week', datePart: 'month' }], rows: [{ field: 'orders', agg: 'sum' }], color: 'status', stack: 'stack' }));
    expect(a.chart).toBe('AreaChart');
    expect(a.data).toHaveLength(1);
    expect(a.series.every((s) => s.component === 'Area' && s.stackId === 'left')).toBe(true);
  });

  it('folds pie slices past the eighth into Other', () => {
    const many = Array.from({ length: 11 }, (_, i) => ['2026-07-06', `R${i}`, 'paid', String(100 - i), 1]);
    const x = p({ mark: 'pie', color: { field: 'region' }, rows: [{ field: 'revenue', agg: 'sum' }] }, many);
    if (x.kind !== 'pie') throw new Error(x.kind);
    expect(x.data).toHaveLength(8);
    expect(x.data[7]).toEqual({ name: 'Other', value: 93 + 92 + 91 + 90, slot: 7 });
    expect(x.cut).toBe(4);
    const d = p({ mark: 'donut', color: 'status', rows: [{ field: 'orders', agg: 'sum' }] });
    expect(d.kind === 'pie' && d.donut).toBe(true);
  });

  it('shows a KPI as the latest period against the one before', () => {
    const x = p({ mark: 'kpi', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }] });
    if (x.kind !== 'kpi') throw new Error(x.kind);
    expect(x.value).toBe(220);
    expect(x.previous).toBe(160);
    expect(x.spark).toHaveLength(2);
    const t = p({ mark: 'kpi', rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(t.kind === 'kpi' && t.value).toBe(380);
  });

  it('bins a histogram and summarises box plots', () => {
    const h = p({ mark: 'histogram', columns: [{ field: 'revenue', agg: 'none' }] });
    if (h.kind !== 'histogram') throw new Error(h.kind);
    expect(h.data.reduce((a, b) => a + b.count, 0)).toBe(6);
    const b = p({ mark: 'box', columns: [{ field: 'status' }], rows: [{ field: 'revenue', agg: 'none' }] });
    if (b.kind !== 'box') throw new Error(b.kind);
    const paid = b.data.find((x) => x.name === 'paid')!;
    expect([paid.min, paid.median, paid.max, paid.n]).toEqual([30, 75, 120, 4]);
  });

  it('builds heatmaps, highlight tables and pivots with subtotals', () => {
    const m = p({ mark: 'heatmap', columns: [{ field: 'region' }], rows: [{ field: 'status' }], color: { field: 'orders', agg: 'sum' } });
    if (m.kind !== 'matrix') throw new Error(m.kind);
    expect(m.numbers).toBe(false);
    expect(m.m.cell('North', 'paid')).toBe(5);
    expect(m.m.max).toBe(5);
    const pv = p({ mark: 'pivot', rows: [{ field: 'region' }, { field: 'status' }], columns: [{ field: 'revenue', agg: 'sum' }] });
    if (pv.kind !== 'pivot') throw new Error(pv.kind);
    expect(pv.p.heads).toEqual(['SUM(revenue)']);
    const kinds = pv.p.rows.map((r) => `${r.kind}:${r.keys.join('/')}:${r.values[0]}`);
    expect(kinds).toEqual([
      'row:North/paid:220',
      'row:North/refunded:10',
      'subtotal:North/:230',
      'row:South/paid:50',
      'row:South/shipped:70',
      'subtotal:South/:120',
      'row:West/paid:30',
      'subtotal:West/:30',
      'total:Total/:380',
    ]);
  });

  describe('pivot subtotals and totals', () => {
    const pv = (agg: 'avg' | 'min' | 'max' | 'median' | 'countd' | 'count', field = 'revenue', columns: ChartSpec['columns'] = []) => {
      const x = p({ mark: 'pivot', rows: [{ field: 'region' }, { field: 'status' }], columns: [...(columns ?? []), { field, agg }] });
      if (x.kind !== 'pivot') throw new Error(x.kind);
      return x.p;
    };
    const at = (pt: ReturnType<typeof pv>, kind: string, key: string, col = 0) => pt.rows.find((r) => r.kind === kind && r.keys[0] === key)!.values[col];

    it('average the group’s rows, not the averages shown, for groups of unequal size', () => {
      const a = pv('avg');
      // North: paid 100 and 120 (avg 110), refunded 10: three rows, 230 / 3.
      expect(a.rows.find((r) => r.keys.join('/') === 'North/paid')!.values[0]).toBe(110);
      expect(at(a, 'subtotal', 'North')).toBeCloseTo(230 / 3);
      expect(at(a, 'subtotal', 'South')).toBe(60);
      expect(at(a, 'subtotal', 'West')).toBe(30);
      // Six rows, 380 / 6 (not 110 + 10 + 50 + 70 + 30).
      expect(at(a, 'total', 'Total')).toBeCloseTo(380 / 6);
    });

    it('take the minimum and maximum of the group', () => {
      expect(at(pv('min'), 'subtotal', 'North')).toBe(10);
      expect(at(pv('min'), 'total', 'Total')).toBe(10);
      expect(at(pv('max'), 'subtotal', 'North')).toBe(120);
      expect(at(pv('max'), 'subtotal', 'South')).toBe(70);
      expect(at(pv('max'), 'total', 'Total')).toBe(120);
    });

    it('recompute medians and distinct counts; counts still add', () => {
      // 10, 30, 50, 70, 100, 120.
      expect(at(pv('median'), 'total', 'Total')).toBe(60);
      expect(at(pv('median'), 'subtotal', 'North')).toBe(100);
      // paid, refunded, shipped: three, not 1 + 1 + 1 + 1 + 1.
      expect(at(pv('countd', 'status'), 'total', 'Total')).toBe(3);
      expect(at(pv('countd', 'status'), 'subtotal', 'North')).toBe(2);
      expect(at(pv('count'), 'subtotal', 'North')).toBe(3);
      expect(at(pv('count'), 'total', 'Total')).toBe(6);
    });

    it('recompute per column group too', () => {
      const a = pv('avg', 'revenue', [{ field: 'week' }]);
      expect(a.heads).toEqual(['2026-07-06 · AVG(revenue)', '2026-07-13 · AVG(revenue)', '2026-07-14 · AVG(revenue)']);
      // Week of 6 July: North 100 and 10, South 50.
      expect(at(a, 'subtotal', 'North', 0)).toBe(55);
      expect(at(a, 'total', 'Total', 0)).toBeCloseTo(160 / 3);
      // No South rows in the week of 13 July: no value, not zero.
      expect(at(a, 'subtotal', 'South', 1)).toBeNull();
      expect(at(a, 'total', 'Total', 1)).toBe(75);
    });
  });

  it('steps a waterfall and lays out Gantt bars', () => {
    const w = p({ mark: 'waterfall', columns: [{ field: 'region' }], rows: [{ field: 'revenue', agg: 'sum' }] });
    if (w.kind !== 'waterfall') throw new Error(w.kind);
    expect(w.data.map((s) => s.range)).toEqual([
      [0, 230],
      [230, 350],
      [350, 380],
      [0, 380],
    ]);
    const g = p({ mark: 'gantt', rows: [{ field: 'region' }], columns: [{ field: 'week', datePart: 'exact' }], size: { field: 'orders', agg: 'sum' } });
    if (g.kind !== 'gantt') throw new Error(g.kind);
    expect(g.data[0]!.range[1] - g.data[0]!.range[0]).toBe(2 * 86_400_000);
  });

  it('draws scatter, radar, treemap, funnel, sankey, gauge and bullet', () => {
    const sc = p({ mark: 'point', columns: [{ field: 'revenue', agg: 'sum' }], rows: [{ field: 'orders', agg: 'sum' }], color: 'region' });
    expect(sc.kind === 'scatter' && sc.groups.map((g) => g.name)).toEqual(['North', 'South', 'West']);
    expect(p({ mark: 'radar', columns: ['status'], rows: [{ field: 'orders', agg: 'sum' }] }).kind).toBe('radar');
    const tm = p({ mark: 'treemap', columns: ['region', 'status'], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(tm.kind === 'treemap' && tm.data[0]!.children!.length).toBe(2);
    expect(p({ mark: 'funnel', columns: ['status'], rows: [{ field: 'orders', agg: 'sum' }] }).kind).toBe('funnel');
    const sk = p({ mark: 'sankey', columns: ['region', 'status'], rows: [{ field: 'orders', agg: 'sum' }] });
    expect(sk.kind === 'sankey' && sk.data.links.length).toBe(5);
    const ga = p({ mark: 'gauge', rows: [{ field: 'orders', agg: 'sum' }] });
    expect(ga.kind === 'gauge' && [ga.value, ga.max]).toEqual([10, 10]);
    const bu = p({ mark: 'bullet', rows: [{ field: 'revenue', agg: 'sum' }, { field: 'orders', agg: 'sum' }] });
    expect(bu.kind === 'bullet' && bu.data[0]).toEqual({ name: 'SUM(revenue)', value: 380, target: 10 });
  });

  it('keeps filled maps as specs it can’t draw yet, filters rows, and says when nothing came back', () => {
    const filled = p({ mark: 'map-filled', columns: ['region'], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(filled.kind === 'unavailable' && filled.why).toMatch(/needs region shapes/);
    const noPlace = p({ mark: 'map-point', columns: ['country'] });
    expect(noPlace.kind === 'empty' && noPlace.why).toMatch(/latitude and a longitude/);
    const f = cart(p({ mark: 'bar', columns: ['region'], rows: [{ field: 'revenue', agg: 'sum' }], filters: [{ field: 'status', in: ['paid'] }] }));
    expect(f.data.find((r) => r.region === 'North')!['SUM(revenue)']).toBe(220);
    expect(p({ mark: 'bar', columns: ['region'], rows: [{ field: 'revenue', agg: 'sum' }] }, []).kind).toBe('empty');
    const t = p({ mark: 'text-table' });
    expect(t.kind === 'table' && t.heads).toEqual(['week', 'region', 'status', 'revenue', 'orders']);
  });

  it('draws point maps at latitude and longitude, sized and coloured by the shelves', () => {
    const cols = [
      { name: 'city', type: 'VARCHAR' },
      { name: 'lat', type: 'DECIMAL(9,6)' },
      { name: 'lng', type: 'DOUBLE' },
      { name: 'region', type: 'VARCHAR' },
      { name: 'revenue', type: 'DECIMAL' },
    ];
    const rows: unknown[][] = [
      ['Pune', '18.52', 73.85, 'West', 100],
      ['Pune', '18.52', 73.85, 'West', 50],
      ['Delhi', 28.61, 77.21, 'North', 300],
      ['Nowhere', null, 10, 'North', 5],
      ['Bad', 95, 10, 'North', 5],
      ['Chennai', 13.08, 80.27, 'South', 20],
    ];
    // Bare lat and lng on the shelves stay locations (never summed).
    const s = normalize({ mark: 'map-point', columns: ['lng'], rows: ['lat'], size: 'revenue', color: 'region', tooltip: ['city'] }, cols);
    expect(s.columns[0]).toEqual({ field: 'lng' });
    const m = plan(s, cols, rows);
    if (m.kind !== 'map') throw new Error(m.kind);
    expect(m.skipped).toBe(2);
    expect(m.points.map((x) => [x.lat, x.lon, x.size])).toEqual([
      [18.52, 73.85, 150],
      [28.61, 77.21, 300],
      [13.08, 80.27, 20],
    ]);
    expect(m.size).toEqual({ label: 'SUM(revenue)', min: 20, max: 300 });
    expect(m.colour).toEqual({ kind: 'category', label: 'region', values: ['West', 'North', 'South'] });
    expect(m.points.map((x) => x.colour)).toEqual([0, 1, 2]);
    expect(m.points[0]!.tip.map((t) => [t.label, t.value])).toEqual([
      ['region', 'West'],
      ['city', 'Pune'],
      ['SUM(revenue)', 150],
    ]);
    expect(radius(m, m.points[2]!)).toBe(4);
    expect(radius(m, m.points[1]!)).toBe(22);
    expect(bounds(m)).toEqual([
      [73.85, 13.08],
      [80.27, 28.61],
    ]);
    // A colour measure: a range, not categories.
    const cm = plan(normalize({ mark: 'map-point', columns: ['lng', 'lat'], color: { field: 'revenue', agg: 'avg' } }, cols), cols, rows);
    expect(cm.kind === 'map' && cm.colour).toEqual({ kind: 'measure', label: 'AVG(revenue)', min: 20, max: 300 });
    expect(describePlan(m, 'Sales')).toBe('Sales: point map of 3 places, sized by SUM(revenue), coloured by region');
  });

  it('takes only XYZ templates as tile servers', () => {
    expect(tileTemplate('https://tiles.example.com/{z}/{x}/{y}.png')).toBe('https://tiles.example.com/{z}/{x}/{y}.png');
    expect(tileTemplate('')).toBeNull();
    expect(tileTemplate('https://tiles.example.com/style.json')).toBeNull();
    expect(tileTemplate('javascript:{z}{x}{y}')).toBeNull();
  });

  it('cuts dates into parts and formats numbers', () => {
    expect(datePart('2026-07-14 10:30:00', 'year')).toBe('2026');
    expect(datePart('2026-07-14', 'quarter')).toBe('2026 Q3');
    expect(datePart('2026-07-14', 'month')).toBe('2026-07');
    expect(datePart('2026-07-14', 'week')).toBe('2026-07-13');
    expect(datePart('2026-07-14 10:30:00', 'hour')).toBe('2026-07-14 10:00');
    expect(fmt(123456)).toMatch(/123(\.5)?K/);
    expect(fmt(0.25, 'percent')).toBe('25%');
  });
});
