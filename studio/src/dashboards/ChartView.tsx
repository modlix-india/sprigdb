// Draws a tile's chart from its plan with Recharts (bar, line, area,
// combo, scatter, pie, radar, treemap, funnel, sankey, gauge; box plots,
// waterfalls, bullets and Gantt bars from Bar shapes), Studio's own HTML
// for tables, pivots, heatmaps and KPIs, and MapLibre for point and
// filled maps (loaded on first use, the country shapes only for a filled
// map).

import { lazy, Suspense, useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Funnel,
  FunnelChart,
  LabelList,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Sankey,
  Scatter,
  ScatterChart,
  Tooltip,
  Treemap,
  XAxis,
  YAxis,
  ZAxis,
} from 'recharts';
import { isDark } from '../app/theme';
import { facets, fmt, num, type Rec } from './data';
import { describePlan, plan, type Plan } from './plan';
import type { ColumnInfo, Spec } from './spec';

const MapView = lazy(() => import('./MapView'));

// The categorical palette (validated for colour-blind separation in both
// themes); slots are given by a series' first appearance and never cycled.
const LIGHT = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#6250d6', '#e34948'];
const DARK = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#0ca30c', '#9085e9', '#e66767'];
// Sequential (one hue, light to dark) for heatmaps.
const SEQ_LIGHT = ['#e8f1fb', '#b9d3f2', '#86b3e8', '#5591dc', '#2a78d6', '#1b5aa6', '#123e74'];
const SEQ_DARK = ['#16263a', '#1b3a5e', '#22508a', '#2c67b3', '#3987e5', '#76abee', '#b6d2f6'];

export interface Palette {
  series: string[];
  seq: string[];
  grid: string;
  text: string;
  muted: string;
  surface: string;
}

/** The palette for the current theme; follows theme changes. */
export function usePalette(): Palette {
  const [dark, setDark] = useState(isDark);
  useEffect(() => {
    const on = () => setDark(isDark());
    window.addEventListener('sprig-theme', on);
    const mq = window.matchMedia?.('(prefers-color-scheme: dark)');
    mq?.addEventListener?.('change', on);
    return () => {
      window.removeEventListener('sprig-theme', on);
      mq?.removeEventListener?.('change', on);
    };
  }, []);
  return dark
    ? { series: DARK, seq: SEQ_DARK, grid: '#2b2f36', text: '#e6e8eb', muted: '#9aa1ab', surface: '#17191d' }
    : { series: LIGHT, seq: SEQ_LIGHT, grid: '#e0e3e8', text: '#1c2026', muted: '#5d6573', surface: '#ffffff' };
}

const slotColour = (pal: Palette, slot: number) => (slot < 8 ? pal.series[slot]! : pal.muted);

const tick = (pal: Palette) => ({ fill: pal.muted, fontSize: 11 });

function tooltipStyle(pal: Palette) {
  return {
    contentStyle: { background: pal.surface, border: `1px solid ${pal.grid}`, borderRadius: 6, color: pal.text, fontSize: 12 },
    labelStyle: { color: pal.text, fontWeight: 600 },
    itemStyle: { color: pal.text },
    cursor: { fill: pal.grid, fillOpacity: 0.4 },
  };
}

const short = (v: unknown) => {
  const s = typeof v === 'string' ? v : v === null || v === undefined ? '' : JSON.stringify(v);
  return s.length > 18 ? `${s.slice(0, 17)}…` : s;
};

/** A chart's box, read as one image; a map, which takes the keyboard and
 * has buttons, is a figure instead. */
function Frame({ label, children, height, interactive = false }: { label: string; children: ReactNode; height: number | string; interactive?: boolean }) {
  return (
    <div className="chart-frame" role={interactive ? 'figure' : 'img'} aria-label={label} style={{ height }}>
      {children}
    </div>
  );
}

function Cartesian({ p, pal, format }: { p: Extract<Plan, { kind: 'cartesian' }>; pal: Palette; format: (v: unknown) => string }) {
  const vertical = p.layout === 'vertical';
  const valueFmt = (v: unknown) => (p.percent ? `${Math.round(num(v) ?? 0)}%` : format(v));
  const cat = vertical ? (
    <YAxis type="category" dataKey={p.xKey || undefined} tick={tick(pal)} width={110} tickFormatter={short} />
  ) : (
    <XAxis dataKey={p.xKey || undefined} tick={tick(pal)} tickFormatter={short} minTickGap={8} />
  );
  const val = (id: 'left' | 'right') =>
    vertical ? (
      <XAxis key={id} xAxisId={id} type="number" tick={tick(pal)} tickFormatter={valueFmt} orientation={id === 'right' ? 'top' : 'bottom'} hide={id === 'right' && !p.rightAxis} />
    ) : (
      <YAxis key={id} yAxisId={id} tick={tick(pal)} tickFormatter={valueFmt} orientation={id} width={56} hide={id === 'right' && !p.rightAxis} />
    );
  const multi = p.series.length > 1;
  const children = [
    <CartesianGrid key="g" stroke={pal.grid} vertical={vertical} horizontal={!vertical} />,
    cat,
    val('left'),
    val('right'),
    <Tooltip key="t" {...tooltipStyle(pal)} formatter={(v) => valueFmt(v)} />,
    multi ? <Legend key="l" wrapperStyle={{ fontSize: 12, color: pal.text }} /> : null,
    ...p.series.map((s) => {
      const colour = slotColour(pal, s.slot);
      const axis = vertical ? { xAxisId: s.yAxisId } : { yAxisId: s.yAxisId };
      const label = p.labels ? <LabelList dataKey={s.dataKey} position={vertical ? 'right' : 'top'} formatter={valueFmt} fill={pal.text} fontSize={11} /> : null;
      if (s.component === 'Line') {
        return (
          <Line key={s.dataKey} {...axis} type="monotone" dataKey={s.dataKey} name={s.name} stroke={colour} strokeWidth={2} dot={p.data.length <= 40 ? { r: 3 } : false} isAnimationActive={false}>
            {label}
          </Line>
        );
      }
      if (s.component === 'Area') {
        return (
          <Area key={s.dataKey} {...axis} type="monotone" dataKey={s.dataKey} name={s.name} stroke={colour} fill={colour} fillOpacity={0.25} strokeWidth={2} stackId={s.stackId} isAnimationActive={false}>
            {label}
          </Area>
        );
      }
      return (
        <Bar key={s.dataKey} {...axis} dataKey={s.dataKey} name={s.name} fill={colour} stackId={s.stackId} radius={s.stackId ? 0 : vertical ? [0, 4, 4, 0] : [4, 4, 0, 0]} maxBarSize={48} isAnimationActive={false}>
          {label}
        </Bar>
      );
    }),
  ];
  const common = { data: p.data, layout: p.layout, margin: { top: 8, right: 12, bottom: 4, left: 4 } } as const;
  const chart =
    p.chart === 'BarChart' ? (
      <BarChart {...common} barGap={2}>{children}</BarChart>
    ) : p.chart === 'LineChart' ? (
      <LineChart {...common}>{children}</LineChart>
    ) : p.chart === 'AreaChart' ? (
      <AreaChart {...common}>{children}</AreaChart>
    ) : (
      <ComposedChart {...common}>{children}</ComposedChart>
    );
  return <ResponsiveContainer width="100%" height="100%">{chart}</ResponsiveContainer>;
}

function Matrix({ p, pal, format }: { p: Extract<Plan, { kind: 'matrix' }>; pal: Palette; format: (v: unknown) => string }) {
  const { m } = p;
  const step = (v: number | null) => {
    if (v === null) return 'transparent';
    const t = m.max > m.min ? (v - m.min) / (m.max - m.min) : 1;
    return pal.seq[Math.min(pal.seq.length - 1, Math.floor(t * (pal.seq.length - 1) + 0.5))]!;
  };
  const ink = (v: number | null) => {
    if (v === null) return pal.text;
    const t = m.max > m.min ? (v - m.min) / (m.max - m.min) : 1;
    return (t > 0.55) !== isDark() ? '#ffffff' : pal.text;
  };
  return (
    <div className="chart-scroll">
      <table className="matrix" aria-label={`${m.measure} by ${m.xLabel}${m.yLabel ? ` and ${m.yLabel}` : ''}`}>
        <thead>
          <tr>
            <th scope="col">{m.yLabel}</th>
            {m.xs.map((x) => (
              <th key={x} scope="col">
                {x}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {m.ys.map((y) => (
            <tr key={y}>
              <th scope="row">{y}</th>
              {m.xs.map((x) => {
                const v = m.cell(x, y);
                return (
                  <td key={x} style={{ background: step(v), color: ink(v) }} title={`${x} · ${y}: ${format(v)}`} className="n">
                    {p.numbers ? format(v) : <span className="visually-hidden">{format(v)}</span>}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Table({ heads, rows, format }: { heads: string[]; rows: Rec[]; format: (v: unknown) => string }) {
  return (
    <div className="chart-scroll">
      <table className="tile-table">
        <thead>
          <tr>
            {heads.map((h) => (
              <th key={h} scope="col">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {heads.map((h) => {
                const v = r[h];
                return (
                  <td key={h} className={typeof v === 'number' ? 'n' : undefined}>
                    {typeof v === 'number' ? format(v) : String(v ?? '')}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** One chart, no facets. */
function One({ p, pal, format, title }: { p: Plan; pal: Palette; format: (v: unknown) => string; title: string }) {
  const tt = tooltipStyle(pal);
  switch (p.kind) {
    case 'cartesian':
      return <Cartesian p={p} pal={pal} format={format} />;
    case 'scatter':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} />
            <XAxis type="number" dataKey={p.xKey} name={p.xKey} tick={tick(pal)} tickFormatter={format} />
            <YAxis type="number" dataKey={p.yKey} name={p.yKey} tick={tick(pal)} tickFormatter={format} width={56} />
            {p.zKey && <ZAxis type="number" dataKey={p.zKey} name={p.zKey} range={[40, 600]} />}
            <Tooltip {...tt} formatter={(v) => format(v)} />
            {p.groups.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {p.groups.map((g) => (
              <Scatter key={g.name} name={g.name} data={g.data} fill={slotColour(pal, g.slot)} fillOpacity={0.8} stroke={pal.surface} strokeWidth={1} isAnimationActive={false} />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      );
    case 'pie':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Tooltip {...tt} formatter={(v) => format(v)} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Pie data={p.data} dataKey="value" nameKey="name" innerRadius={p.donut ? '55%' : 0} outerRadius="80%" stroke={pal.surface} strokeWidth={2} isAnimationActive={false}>
              {p.data.map((d) => (
                <Cell key={d.name} fill={slotColour(pal, d.slot)} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
      );
    case 'funnel':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <FunnelChart>
            <Tooltip {...tt} formatter={(v) => format(v)} />
            <Funnel data={p.data} dataKey="value" nameKey="name" isAnimationActive={false} stroke={pal.surface}>
              {p.data.map((d) => (
                <Cell key={d.name} fill={slotColour(pal, 0)} fillOpacity={1 - d.slot * (0.6 / Math.max(1, p.data.length))} />
              ))}
              <LabelList position="right" dataKey="name" fill={pal.text} fontSize={11} />
            </Funnel>
          </FunnelChart>
        </ResponsiveContainer>
      );
    case 'radar':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <RadarChart data={p.data} outerRadius="75%">
            <PolarGrid stroke={pal.grid} />
            <PolarAngleAxis dataKey={p.angleKey} tick={tick(pal)} tickFormatter={short} />
            <PolarRadiusAxis tick={tick(pal)} tickFormatter={format} />
            <Tooltip {...tt} formatter={(v) => format(v)} />
            {p.series.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {p.series.map((s) => (
              <Radar key={s.dataKey} dataKey={s.dataKey} name={s.name} stroke={slotColour(pal, s.slot)} fill={slotColour(pal, s.slot)} fillOpacity={0.2} isAnimationActive={false} />
            ))}
          </RadarChart>
        </ResponsiveContainer>
      );
    case 'treemap':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <Treemap data={p.data} dataKey="size" nameKey="name" stroke={pal.surface} fill={slotColour(pal, 0)} isAnimationActive={false}>
            <Tooltip {...tt} formatter={(v) => format(v)} />
          </Treemap>
        </ResponsiveContainer>
      );
    case 'sankey':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <Sankey data={p.data} nodePadding={16} margin={{ top: 8, right: 120, bottom: 8, left: 8 }} link={{ stroke: pal.series[0]!, strokeOpacity: 0.3 }} node={{ fill: pal.series[0]!, stroke: pal.surface }}>
            <Tooltip {...tt} formatter={(v) => format(v)} />
          </Sankey>
        </ResponsiveContainer>
      );
    case 'histogram':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={p.data} barCategoryGap={1} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} vertical={false} />
            <XAxis dataKey="bin" tick={tick(pal)} minTickGap={12} />
            <YAxis tick={tick(pal)} allowDecimals={false} width={44} />
            <Tooltip {...tt} />
            <Bar dataKey="count" name={`Rows by ${p.measure}`} fill={slotColour(pal, 0)} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      );
    case 'box':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={p.data.map((b) => ({ ...b, span: [b.min, b.max] }))} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} vertical={false} />
            <XAxis dataKey="name" tick={tick(pal)} tickFormatter={short} />
            <YAxis tick={tick(pal)} tickFormatter={format} width={56} domain={['auto', 'auto']} />
            <Tooltip
              {...tt}
              content={({ active, payload }) => {
                const b = active && payload?.[0] ? (payload[0].payload as (typeof p.data)[number]) : null;
                return b ? (
                  <div className="chart-tip">
                    <b>{b.name}</b>
                    <div>max {format(b.max)} · q3 {format(b.q3)}</div>
                    <div>median {format(b.median)}</div>
                    <div>q1 {format(b.q1)} · min {format(b.min)}</div>
                    <div className="muted">{b.n} rows</div>
                  </div>
                ) : null;
              }}
            />
            <Bar
              dataKey="span"
              name={p.measure}
              fill={slotColour(pal, 0)}
              isAnimationActive={false}
              shape={(props: unknown) => {
                const { x, y, width, height, payload } = props as { x: number; y: number; width: number; height: number; payload: (typeof p.data)[number] };
                const span = payload.max - payload.min || 1;
                const at = (v: number) => y + (height * (payload.max - v)) / span;
                const cx = x + width / 2;
                const bw = Math.min(width, 40);
                const c = slotColour(pal, 0);
                return (
                  <g>
                    <line x1={cx} x2={cx} y1={at(payload.max)} y2={at(payload.min)} stroke={pal.text} strokeWidth={1} />
                    <line x1={cx - bw / 4} x2={cx + bw / 4} y1={at(payload.max)} y2={at(payload.max)} stroke={pal.text} />
                    <line x1={cx - bw / 4} x2={cx + bw / 4} y1={at(payload.min)} y2={at(payload.min)} stroke={pal.text} />
                    <rect x={cx - bw / 2} y={at(payload.q3)} width={bw} height={Math.max(1, at(payload.q1) - at(payload.q3))} fill={c} fillOpacity={0.35} stroke={c} rx={2} />
                    <line x1={cx - bw / 2} x2={cx + bw / 2} y1={at(payload.median)} y2={at(payload.median)} stroke={pal.text} strokeWidth={2} />
                  </g>
                );
              }}
            />
          </BarChart>
        </ResponsiveContainer>
      );
    case 'waterfall':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={p.data} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} vertical={false} />
            <XAxis dataKey="name" tick={tick(pal)} tickFormatter={short} />
            <YAxis tick={tick(pal)} tickFormatter={format} width={56} />
            <Tooltip {...tt} formatter={(_v, _n, item) => format((item?.payload as { delta: number }).delta)} />
            <Bar dataKey="range" name={p.measure} isAnimationActive={false}>
              {p.data.map((d) => (
                <Cell key={d.name} fill={d.total ? pal.muted : d.delta >= 0 ? slotColour(pal, 0) : slotColour(pal, 1)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      );
    case 'gantt': {
      const tasks = p.data.map((d, i) => ({ ...d, row: `${d.task}${'​'.repeat(i % 3)}` }));
      const day = (t: unknown) => new Date(Number(t)).toISOString().slice(0, 10);
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={tasks} layout="vertical" margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} horizontal={false} />
            <XAxis type="number" domain={['dataMin', 'dataMax']} tickFormatter={day} tick={tick(pal)} scale="time" />
            <YAxis type="category" dataKey="task" tick={tick(pal)} width={110} tickFormatter={short} />
            <Tooltip {...tt} formatter={(v) => (Array.isArray(v) ? `${day(v[0])} → ${day(v[1])}` : String(v))} />
            <Bar dataKey="range" name="Dates" fill={slotColour(pal, 0)} radius={4} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      );
    }
    case 'kpi': {
      const change = p.value !== null && p.previous ? (p.value - p.previous) / Math.abs(p.previous) : null;
      return (
        <div className="kpi">
          <div className="kpi-value" data-testid="kpi-value">
            {format(p.value)}
          </div>
          <div className="kpi-label muted">
            {p.label}
            {change !== null && (
              <span className="kpi-change">
                {' '}
                {change >= 0 ? '▲' : '▼'} {Math.abs(change * 100).toFixed(1)}% from the period before
              </span>
            )}
          </div>
          {p.spark.length > 1 && (
            <div className="kpi-spark" aria-hidden="true">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={p.spark} accessibilityLayer={false} tabIndex={-1}>
                  <Line dataKey="v" stroke={slotColour(pal, 0)} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      );
    }
    case 'gauge':
      return (
        <div className="gauge">
          <ResponsiveContainer width="100%" height="100%">
            <RadialBarChart data={[{ name: p.label, value: p.value, fill: slotColour(pal, 0) }]} startAngle={210} endAngle={-30} innerRadius="70%" outerRadius="100%">
              <PolarRadiusAxis type="number" domain={[0, p.max]} tick={false} axisLine={false} />
              <RadialBar dataKey="value" background={{ fill: pal.grid }} cornerRadius={6} isAnimationActive={false} />
            </RadialBarChart>
          </ResponsiveContainer>
          <div className="gauge-value">
            <b>{format(p.value)}</b>
            <span className="muted"> of {format(p.max)}</span>
          </div>
        </div>
      );
    case 'bullet':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={p.data} layout="vertical" margin={{ top: 8, right: 16, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={pal.grid} horizontal={false} />
            <XAxis type="number" tick={tick(pal)} tickFormatter={format} />
            <YAxis type="category" dataKey="name" tick={tick(pal)} width={110} tickFormatter={short} />
            <Tooltip {...tt} formatter={(v) => format(v)} />
            <Bar dataKey="value" name={p.measure} fill={slotColour(pal, 0)} barSize={14} radius={[0, 4, 4, 0]} isAnimationActive={false} />
            <Scatter
              dataKey="target"
              name="Target"
              fill={pal.text}
              isAnimationActive={false}
              shape={(props: unknown) => {
                const { cx, cy } = props as { cx: number; cy: number };
                return <rect x={cx - 1.5} y={cy - 11} width={3} height={22} fill={pal.text} />;
              }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      );
    case 'matrix':
      return <Matrix p={p} pal={pal} format={format} />;
    case 'pivot':
      return (
        <div className="chart-scroll">
          <table className="tile-table pivot">
            <thead>
              <tr>
                {p.p.rowDims.map((d) => (
                  <th key={d} scope="col">
                    {d}
                  </th>
                ))}
                {p.p.heads.map((h) => (
                  <th key={h} scope="col" className="n">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {p.p.rows.map((r, i) => (
                <tr key={i} className={`pv-${r.kind}`}>
                  {r.keys.map((k, j) => (
                    <th key={j} scope="row">
                      {r.kind === 'subtotal' && j === r.keys.findIndex((x) => x === '') ? 'Subtotal' : k}
                    </th>
                  ))}
                  {r.values.map((v, j) => (
                    <td key={j} className="n">
                      {format(v)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    case 'table':
      return <Table heads={p.heads} rows={p.rows} format={format} />;
    case 'map':
    case 'filled':
      return (
        <Suspense fallback={<p className="muted chart-note">Loading the map…</p>}>
          <MapView m={p} pal={pal} format={format} />
        </Suspense>
      );
    case 'unavailable':
    case 'empty':
      return <p className="muted chart-note">{p.why}</p>;
  }
  void title;
}

/** A spec drawn over a result; small multiples when it has a facet. */
export function ChartView({
  spec,
  columns,
  rows,
  title,
  height = '100%',
}: {
  spec: Spec;
  columns: ColumnInfo[];
  rows: unknown[][];
  title: string;
  height?: number | string;
}) {
  const pal = usePalette();
  const firstMeasure = [...spec.rows, ...spec.columns].find((f) => f.agg !== undefined)?.field;
  const format = (v: unknown) => fmt(v, firstMeasure ? spec.format[firstMeasure] : undefined);
  const parts = useMemo(() => (spec.facet ? facets(spec, columns, rows) : []), [spec, columns, rows]);
  const single = useMemo(() => (spec.facet ? null : plan(spec, columns, rows)), [spec, columns, rows]);
  if (single) {
    const cut = 'cut' in single && single.cut ? single.cut : 0;
    return (
      <div className="chart-wrap" style={{ height }}>
        <Frame label={describePlan(single, title)} height={cut ? 'calc(100% - 18px)' : '100%'} interactive={single.kind === 'map' || single.kind === 'filled'}>
          <One p={single} pal={pal} format={format} title={title} />
        </Frame>
        {cut > 0 && <p className="muted chart-cut">{cut} more categories not shown</p>}
      </div>
    );
  }
  const inner = { ...spec, facet: null };
  return (
    <div className="facets" style={{ height }} data-testid="small-multiples">
      {parts.map((f) => {
        const p = plan(inner, columns, f.rows);
        return (
          <div className="facet" key={f.value}>
            <div className="facet-title">{f.value}</div>
            <Frame label={describePlan(p, `${title}, ${f.value}`)} height="calc(100% - 20px)" interactive={p.kind === 'map' || p.kind === 'filled'}>
              <One p={p} pal={pal} format={format} title={title} />
            </Frame>
          </div>
        );
      })}
    </div>
  );
}

export type { Rec };
