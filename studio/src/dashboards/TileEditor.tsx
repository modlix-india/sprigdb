// The chart editor: a tile's query and its result's fields (dimensions,
// dates, measures), the shelves (Columns, Rows, Filters; Colour, Size,
// Label, Detail, Tooltip on the Marks card), aggregations and date parts,
// sort, top-N, stacking, facets, and Show Me, which lights up the chart
// types that fit the shelves by the same rules the assistant's tiles are
// checked with.

import { useEffect, useMemo, useState, type DragEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useLocation, useNavigate, useParams, useSearchParams } from 'react-router';
import { errorText } from '../api/errors';
import { useDatabases } from '../catalog/snapshot';
import { useRel } from '../workspace/WorkspaceTabs';
import { addTile, placeholders, runQuery, updateTile, useDashboard, useStoreDashboard, type Dashboard, type Tile, type TileInput, type TileRun } from './api';
import { ChartView } from './ChartView';
import {
  AGGS,
  DATE_PARTS,
  MARKS,
  PRESETS,
  applyPreset,
  best,
  counts,
  fits,
  normalize,
  presetFits,
  presetOf,
  roleOf,
  toStored,
  DRAWN,
  type Agg,
  type ChartSpec,
  type DatePart,
  type Field,
  type Mark,
  type Spec,
} from './spec';
import { labelOf } from './data';
import './dashboards.css';

type ShelfName = 'columns' | 'rows' | 'color' | 'size' | 'label' | 'detail' | 'tooltip' | 'facet';
const LIST_SHELVES: ShelfName[] = ['columns', 'rows', 'detail', 'tooltip'];

const MARK_NAMES: Record<Mark, string> = {
  bar: 'Bar',
  line: 'Line',
  area: 'Area',
  point: 'Scatter',
  pie: 'Pie',
  donut: 'Donut',
  waterfall: 'Waterfall',
  histogram: 'Histogram',
  box: 'Box plot',
  heatmap: 'Heatmap',
  'highlight-table': 'Highlight table',
  treemap: 'Treemap',
  funnel: 'Funnel',
  radar: 'Radar',
  sankey: 'Sankey',
  kpi: 'KPI',
  gauge: 'Gauge',
  bullet: 'Bullet',
  gantt: 'Gantt',
  'text-table': 'Text table',
  pivot: 'Pivot table',
  'map-point': 'Point map',
  'map-filled': 'Filled map',
};

const ICONS: Record<string, string> = {
  'text-table': '▤',
  'highlight-table': '▦',
  heatmap: '▩',
  pivot: '⊞',
  bar: '▮',
  hbar: '▬',
  stacked: '▮▮',
  percent: '▯',
  line: '╱',
  area: '◢',
  'stacked-area': '◣',
  combo: '▮╱',
  waterfall: '⇵',
  point: '∴',
  bubble: '⚬',
  pie: '◔',
  donut: '◎',
  histogram: '▙',
  box: '⊟',
  treemap: '▣',
  funnel: '⏷',
  radar: '✶',
  sankey: '≋',
  kpi: '42',
  gauge: '◠',
  bullet: '▭',
  gantt: '▭▭',
  'map-point': '◍',
  'map-filled': '◉',
};

function shelfList(s: Spec, name: ShelfName): Field[] {
  const v = s[name];
  return Array.isArray(v) ? v : v ? [v] : [];
}

function setShelf(s: Spec, name: ShelfName, list: Field[]): Spec {
  if (LIST_SHELVES.includes(name)) return { ...s, [name]: list };
  return { ...s, [name]: list[list.length - 1] ?? null };
}

/** A field dropped on a shelf, given its role: measures get SUM, dates their exact value. */
function fieldFor(name: string, role: 'measure' | 'dimension' | 'date', shelf: ShelfName): Field {
  if (role === 'measure' && shelf !== 'detail') return { field: name, agg: 'sum' };
  if (role === 'date') return { field: name, datePart: shelf === 'columns' || shelf === 'rows' ? 'month' : 'exact' };
  return { field: name };
}

function Pill({
  f,
  onChange,
  onRemove,
  canAxis,
}: {
  f: Field;
  onChange: (f: Field) => void;
  onRemove: () => void;
  canAxis: boolean;
}) {
  const cls = f.agg !== undefined ? 'mea' : f.datePart ? 'date' : 'dim';
  return (
    <span className={`pill ${cls}`} data-testid={`pill-${labelOf(f)}`}>
      {f.agg !== undefined ? (
        <select aria-label={`Aggregation of ${f.field}`} value={f.agg} onChange={(e) => onChange({ ...f, agg: e.target.value as Agg })}>
          {AGGS.map((a) => (
            <option key={a} value={a}>
              {a === 'countd' ? 'CNTD' : a.toUpperCase()}
            </option>
          ))}
        </select>
      ) : f.datePart ? (
        <select aria-label={`Date part of ${f.field}`} value={f.datePart} onChange={(e) => onChange({ ...f, datePart: e.target.value as DatePart })}>
          {DATE_PARTS.map((d) => (
            <option key={d} value={d}>
              {d.toUpperCase()}
            </option>
          ))}
        </select>
      ) : null}
      <span>{f.field}</span>
      {canAxis && f.agg !== undefined && (
        <button
          aria-pressed={f.axis === 'right'}
          title="On the right axis, as a line (a combo chart)"
          aria-label={`${f.field} on the right axis as a line`}
          onClick={() => onChange(f.axis === 'right' ? { field: f.field, agg: f.agg } : { ...f, axis: 'right', mark: 'line' })}
        >
          {f.axis === 'right' ? '⇥' : '⇤'}
        </button>
      )}
      <button aria-label={`Remove ${labelOf(f)}`} onClick={onRemove}>
        ✕
      </button>
    </span>
  );
}

export function TileEditor() {
  const { id = '', tile = 'new' } = useParams();
  const loc = useLocation();
  const dash = useDashboard(id);
  const draft = (loc.state as { draft?: Partial<TileInput> } | null)?.draft;
  if (dash.isPending) return <p className="muted">Loading…</p>;
  if (!dash.data) return <p className="error">{errorText(dash.error)}</p>;
  const existing = dash.data.tiles.find((t) => t.id === tile);
  return <Editor key={`${id}/${tile}`} dash={dash.data} existing={existing} draft={draft} />;
}

function Editor({ dash, existing, draft }: { dash: Dashboard; existing: Tile | undefined; draft: Partial<TileInput> | undefined }) {
  const id = dash.id;
  const [search] = useSearchParams();
  const navigate = useNavigate();
  const { base } = useRel();
  const store = useStoreDashboard();
  const dbs = useDatabases();
  const src: Partial<TileInput> | undefined = existing ?? draft;
  const [title, setTitle] = useState(src?.title ?? 'New tile');
  const [db, setDb] = useState<string>(src?.database ?? '');
  const [sql, setSql] = useState(src?.sql ?? '');
  const [refresh, setRefresh] = useState(src?.refresh_seconds ?? 0);
  const [stored, setStored] = useState<ChartSpec>(src?.chart ?? { mark: 'bar' });
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [over, setOver] = useState<string | null>(null);
  // What was last run: the query runs when the editor opens and on Run.
  const [ran, setRan] = useState({ sql: src?.sql ?? '', db: src?.database ?? '', n: 0 });
  const runQ = useQuery({
    queryKey: ['tile-editor-run', id, ran],
    queryFn: ({ signal }) => runQuery(ran.db || null, ran.sql, dash.filters, {}, signal),
    enabled: !!ran.sql.trim(),
    retry: false,
  });
  const run: TileRun | null = runQ.data ?? null;
  const running = runQ.isFetching;

  const columns = useMemo(() => run?.columns ?? [], [run]);
  const spec = useMemo(() => normalize(stored, columns), [stored, columns]);
  const setSpec = (s: Spec) => setStored(toStored(s));
  const c = counts(toStored(spec));
  const exact = { ...c, unknown: 0 };
  const pick = best(exact);
  const doRun = () => setRan({ sql, db, n: ran.n + 1 });

  // "Change chart" in a tile's menu opens the editor at Show Me.
  const focusShowMe = search.get('focus') === 'showme';
  useEffect(() => {
    if (focusShowMe) document.getElementById('showme')?.focus();
  }, [focusShowMe]);

  const missing = placeholders(sql).filter((n) => !dash.filters.some((f) => f.name.toLowerCase() === n.toLowerCase()));

  const save = async () => {
    setSaving(true);
    setError(null);
    const body: TileInput = { title: title.trim(), database: db || null, sql, chart: toStored(spec), refresh_seconds: refresh };
    try {
      const r = existing ? await updateTile(id, existing.id, body) : await addTile(id, body);
      store(r.dashboard);
      void navigate(`${base}/d/${id}`);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setSaving(false);
    }
  };

  // ---- drag and drop (and the keyboard's "Add to" menus) ----
  const drop = (shelf: ShelfName) => ({
    onDragOver: (e: DragEvent) => {
      e.preventDefault();
      setOver(shelf);
    },
    onDragLeave: () => setOver(null),
    onDrop: (e: DragEvent) => {
      e.preventDefault();
      setOver(null);
      const name = e.dataTransfer.getData('text/x-sprig-field') || e.dataTransfer.getData('text/plain');
      if (name) addTo(shelf, name);
    },
  });
  const addTo = (shelf: ShelfName, name: string) => {
    const col = columns.find((x) => x.name === name);
    const role = col ? roleOf(col) : 'dimension';
    const f = fieldFor(name, role, shelf);
    const list = shelfList(spec, shelf);
    setSpec(setShelf(spec, shelf, [...list, f]));
  };
  const shelfUi = (shelf: ShelfName, label: string, compact = false) => {
    const list = shelfList(spec, shelf);
    const pills = list.map((f, i) => (
      <Pill
        key={`${labelOf(f)}-${i}`}
        f={f}
        canAxis={shelf === 'rows' && list.filter((x) => x.agg !== undefined).length > 1}
        onChange={(nf) => setSpec(setShelf(spec, shelf, list.map((x, j) => (j === i ? nf : x))))}
        onRemove={() => setSpec(setShelf(spec, shelf, list.filter((_, j) => j !== i)))}
      />
    ));
    if (compact) {
      return (
        <div className={`mk${over === shelf ? ' over' : ''}`} {...drop(shelf)} data-testid={`shelf-${shelf}`} aria-label={`${label} shelf`} role="group">
          <span>{label}</span>
          {pills}
        </div>
      );
    }
    return (
      <div className={`shelf${over === shelf ? ' over' : ''}`} {...drop(shelf)} data-testid={`shelf-${shelf}`} role="group" aria-label={`${label} shelf`}>
        <b>{label}</b>
        <div className="drop">{pills.length ? pills : <span className="muted small">drop fields here</span>}</div>
      </div>
    );
  };

  const roles = columns.map((col) => ({ col, role: roleOf(col) }));
  const fieldRow = (col: { name: string; type: string }, role: 'measure' | 'dimension' | 'date') => (
    <li key={col.name}>
      <span
        className={`pill ${role === 'measure' ? 'mea' : role === 'date' ? 'date' : 'dim'}`}
        draggable
        onDragStart={(e) => {
          e.dataTransfer.setData('text/x-sprig-field', col.name);
          e.dataTransfer.setData('text/plain', col.name);
        }}
        title={`${col.name}: ${col.type}`}
        data-testid={`field-${col.name}`}
      >
        <span aria-hidden="true">{role === 'measure' ? '#' : role === 'date' ? '📅︎' : 'Abc'}</span> {col.name}
      </span>{' '}
      <select
        aria-label={`Add ${col.name} to a shelf`}
        value=""
        onChange={(e) => {
          if (e.target.value) addTo(e.target.value as ShelfName, col.name);
        }}
        data-testid={`add-${col.name}`}
      >
        <option value="">＋</option>
        <option value="columns">Columns</option>
        <option value="rows">Rows</option>
        <option value="color">Colour</option>
        <option value="size">Size</option>
        <option value="label">Label</option>
        <option value="detail">Detail</option>
        <option value="tooltip">Tooltip</option>
        <option value="facet">Facet (small multiples)</option>
      </select>
    </li>
  );
  const allFields = [...spec.columns, ...spec.rows, ...(spec.color ? [spec.color] : [])];
  const sortOptions = allFields.map(labelOf);
  return (
    <div className="ce" data-testid="chart-editor">
      <aside className="ce-data" aria-label="Query and fields">
        <h3>Query</h3>
        <label className="stack small">
          <span className="muted">Database</span>
          <select value={db} onChange={(e) => setDb(e.target.value)} aria-label="Database">
            <option value="">(none)</option>
            {(dbs.data ?? []).map((d) => (
              <option key={d}>{d}</option>
            ))}
            {db && !(dbs.data ?? []).includes(db) && <option>{db}</option>}
          </select>
        </label>
        <textarea className="ce-sql" aria-label="Tile query" value={sql} onChange={(e) => setSql(e.target.value)} spellCheck={false} data-testid="tile-sql" />
        <button onClick={doRun} disabled={running || !sql.trim()} data-testid="tile-run">
          {running ? 'Running…' : 'Run'}
        </button>
        {missing.length > 0 && (
          <p className="error small">
            {missing.map((m) => `{{${m}}}`).join(', ')} isn’t a filter of this dashboard yet: add it on the dashboard in edit mode.
          </p>
        )}
        {runQ.error && <p className="error small">{errorText(runQ.error)}</p>}
        {run && (
          <p className="muted small">
            {run.row_count} rows · {run.ms} ms{run.truncated ? ' · cut at studio.max_result_rows' : ''}
          </p>
        )}
        <h3>Dimensions</h3>
        <ul className="fields">{roles.filter((r) => r.role !== 'measure').map((r) => fieldRow(r.col, r.role))}</ul>
        <h3>Measures</h3>
        <ul className="fields">{roles.filter((r) => r.role === 'measure').map((r) => fieldRow(r.col, r.role))}</ul>
        {!run && <p className="muted small">Run the query to see its fields.</p>}
      </aside>
      <div className="ce-main">
        {shelfUi('columns', 'Columns')}
        {shelfUi('rows', 'Rows')}
        <div className="shelf" role="group" aria-label="Filters shelf">
          <b>Filters</b>
          <div className="drop">
            {spec.filters.map((f, i) => (
              <span className="pill dim" key={f.field}>
                {f.field}: {f.in.map(String).join(', ')}
                <button aria-label={`Remove filter on ${f.field}`} onClick={() => setSpec({ ...spec, filters: spec.filters.filter((_, j) => j !== i) })}>
                  ✕
                </button>
              </span>
            ))}
            <select
              aria-label="Filter rows by a field"
              value=""
              onChange={(e) => {
                const name = e.target.value;
                if (!name || !run) return;
                const i = run.columns.findIndex((x) => x.name === name);
                const values = [...new Set(run.rows.map((r) => r[i] === null || r[i] === undefined ? 'NULL' : typeof r[i] === 'string' ? r[i] : JSON.stringify(r[i])))].slice(0, 50);
                const keep = window.prompt(`Keep rows where ${name} is one of (comma-separated):`, values.slice(0, 5).join(', '));
                if (keep === null) return;
                setSpec({ ...spec, filters: [...spec.filters, { field: name, in: keep.split(',').map((x) => x.trim()).filter(Boolean) }] });
              }}
            >
              <option value="">＋ filter</option>
              {columns.map((col) => (
                <option key={col.name}>{col.name}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="ce-body">
          <div className="marks" aria-label="Marks">
            <h3>Marks</h3>
            <select aria-label="Mark" value={spec.mark} onChange={(e) => setSpec({ ...spec, mark: e.target.value as Mark })} data-testid="mark-select">
              {MARKS.map((m) => (
                <option key={m} value={m} disabled={!DRAWN.has(m)}>
                  {MARK_NAMES[m]}
                  {fits(m, exact) ? '' : ' (doesn’t fit the shelves)'}
                  {DRAWN.has(m) ? '' : ' (not drawn yet)'}
                </option>
              ))}
            </select>
            {shelfUi('color', 'Colour', true)}
            {shelfUi('size', 'Size', true)}
            {shelfUi('label', 'Label', true)}
            {shelfUi('detail', 'Detail', true)}
            {shelfUi('tooltip', 'Tooltip', true)}
            <label className="row">
              Stack
              <select aria-label="Stack" value={spec.stack} onChange={(e) => setSpec({ ...spec, stack: e.target.value as Spec['stack'] })}>
                <option value="none">none</option>
                <option value="stack">stacked</option>
                <option value="percent">100%</option>
              </select>
            </label>
            <label className="row">
              Sort
              <select
                aria-label="Sort by"
                value={spec.sort ? `${spec.sort.by}|${spec.sort.dir}` : ''}
                onChange={(e) => {
                  const [by, dir] = e.target.value.split('|');
                  setSpec({ ...spec, sort: by ? { by, dir: dir === 'asc' ? 'asc' : 'desc' } : null });
                }}
              >
                <option value="">as the query returns</option>
                {sortOptions.flatMap((o) => [
                  <option key={`${o}a`} value={`${o}|asc`}>
                    {o} ↑
                  </option>,
                  <option key={`${o}d`} value={`${o}|desc`}>
                    {o} ↓
                  </option>,
                ])}
              </select>
            </label>
            <label className="row">
              Top
              <input
                aria-label="Top N"
                type="number"
                min={0}
                style={{ inlineSize: 70 }}
                value={spec.top ?? ''}
                onChange={(e) => setSpec({ ...spec, top: e.target.value ? Math.max(0, Number(e.target.value)) || null : null })}
              />
            </label>
            {shelfUi('facet', 'Facet', true)}
          </div>
          <div className="canvas">
            <div className="row">
              <input aria-label="Tile title" value={title} onChange={(e) => setTitle(e.target.value)} style={{ flex: 1, fontWeight: 600 }} data-testid="tile-title" />
              <span className="dash-badge">{presetOf(spec)}</span>
              <label className="row small">
                Refresh
                <select aria-label="Refresh" value={refresh} onChange={(e) => setRefresh(Number(e.target.value))}>
                  <option value={0}>when opened</option>
                  <option value={60}>every minute</option>
                  <option value={300}>every 5 min</option>
                  <option value={900}>every 15 min</option>
                  <option value={3600}>every hour</option>
                </select>
              </label>
              <button onClick={() => void navigate(`${base}/d/${id}`)}>Cancel</button>
              <button className="primary" onClick={() => void save()} disabled={saving || !title.trim() || !sql.trim()} data-testid="save-tile">
                Save tile
              </button>
            </div>
            {error && (
              <p className="error" role="alert">
                {error}
              </p>
            )}
            <div className="canvas-chart">
              {run ? <ChartView spec={spec} columns={run.columns} rows={run.rows} title={title} /> : <p className="muted">Run the query to draw the chart.</p>}
            </div>
            {!fits(spec.mark, exact) && (
              <p className="banner small" role="status">
                A {MARK_NAMES[spec.mark].toLowerCase()} chart doesn’t fit {c.dims} dimension(s) and {c.measures} measure(s); Show Me suggests {MARK_NAMES[pick.mark].toLowerCase()}. The tile can’t be saved until it fits.
              </p>
            )}
          </div>
        </div>
      </div>
      <aside className="showme" aria-label="Show Me" id="showme" tabIndex={-1}>
        <h3>Show Me</h3>
        <div className="gallery">
          {PRESETS.map((p) => {
            const ok = presetFits(p, exact) && DRAWN.has(p.mark);
            const on = presetOf(spec) === p.id;
            return (
              <button
                key={p.id}
                aria-pressed={on}
                disabled={!ok}
                className={p.mark === pick.mark && !p.stack && !p.horizontal && !p.combo && p.id === pick.mark ? 'best' : undefined}
                onClick={() => setSpec(applyPreset(spec, p))}
                title={ok ? p.label : DRAWN.has(p.mark) ? `${p.label}: needs other fields` : `${p.label}: not drawn in Studio yet`}
                data-testid={`showme-${p.id}`}
              >
                <i aria-hidden="true">{ICONS[p.id] ?? '▮'}</i>
                {p.label}
              </button>
            );
          })}
        </div>
        <p className="small muted" style={{ marginBlockStart: 10 }}>
          For <b>{c.dates} date(s)</b>, <b>{c.dims - c.dates} other dimension(s)</b> and <b>{c.measures} measure(s)</b>: {pick.why || 'a table'}. Greyed-out types need other fields.
        </p>
      </aside>
    </div>
  );
}
