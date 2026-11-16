// Dashboards: the side panel (Mine, Shared, Digest), a dashboard's
// 12-column grid of tiles with edit mode (drag, resize, remove), the tile
// menu, dashboard-wide filters, sharing, and the ask bar that has the data
// assistant propose a tile to preview and accept.

import { useEffect, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent as RPointerEvent } from 'react';
import { useNavigate, useParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import { addTab } from '../editor/tabs';
import { useRel } from '../workspace/WorkspaceTabs';
import { openDoc } from '../workspace/store';
import { Mascot } from '../app/Mascot';
import { chat } from '../ai/api';
import type { Proposal } from '../ai/thread';
import {
  KEYS,
  acceptTile,
  addTile,
  deleteDashboard,
  deleteTile,
  dismissProposal,
  paramsOf,
  runQuery,
  runTile,
  shareDashboard,
  updateDashboard,
  useDashboard,
  useDashboards,
  useStoreDashboard,
  type DashFilter,
  type Dashboard,
  type FilterKind,
  type Pos,
  type Tile,
} from './api';
import { ChartView } from './ChartView';
import { normalize, type ChartSpec } from './spec';
import { clampPos, moves, settle } from './layout';
import './dashboards.css';

const ROW_H = 64;
const GAP = 10;

function ago(ms: number): string {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return 'just now';
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  return `${Math.round(s / 3600)} h ago`;
}

function every(sec: number): string {
  if (!sec) return 'refreshed when opened';
  if (sec < 3600) return `every ${Math.round(sec / 60)} min`;
  return `every ${Math.round(sec / 3600)} h`;
}

// ---- one tile ----

function TileBody({ dash, tile, params }: { dash: Dashboard; tile: Tile; params: Record<string, unknown> }) {
  const q = useQuery({
    queryKey: ['tile-run', dash.id, tile.id, tile.sql, JSON.stringify(tile.chart), JSON.stringify(params)],
    queryFn: ({ signal }) => runTile(dash.id, tile.id, params, signal),
    refetchInterval: tile.refresh_seconds ? tile.refresh_seconds * 1000 : false,
    staleTime: tile.refresh_seconds ? tile.refresh_seconds * 1000 : 60_000,
    retry: false,
  });
  const spec = useMemo(() => normalize(tile.chart, q.data?.columns ?? []), [tile.chart, q.data?.columns]);
  if (q.isPending) return <p className="muted chart-note">Running…</p>;
  if (q.error) return <p className="error chart-note">{errorText(q.error)}</p>;
  return <ChartView spec={spec} columns={q.data.columns} rows={q.data.rows} title={tile.title} />;
}

function TileFooter({ dash, tile, params }: { dash: Dashboard; tile: Tile; params: Record<string, unknown> }) {
  const qc = useQueryClient();
  const state = qc.getQueryState(['tile-run', dash.id, tile.id, tile.sql, JSON.stringify(tile.chart), JSON.stringify(params)]);
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((n) => n + 1), 30_000);
    return () => clearInterval(t);
  }, []);
  const at = state?.dataUpdatedAt;
  return (
    <div className="tf">
      {at ? `updated ${ago(at)}` : ' '} · {every(tile.refresh_seconds)}
      {tile.database ? ` · ${tile.database}` : ''}
    </div>
  );
}

function TileCard({
  dash,
  tile,
  pos,
  editing,
  params,
  onMenu,
  menuOpen,
  onGrip,
  onResize,
  onKey,
  dragging,
}: {
  dash: Dashboard;
  tile: Tile;
  pos: Pos;
  editing: boolean;
  params: Record<string, unknown>;
  onMenu: (open: boolean) => void;
  menuOpen: boolean;
  onGrip: (e: RPointerEvent) => void;
  onResize: (e: RPointerEvent) => void;
  onKey: (e: KeyboardEvent) => void;
  dragging: boolean;
}) {
  const { base } = useRel();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const store = useStoreDashboard();
  const [error, setError] = useState<string | null>(null);
  const act = async (what: 'edit' | 'chart' | 'duplicate' | 'query' | 'remove' | 'refresh') => {
    onMenu(false);
    setError(null);
    try {
      if (what === 'edit') void navigate(`${base}/d/${dash.id}/tile/${tile.id}`);
      if (what === 'chart') void navigate(`${base}/d/${dash.id}/tile/${tile.id}?focus=showme`);
      if (what === 'refresh') await qc.invalidateQueries({ queryKey: ['tile-run', dash.id, tile.id] });
      if (what === 'query') {
        const t = addTab({ title: tile.title.slice(0, 40), sql: tile.sql, db: tile.database });
        void navigate(`${base}/q/${t.id}`);
      }
      if (what === 'duplicate') {
        const r = await addTile(dash.id, { title: `${tile.title} (copy)`, database: tile.database, sql: tile.sql, chart: tile.chart, refresh_seconds: tile.refresh_seconds });
        store(r.dashboard);
      }
      if (what === 'remove') {
        if (!window.confirm(`Remove the tile "${tile.title}"?`)) return;
        await deleteTile(dash.id, tile.id);
        store({ ...dash, tiles: dash.tiles.filter((t) => t.id !== tile.id) });
      }
    } catch (e) {
      setError(errorText(e));
    }
  };
  return (
    <section
      className={`tile${dragging ? ' dragging' : ''}`}
      style={{ gridColumn: `${pos.x + 1} / span ${pos.w}`, gridRow: `${pos.y + 1} / span ${pos.h}` }}
      aria-label={tile.title}
      data-testid={`tile-${tile.title}`}
    >
      <div className="th">
        {editing && (
          <button
            className="grip"
            aria-label={`Move ${tile.title} (arrow keys move, Shift and arrow keys resize)`}
            title="Drag to move; arrow keys move, Shift+arrows resize"
            onPointerDown={onGrip}
            onKeyDown={onKey}
            data-testid="tile-grip"
          >
            ⠿
          </button>
        )}
        <span className="title" title={tile.title}>
          {tile.title}
        </span>
        <button aria-label={`${tile.title}: tile menu`} aria-haspopup="menu" aria-expanded={menuOpen} onClick={() => onMenu(!menuOpen)} data-testid="tile-menu">
          ⋯
        </button>
        {editing && dash.can_edit && (
          <button aria-label={`Remove ${tile.title}`} title="Remove" onClick={() => void act('remove')}>
            ✕
          </button>
        )}
      </div>
      {menuOpen && (
        <div className="tile-menu" role="menu" onKeyDown={(e) => e.key === 'Escape' && onMenu(false)}>
          {dash.can_edit && (
            <>
              <button role="menuitem" onClick={() => void act('edit')}>
                Edit query and chart
              </button>
              <button role="menuitem" onClick={() => void act('chart')}>
                Change chart
              </button>
              <button role="menuitem" onClick={() => void act('duplicate')}>
                Duplicate
              </button>
            </>
          )}
          <button role="menuitem" onClick={() => void act('query')}>
            Open in a query tab
          </button>
          <button role="menuitem" onClick={() => void act('refresh')}>
            Run again
          </button>
          {dash.can_edit && (
            <button role="menuitem" className="danger" onClick={() => void act('remove')}>
              Remove
            </button>
          )}
        </div>
      )}
      <div className="tb">
        <TileBody dash={dash} tile={tile} params={params} />
      </div>
      {error && <p className="error chart-note">{error}</p>}
      <TileFooter dash={dash} tile={tile} params={params} />
      {editing && <button className="resize" aria-label={`Resize ${tile.title}`} onPointerDown={onResize} tabIndex={-1} />}
    </section>
  );
}

// ---- filters ----

function FilterInput({ f, value, onChange }: { f: DashFilter; value: unknown; onChange: (v: unknown) => void }) {
  const v = value === undefined ? f.value : value;
  if (f.kind === 'list') {
    return (
      <input
        aria-label={f.label}
        value={Array.isArray(v) ? v.join(', ') : ''}
        placeholder="a, b, c"
        onChange={(e) =>
          onChange(
            e.target.value
              .split(',')
              .map((x) => x.trim())
              .filter(Boolean),
          )
        }
      />
    );
  }
  return (
    <input
      aria-label={f.label}
      type={f.kind === 'date' ? 'date' : f.kind === 'number' ? 'number' : 'text'}
      value={v === null || v === undefined ? '' : (typeof v === 'string' ? v : JSON.stringify(v)).slice(0, f.kind === 'date' ? 10 : undefined)}
      onChange={(e) => onChange(e.target.value === '' ? null : f.kind === 'number' ? Number(e.target.value) : e.target.value)}
    />
  );
}

function FiltersBar({
  dash,
  local,
  setLocal,
  editing,
}: {
  dash: Dashboard;
  local: Record<string, unknown>;
  setLocal: (v: Record<string, unknown>) => void;
  editing: boolean;
}) {
  const store = useStoreDashboard();
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState<{ name: string; label: string; kind: FilterKind } | null>(null);
  const save = async (filters: DashFilter[]) => {
    setError(null);
    try {
      store(await updateDashboard(dash.id, { filters }));
      setLocal({});
    } catch (e) {
      setError(errorText(e));
    }
  };
  if (!dash.filters.length && !editing) return null;
  const changed = Object.keys(local).length > 0;
  return (
    <div className="dash-filters" role="group" aria-label="Dashboard filters">
      {dash.filters.map((f) => (
        <label key={f.name}>
          <span>
            {f.label} <code className="muted">{`{{${f.name}}}`}</code>
          </span>
          <span className="row">
            <FilterInput f={f} value={local[f.name]} onChange={(v) => setLocal({ ...local, [f.name]: v })} />
            {editing && dash.can_edit && (
              <button aria-label={`Remove filter ${f.label}`} onClick={() => void save(dash.filters.filter((x) => x.name !== f.name))}>
                ✕
              </button>
            )}
          </span>
        </label>
      ))}
      {changed && (
        <button onClick={() => setLocal({})} title="Back to the saved values">
          Reset
        </button>
      )}
      {changed && dash.can_edit && (
        <button onClick={() => void save(dash.filters.map((f) => (f.name in local ? { ...f, value: local[f.name] } : f)))}>Save as default</button>
      )}
      {editing && dash.can_edit && !adding && (
        <button onClick={() => setAdding({ name: '', label: '', kind: 'date' })} data-testid="add-filter">
          ＋ Filter
        </button>
      )}
      {adding && (
        <span className="row" role="group" aria-label="New filter">
          <input aria-label="Filter name in queries" placeholder="from" value={adding.name} onChange={(e) => setAdding({ ...adding, name: e.target.value })} />
          <input aria-label="Filter label" placeholder="From" value={adding.label} onChange={(e) => setAdding({ ...adding, label: e.target.value })} />
          <select aria-label="Filter kind" value={adding.kind} onChange={(e) => setAdding({ ...adding, kind: e.target.value as FilterKind })}>
            <option value="date">date</option>
            <option value="text">text</option>
            <option value="number">number</option>
            <option value="list">list</option>
          </select>
          <button
            className="primary"
            onClick={() => {
              const f = { name: adding.name.trim(), label: adding.label.trim() || adding.name.trim(), kind: adding.kind, value: null };
              setAdding(null);
              void save([...dash.filters, f]);
            }}
          >
            Add
          </button>
          <button onClick={() => setAdding(null)}>Cancel</button>
        </span>
      )}
      {error && <span className="error">{error}</span>}
    </div>
  );
}

// ---- the ask bar ----

interface Preview {
  proposal: Proposal;
  title: string;
  sql: string;
  db: string | null;
  chart: ChartSpec;
  question: string;
}

function chartOf(v: unknown): ChartSpec {
  if (typeof v === 'string') {
    try {
      return JSON.parse(v) as ChartSpec;
    } catch {
      return { mark: 'text-table' };
    }
  }
  return (v ?? { mark: 'text-table' }) as ChartSpec;
}

function PreviewTile({ dash, p, onDone, params }: { dash: Dashboard; p: Preview; onDone: () => void; params: Record<string, unknown> }) {
  const { base } = useRel();
  const navigate = useNavigate();
  const store = useStoreDashboard();
  const qc = useQueryClient();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const run = useQuery({
    queryKey: ['tile-preview', p.proposal.id],
    queryFn: ({ signal }) => runQuery(p.db, p.sql, dash.filters, params, signal),
    retry: false,
  });
  const spec = useMemo(() => normalize(p.chart, run.data?.columns ?? []), [p.chart, run.data?.columns]);
  const accept = async () => {
    setBusy(true);
    setError(null);
    try {
      await acceptTile(p.proposal.id, { dashboard: dash.id });
      await qc.invalidateQueries({ queryKey: KEYS.one(dash.id) });
      void qc.invalidateQueries({ queryKey: KEYS.list });
      onDone();
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const discard = async () => {
    try {
      await dismissProposal(p.proposal.id);
    } catch {
      // The preview goes either way.
    }
    onDone();
  };
  const next = dash.tiles.reduce((m, t) => Math.max(m, t.pos.y + t.pos.h), 0);
  return (
    <section className="tile preview" style={{ gridColumn: '1 / span 8', gridRow: `${next + 1} / span 4` }} aria-label={`Preview: ${p.title}`} data-testid="tile-preview">
      <div className="th">
        <span className="title">✦ Preview: {p.title}</span>
        <button className="primary" onClick={() => void accept()} disabled={busy || !dash.can_edit} data-testid="preview-accept">
          Accept
        </button>
        <button
          onClick={() => {
            store(dash);
            void navigate(`${base}/d/${dash.id}/tile/new`, { state: { draft: { title: p.title, database: p.db, sql: p.sql, chart: p.chart } } });
          }}
        >
          Edit query
        </button>
        <button onClick={() => void discard()}>Discard</button>
      </div>
      <div className="tb">
        {run.isPending ? <p className="muted chart-note">Running…</p> : run.error ? <p className="error chart-note">{errorText(run.error)}</p> : <ChartView spec={spec} columns={run.data.columns} rows={run.data.rows} title={p.title} />}
      </div>
      <div className="tf">
        From your request "{p.question}"{dash.filters.length ? " · uses the dashboard's filters" : ''}
      </div>
      {error && <p className="error chart-note">{error}</p>}
    </section>
  );
}

function AskBar({ dash, onPreview }: { dash: Dashboard; onPreview: (p: Preview) => void }) {
  const me = useSession((s) => s.me);
  const [text, setText] = useState('');
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const enabled = !!me?.ai?.enabled && (me.ai.roles ?? []).includes('data');
  const ask = async () => {
    const q = text.trim();
    if (!q) return;
    setBusy(true);
    setStatus('Asking the data assistant…');
    let found: Proposal | null = null;
    let said = '';
    const filters = dash.filters.map((f) => `{{${f.name}}} (${f.kind}, "${f.label}")`).join(', ');
    const dbs = [...new Set(dash.tiles.map((t) => t.database).filter(Boolean))];
    try {
      await chat(
        {
          role: 'data',
          message: `Propose one dashboard tile with propose_tile for the dashboard "${dash.name}": ${q}`,
          ...(dbs[0] ? { database: dbs[0] } : {}),
          context: [
            {
              kind: 'note',
              ref: `Dashboard "${dash.name}" (id ${dash.id}).${filters ? ` Its filters, usable in the tile's query as bare placeholders: ${filters}.` : ''}`,
            },
          ],
        },
        (e) => {
          if (e.type === 'proposal' && e.proposal.kind === 'tile' && !found) found = e.proposal;
          if (e.type === 'text') said += e.text;
          if (e.type === 'error') setStatus(e.error.message ?? 'The assistant stopped with an error.');
        },
      );
      const p = found as Proposal | null;
      if (p) {
        const b = p.body;
        onPreview({
          proposal: p,
          title: typeof b.title === 'string' ? b.title : q,
          sql: typeof b.sql === 'string' ? b.sql : '',
          db: typeof b.db === 'string' ? b.db : null,
          chart: chartOf(b.chart),
          question: q,
        });
        setStatus(null);
        setText('');
      } else {
        setStatus(said.trim() ? said.trim().slice(0, 400) : 'The assistant didn’t propose a tile. Try saying which table or measure.');
      }
    } catch (e) {
      setStatus(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  if (!dash.can_edit) return null;
  return (
    <>
      <form
        className="askbar"
        onSubmit={(e) => {
          e.preventDefault();
          void ask();
        }}
      >
        <Mascot pose="avatar" size={20} />
        <input
          aria-label="Ask to add a tile"
          placeholder={enabled ? 'Ask to add a tile: "average order value by region"' : 'The data assistant is off for this tenant'}
          value={text}
          disabled={!enabled || busy}
          onChange={(e) => setText(e.target.value)}
          data-testid="ask-tile"
        />
        <button className="primary" type="submit" disabled={!enabled || busy || !text.trim()}>
          Ask
        </button>
      </form>
      {status && (
        <p className="muted" role="status" style={{ margin: '0 8px 8px' }}>
          {status}
        </p>
      )}
    </>
  );
}

// ---- the page ----

export function DashboardsPage() {
  const { id } = useParams();
  const { base } = useRel();
  const navigate = useNavigate();
  const list = useDashboards();
  // Without an id: the first dashboard, else the empty state.
  useEffect(() => {
    if (!id && list.data) {
      const first = list.data.mine[0] ?? list.data.shared[0];
      if (first) void navigate(`${base}/d/${first.id}`, { replace: true });
    }
  }, [id, list.data, base, navigate]);
  if (!id) {
    return (
      <div className="dash-empty">
        <Mascot pose="avatar" size={48} />
        <h2>Dashboards</h2>
        <p className="muted">Charts over your queries, for you or shared with the tenant. Make one with “New dashboard” in the side panel, or “Add to dashboard” under any query result.</p>
      </div>
    );
  }
  return <DashboardView id={id} key={id} />;
}

function DashboardView({ id }: { id: string }) {
  const q = useDashboard(id);
  const { base } = useRel();
  const navigate = useNavigate();
  const store = useStoreDashboard();
  const qc = useQueryClient();
  const [editing, setEditing] = useState(false);
  const [local, setLocal] = useState<Record<string, unknown>>({});
  const [menu, setMenu] = useState<string | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [drag, setDrag] = useState<{ id: string; pos: Pos } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const grid = useRef<HTMLDivElement>(null);
  const d = q.data;
  const params = useMemo(() => (d ? paramsOf(d.filters, local) : {}), [d, local]);
  // The tab shows the dashboard's name.
  useEffect(() => {
    if (d) openDoc({ rel: `d/${d.id}`, kind: 'dashboard', title: d.name });
  }, [d]);
  if (q.isPending) return <p className="muted">Loading…</p>;
  if (q.error || !d) return <p className="error">{errorText(q.error)}</p>;

  const placed = d.tiles.map((t) => (drag && drag.id === t.id ? { ...t, pos: drag.pos } : t));
  // The newest layout: keys can arrive faster than renders.
  const current = () => qc.getQueryData<Dashboard>(KEYS.one(d.id)) ?? d;
  const commit = async (tileId: string, pos: Pos) => {
    const now = current();
    const after = settle(
      now.tiles.map((t) => (t.id === tileId ? { ...t, pos } : t)),
      tileId,
    );
    const changed = moves(now.tiles, after);
    store({ ...now, tiles: after });
    if (!changed.length) return;
    try {
      store(await updateDashboard(d.id, { positions: changed }));
    } catch (e) {
      setError(errorText(e));
      void qc.invalidateQueries({ queryKey: KEYS.one(d.id) });
    }
  };
  const cell = () => {
    const w = grid.current?.clientWidth ?? 1200;
    return { cw: (w - GAP * 11) / 12 + GAP, rh: ROW_H + GAP };
  };
  const pointer = (t: Tile, mode: 'move' | 'resize') => (e: RPointerEvent) => {
    if (!d.can_edit) return;
    e.preventDefault();
    const sx = e.clientX;
    const sy = e.clientY;
    const start = t.pos;
    const { cw, rh } = cell();
    let last = start;
    const move = (ev: PointerEvent) => {
      const dx = Math.round((ev.clientX - sx) / cw);
      const dy = Math.round((ev.clientY - sy) / rh);
      last = clampPos(mode === 'move' ? { ...start, x: start.x + dx, y: start.y + dy } : { ...start, w: start.w + dx, h: start.h + dy });
      setDrag({ id: t.id, pos: last });
    };
    const up = () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      setDrag(null);
      void commit(t.id, last);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };
  const key = (t: Tile) => (e: KeyboardEvent) => {
    const dirs: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    const step = dirs[e.key];
    if (!step) return;
    e.preventDefault();
    const p = current().tiles.find((x) => x.id === t.id)?.pos ?? t.pos;
    const next = e.shiftKey ? { ...p, w: p.w + step[0], h: p.h + step[1] } : { ...p, x: p.x + step[0], y: p.y + step[1] };
    void commit(t.id, clampPos(next));
  };
  const share = async (shared: boolean) => {
    setError(null);
    try {
      store(await shareDashboard(d.id, shared));
    } catch (e) {
      setError(errorText(e));
    }
  };
  const rename = async () => {
    const name = window.prompt('Dashboard name', d.name);
    if (!name?.trim() || name.trim() === d.name) return;
    try {
      store(await updateDashboard(d.id, { name: name.trim() }));
    } catch (e) {
      setError(errorText(e));
    }
  };
  const remove = async () => {
    if (!window.confirm(`Delete the dashboard "${d.name}" and its tiles?`)) return;
    try {
      await deleteDashboard(d.id);
      qc.removeQueries({ queryKey: KEYS.one(d.id) });
      await qc.invalidateQueries({ queryKey: KEYS.list });
      void navigate(`${base}/d`);
    } catch (e) {
      setError(errorText(e));
    }
  };
  const rows = placed.reduce((m, t) => Math.max(m, t.pos.y + t.pos.h), 0) + (preview ? 4 : 0);
  return (
    <div className="dash" onClick={(e) => menu && !(e.target as HTMLElement).closest('.tile-menu, [data-testid="tile-menu"]') && setMenu(null)}>
      <div className="dash-h">
        <h2>{d.name}</h2>
        <span className="dash-badge" data-testid="share-badge">
          {d.shared ? 'Tenant' : 'Only me'}
        </span>
        {!d.mine && <span className="muted small">by {d.owner}</span>}
        <span className="sp" />
        {d.can_edit && (
          <button className={editing ? 'primary' : undefined} onClick={() => setEditing(!editing)} aria-pressed={editing} data-testid="edit-dashboard">
            {editing ? 'Done editing' : 'Edit'}
          </button>
        )}
        {d.can_edit && (
          <button onClick={() => void navigate(`${base}/d/${d.id}/tile/new`)} data-testid="add-tile">
            ＋ Tile
          </button>
        )}
        {d.mine && (
          <label className="row">
            <span className="muted small">Share</span>
            <select aria-label="Who sees this dashboard" value={d.shared ? 'tenant' : 'me'} onChange={(e) => void share(e.target.value === 'tenant')} data-testid="share-select">
              <option value="me">Only me</option>
              <option value="tenant">Tenant</option>
            </select>
          </label>
        )}
        {d.can_edit && (
          <>
            <button onClick={() => void rename()}>Rename</button>
            <button className="danger" onClick={() => void remove()}>
              Delete
            </button>
          </>
        )}
      </div>
      <FiltersBar dash={d} local={local} setLocal={setLocal} editing={editing} />
      {error && <p className="error">{error}</p>}
      {!d.tiles.length && !preview && (
        <div className="dash-empty muted">
          <p>No tiles yet. Add one with “＋ Tile”, from a query result’s “Add to dashboard”, or ask below.</p>
        </div>
      )}
      <div className={`dgrid${editing ? ' editing' : ''}`} ref={grid} style={{ gridTemplateRows: `repeat(${Math.max(rows, 1)}, ${ROW_H}px)` }} data-testid="dash-grid">
        {placed.map((t) => (
          <TileCard
            key={t.id}
            dash={d}
            tile={t}
            pos={t.pos}
            editing={editing}
            params={params}
            menuOpen={menu === t.id}
            onMenu={(o) => setMenu(o ? t.id : null)}
            onGrip={pointer(t, 'move')}
            onResize={pointer(t, 'resize')}
            onKey={key(t)}
            dragging={drag?.id === t.id}
          />
        ))}
        {preview && <PreviewTile dash={d} p={preview} params={params} onDone={() => setPreview(null)} />}
      </div>
      <AskBar dash={d} onPreview={setPreview} />
    </div>
  );
}
