import { Fingerprints } from './Fingerprints';
import { useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import type { Privileges } from '../api/types';
import {
  ACTION_LABEL,
  actionsFor,
  columnsText,
  newestFirst,
  pendingFor,
  tableOf,
  usText,
  type IndexAction,
  type IndexDetail,
  type IndexListing,
  type IndexRecord,
  type IndexRequest,
} from './records';

export function mayIndex(p: Privileges | undefined, db: string): boolean {
  if (!p) return false;
  return p.index_all || p.index_dbs.some((d) => d.toLowerCase() === db.toLowerCase());
}

function q(params: Record<string, string>): string {
  const s = new URLSearchParams(Object.entries(params).filter(([, v]) => v)).toString();
  return s ? `?${s}` : '';
}

const fmt = (n: number) => (Number.isFinite(n) ? n.toFixed(1) : '');

function Pending({ r }: { r: IndexRequest | undefined }) {
  if (!r) return null;
  return (
    <span className="badge" title={`${r.actor}${r.reason ? `: ${r.reason}` : ''}`}>
      {r.action} requested
    </span>
  );
}

/** Automatic indexing: the tenant's automatic indexes with their state, the
 * loop's recent decisions, recommendations (approved or dismissed in the
 * approvals inbox) and the query patterns behind them. Pinning, unpinning,
 * dropping and restoring need the privilege to create indexes in the
 * index's database; the node running the tenant's loop applies a request
 * within a minute. */
export function IndexingPage() {
  const me = useSession((s) => s.me);
  const { tenant = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const view = params.get('view') ?? 'indexes';
  const db = params.get('db') ?? '';
  const table = params.get('table') ?? '';
  const [showAll, setShowAll] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ['indexing', view, db, table, showAll],
    enabled: view !== 'fingerprints',
    queryFn: () =>
      json<IndexListing>(
        view === 'recommendations'
          ? `/v1/indexing/recommendations${q({ db, table })}`
          : `/v1/indexing/indexes${q({ db, table, all: showAll || view === 'events' ? 'true' : '' })}`,
      ),
  });
  const detail = useQuery({
    queryKey: ['indexing', 'detail', selected],
    enabled: !!selected,
    queryFn: () => json<IndexDetail>(`/v1/indexing/indexes/${encodeURIComponent(selected ?? '')}`),
  });
  const act = async (ix: IndexRecord, action: IndexAction) => {
    const reason = window.prompt(`Why ${action} ${ix.name}? (recorded with the decision)`, '');
    if (reason === null) return;
    setMsg(null);
    try {
      await json<IndexRequest>(`/v1/indexing/indexes/${encodeURIComponent(ix.name)}:${action}`, { body: { reason } });
      setMsg({ ok: true, text: `${ix.name}: ${action} requested. The node running the tenant's loop applies it within a minute.` });
      await qc.invalidateQueries({ queryKey: ['indexing'] });
    } catch (e) {
      setMsg({ ok: false, text: errorText(e) });
    }
  };
  const tab = (v: string, label: string) => (
    <button role="tab" aria-selected={view === v} onClick={() => setParams({ view: v, ...(db ? { db } : {}), ...(table ? { table } : {}) })}>
      {label}
    </button>
  );
  const inbox = `/t/${encodeURIComponent(tenant)}/inbox`;
  const requests = list.data?.requests ?? [];
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Automatic indexing</h1>
      <p className="muted">
        Statements run from Studio are not meant to cause indexes (<code>auto_index.include_studio</code>).
      </p>
      <div className="tabs" role="tablist">
        {tab('indexes', 'Indexes')}
        {tab('recommendations', 'Recommendations')}
        {tab('events', 'Events')}
        {tab('fingerprints', 'Fingerprints')}
      </div>
      {(db || table) && (
        <p>
          Filtered to {db}
          {table ? `.${table}` : ''} <button onClick={() => setParams({ view })}>Show all</button>
        </p>
      )}
      {msg && (
        <p className={msg.ok ? 'banner' : 'error'} role={msg.ok ? 'status' : 'alert'}>
          {msg.text}
        </p>
      )}
      {view === 'fingerprints' ? (
        <Fingerprints />
      ) : (
        <div className="stack">
          {view === 'indexes' && (
            <label className="row">
              <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} /> Include dropped, rejected and
              failed (history)
            </label>
          )}
          {view === 'recommendations' && (
            <p className="muted">
              With <code>auto_index.mode = recommend</code>, each index waits here until it is approved or dismissed in the{' '}
              <Link to={inbox}>approvals inbox</Link>.
            </p>
          )}
          {list.isLoading && <p className="muted">Loading…</p>}
          {list.error && <p className="error">{errorText(list.error)}</p>}
          {view === 'events' && list.data && (
            <>
              {list.data.events.length === 0 && <p className="muted">No decisions in the last 30 days.</p>}
              {list.data.events.length > 0 && (
                <table data-testid="index-events">
                  <thead>
                    <tr>
                      <th scope="col">When</th>
                      <th scope="col">Decision</th>
                      <th scope="col">Index</th>
                      <th scope="col">Table</th>
                      <th scope="col">Definition</th>
                      <th scope="col">Why</th>
                    </tr>
                  </thead>
                  <tbody>
                    {newestFirst(list.data.events).map((e, i) => (
                      <tr key={`${e.at_us}-${i}`}>
                        <td>{usText(e.at_us)}</td>
                        <td>
                          <span className="badge">{e.kind}</span>
                        </td>
                        <td className="mono">{e.index}</td>
                        <td>{e.table}</td>
                        <td className="mono">{e.def}</td>
                        <td>{e.reason}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}
          {view !== 'events' && list.data && list.data.indexes.length === 0 && <p className="muted">None.</p>}
          {view !== 'events' && list.data && list.data.indexes.length > 0 && (
            <table data-testid="indexes-table">
              <thead>
                <tr>
                  <th scope="col">Index</th>
                  <th scope="col">Table</th>
                  <th scope="col">Columns</th>
                  <th scope="col">State</th>
                  <th scope="col">Saves ms/h</th>
                  <th scope="col">Reads</th>
                  <th scope="col">Actions</th>
                </tr>
              </thead>
              <tbody>
                {list.data.indexes.map((ix) => {
                  const can = mayIndex(me?.privileges, ix.spec.db);
                  const pending = pendingFor(requests, ix.name);
                  return (
                    <tr key={ix.name}>
                      <td>
                        <button onClick={() => setSelected(ix.name)} className="mono">
                          {ix.name}
                        </button>
                      </td>
                      <td>{tableOf(ix)}</td>
                      <td className="mono">{columnsText(ix)}</td>
                      <td className="row">
                        <span className="badge">{ix.state}</span>
                        {ix.pinned && <span className="badge">pinned</span>}
                        <Pending r={pending} />
                      </td>
                      <td>{fmt(ix.benefit_ms_per_hour)}</td>
                      <td>{ix.reads_total ?? 0}</td>
                      <td className="row">
                        {ix.state === 'recommended' && ix.approval && (
                          <Link to={inbox} className="small">
                            Approve or dismiss in the inbox
                          </Link>
                        )}
                        {can &&
                          !pending &&
                          actionsFor(ix).map((a) => (
                            <button key={a} onClick={() => void act(ix, a)}>
                              {ACTION_LABEL[a]}
                            </button>
                          ))}
                        {!can && <span className="muted">view only</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      )}
      {selected && (
        <section className="card stack" aria-label="Evidence">
          <div className="row">
            <h2 style={{ margin: 0, flex: 1 }}>Evidence: {detail.data?.index.name ?? selected}</h2>
            <button onClick={() => setSelected(null)}>Close</button>
          </div>
          {detail.error && <p className="error">{errorText(detail.error)}</p>}
          {detail.data && (
            <>
              <p>
                <span className="mono">
                  {tableOf(detail.data.index)} ({columnsText(detail.data.index)})
                </span>
                , {detail.data.index.state}
                {detail.data.index.pinned ? ', pinned' : ''}. Decided {usText(detail.data.index.created_us)} because of{' '}
                <code>{detail.data.index.reason}</code>
                {detail.data.index.end_reason ? `; ended: ${detail.data.index.end_reason}` : ''}.
              </p>
              <p>
                Rows examined without it {fmt(detail.data.index.est_before)}, with it {fmt(detail.data.index.est_after)} (planner
                estimates over the replayed statements); saves about {fmt(detail.data.index.benefit_ms_per_hour)} ms an hour; read by{' '}
                {detail.data.index.reads_total ?? 0} plans
                {detail.data.index.last_used_us ? `, last ${usText(detail.data.index.last_used_us)}` : ''}.
              </p>
              {detail.data.index.build && (
                <p>
                  Built {usText(detail.data.index.built_us)}: {detail.data.index.build.rows} rows in {detail.data.index.build.ms} ms (
                  {detail.data.index.build.batches} batches).
                </p>
              )}
              {detail.data.requests.length > 0 && (
                <p>
                  Pending: {detail.data.requests.map((r) => `${r.action} by ${r.actor} (${usText(r.at_us)})`).join('; ')}
                </p>
              )}
              <h3>Decisions</h3>
              {detail.data.events.length === 0 && <p className="muted">None in the last 30 days.</p>}
              <ol>
                {newestFirst(detail.data.events).map((e, i) => (
                  <li key={i}>
                    {usText(e.at_us)} {e.kind} {e.reason}
                  </li>
                ))}
              </ol>
            </>
          )}
        </section>
      )}
    </div>
  );
}
