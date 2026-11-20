// The home pages of the five tenant workspaces. Each is built only from
// what Studio already fetches for its other pages (data.ts); what a
// workspace's home should show but no route provides yet is left out
// rather than faked.

import { useState, type ReactNode } from 'react';
import { Link, useParams } from 'react-router';
import { errorText, isApiError } from '../api/errors';
import { useSession } from '../auth/session';
import { recommendationTarget, useOpenApprovals } from '../approvals/InboxPage';
import { useLibrary } from '../workspace/library';
import { ago, firstLine, useOpen } from '../workspace/LibraryPanels';
import { useWorkspace } from '../workspace/store';
import {
  loadSummary,
  modelCounts,
  searchHome,
  useFailingLogic,
  useFingerprints,
  useIndexAdvice,
  useSnapshots,
  useTenantSettings,
  useUsers,
} from './data';

const fmt = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 1 });

function useBase(): string {
  const { tenant = '' } = useParams();
  return `/t/${encodeURIComponent(tenant)}`;
}

function Card({ id, title, more, children }: { id: string; title: string; more?: { to: string; label: string }; children: ReactNode }) {
  return (
    <section className="card stack" aria-labelledby={`home-${id}`} data-testid={`home-${id}`}>
      <div className="row">
        <h2 id={`home-${id}`} style={{ flex: 1, margin: 0 }}>
          {title}
        </h2>
        {more && (
          <Link to={more.to} className="small">
            {more.label}
          </Link>
        )}
      </div>
      {children}
    </section>
  );
}

/** Loading, refused or failed; null once there is data. */
function QueryNote({ q }: { q: { isLoading: boolean; error: unknown } }) {
  if (q.isLoading) return <p className="muted">Loading…</p>;
  if (!q.error) return null;
  if (isApiError(q.error) && (q.error.status === 403 || q.error.status === 401)) {
    return <p className="muted">Not available to you.</p>;
  }
  return <p className="error">{errorText(q.error)}</p>;
}

function RecentQueries({ n = 8 }: { n?: number }) {
  const history = useLibrary((s) => s.history);
  const open = useOpen();
  return (
    <Card id="recent" title="Recent queries">
      {history.length === 0 && <p className="muted">Queries you run show here.</p>}
      <ul className="linklist">
        {history.slice(0, n).map((h) => (
          <li key={h.id}>
            <button className="link" onClick={() => open({ sql: h.sql, db: h.db, language: h.language })} title={h.sql}>
              <span className="mono">{firstLine(h.sql)}</span>
            </button>{' '}
            <span className="muted small">
              {h.db ? `${h.db} · ` : ''}
              {ago(h.at)}
              {h.ok ? '' : ' · failed'}
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

// ---- Analyst ----

export function AnalystHome() {
  const base = useBase();
  const { dbs, snaps, loading } = useSnapshots();
  const saved = useLibrary((s) => s.saved);
  const docs = useWorkspace((s) => s.docs);
  const open = useOpen();
  const [q, setQ] = useState('');
  const found = searchHome(q, dbs, snaps, saved);
  const tables = docs.filter((d) => d.kind === 'table' || d.kind === 'view' || d.kind === 'graph');
  return (
    <div className="home-grid">
      <Card id="search" title="Find a table or a saved query">
        <input
          type="search"
          aria-label="Search tables and saved queries"
          placeholder="Table, view or saved query…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        {q && loading && <p className="muted">Loading…</p>}
        {q && !loading && found.length === 0 && <p className="muted">Nothing matches.</p>}
        <ul className="linklist" data-testid="home-found">
          {found.map((f) =>
            f.kind === 'object' ? (
              <li key={`o:${f.rel}`}>
                <Link to={`${base}/${f.rel}`}>{f.name}</Link> <span className="muted small">{f.hint}</span>
              </li>
            ) : (
              <li key={`s:${f.id}`}>
                <button
                  className="link"
                  onClick={() => {
                    const s = saved.find((x) => x.id === f.id);
                    if (s) open({ sql: s.sql, db: s.db, language: s.language, name: s.name });
                  }}
                >
                  {f.name}
                </button>{' '}
                <span className="muted small">saved query{f.hint ? ` · ${f.hint}` : ''}</span>
              </li>
            ),
          )}
        </ul>
      </Card>
      <RecentQueries />
      <Card id="saved" title="Saved queries">
        {saved.length === 0 && <p className="muted">Save a query with Ctrl+S to keep it here.</p>}
        <ul className="linklist">
          {saved.slice(0, 8).map((s) => (
            <li key={s.id}>
              <button className="link" onClick={() => open({ sql: s.sql, db: s.db, language: s.language, name: s.name })}>
                {s.name}
              </button>{' '}
              {s.db && <span className="muted small">{s.db}</span>}
            </li>
          ))}
        </ul>
      </Card>
      <Card id="tables" title="Tables you have open">
        {tables.length === 0 && <p className="muted">Tables you open show here.</p>}
        <ul className="linklist">
          {tables.map((d) => (
            <li key={d.rel}>
              <Link to={`${base}/${d.rel}`}>{d.title}</Link> <span className="muted small">{d.kind}</span>
            </li>
          ))}
        </ul>
      </Card>
    </div>
  );
}

// ---- Developer ----

function DatabasesByModel() {
  const base = useBase();
  const { dbs, snaps, loading, error } = useSnapshots();
  const rows = modelCounts(dbs, snaps);
  return (
    <Card id="models" title="Databases">
      <QueryNote q={{ isLoading: loading && dbs.length === 0, error }} />
      {!loading && dbs.length === 0 && !error && <p className="muted">No databases yet.</p>}
      {dbs.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>Database</th>
              <th>Tables</th>
              <th>Views</th>
              <th>Graphs</th>
              <th>Routines</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.db}>
                <td>
                  <Link to={`${base}/db/${encodeURIComponent(r.db)}`}>{r.db}</Link>
                </td>
                <td>{r.tables}</td>
                <td>{r.views}</td>
                <td>{r.graphs}</td>
                <td>{r.routines}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

function FailingLogic() {
  const base = useBase();
  const { dbs } = useSnapshots();
  const { rows, loading, checked } = useFailingLogic(dbs);
  return (
    <Card id="logic" title="Logic with failing runs" more={{ to: `${base}/logic`, label: 'Logic' }}>
      {loading && <p className="muted">Loading…</p>}
      {!loading && rows.length === 0 && (
        <p className="muted">
          {checked === 0 ? 'No triggers or schedules to check.' : `No dead letters in the ${checked} triggers and schedules checked.`}
        </p>
      )}
      <ul className="linklist">
        {rows.map((r) => (
          <li key={`${r.db}/${r.module.kind}/${r.module.name}`}>
            <Link to={`${base}/logic/${encodeURIComponent(r.db)}/${encodeURIComponent(r.module.kind)}/${encodeURIComponent(r.module.name)}`}>
              {r.module.name}
            </Link>{' '}
            <span className="muted small">
              {r.module.kind} · {r.db} · {r.dead} dead letter{r.dead === 1 ? '' : 's'}
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

export function DeveloperHome() {
  return (
    <div className="home-grid">
      <DatabasesByModel />
      <RecentQueries />
      <FailingLogic />
    </div>
  );
}

// ---- Database administrator ----

function Load() {
  const base = useBase();
  const q = useFingerprints();
  const { heaviest, slowest } = loadSummary(q.data?.fingerprints ?? []);
  const line = (r: (typeof heaviest)[number], what: string) => (
    <li key={r.id}>
      <span className="mono" title={r.text}>
        {firstLine(r.text)}
      </span>{' '}
      <span className="muted small">
        {r.db ? `${r.db} · ` : ''}
        {what}
      </span>
    </li>
  );
  return (
    <Card id="load" title="Load and slow statements, last 24 hours" more={{ to: `${base}/indexing?view=fingerprints`, label: 'Fingerprints' }}>
      <QueryNote q={q} />
      {q.data && q.data.fingerprints.length === 0 && <p className="muted">No statements recorded.</p>}
      {heaviest.length > 0 && (
        <>
          <h3>Most total time</h3>
          <ul className="linklist">{heaviest.map((r) => line(r, `${fmt(r.runs)} runs · ${fmt(r.total_ms)} ms in all`))}</ul>
          <h3>Slowest (p95)</h3>
          <ul className="linklist">{slowest.map((r) => line(r, `p95 ${fmt(r.p95_ms)} ms · ${fmt(r.runs)} runs`))}</ul>
        </>
      )}
    </Card>
  );
}

function Approvals({ n = 5 }: { n?: number }) {
  const base = useBase();
  const q = useOpenApprovals();
  const items = q.data?.items ?? [];
  return (
    <Card id="approvals" title="Pending approvals" more={{ to: `${base}/inbox`, label: 'Approvals' }}>
      <QueryNote q={q} />
      {q.data && <p data-testid="home-approvals-count">{q.data.open === 0 ? 'Nothing waits for approval.' : `${q.data.open} waiting.`}</p>}
      <ul className="linklist">
        {items.slice(0, n).map((r) => (
          <li key={r.id}>
            <Link to={`${base}/inbox?id=${encodeURIComponent(r.id)}`}>{r.summary || r.kind}</Link>{' '}
            <span className="muted small">
              {r.kind} · {recommendationTarget(r)}
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

function IndexAdviceCard({ n = 5 }: { n?: number }) {
  const base = useBase();
  const q = useIndexAdvice();
  const rows = [...(q.data?.indexes ?? [])].sort((a, b) => b.est_net_ms_h - a.est_net_ms_h).slice(0, n);
  return (
    <Card id="advice" title="Index advice" more={{ to: `${base}/indexing?view=recommendations`, label: 'Automatic indexing' }}>
      <QueryNote q={q} />
      {q.data && rows.length === 0 && <p className="muted">No indexes recommended.</p>}
      <ul className="linklist">
        {rows.map((r) => (
          <li key={r.index_id}>
            <span className="mono">
              {r.db}.{r.table} ({r.columns.join(', ')})
            </span>{' '}
            <span className="muted small">
              {r.state} · saves about {fmt(r.est_net_ms_h)} ms an hour
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

export function DbAdminHome() {
  return (
    <div className="home-grid">
      <Load />
      <Approvals />
      <IndexAdviceCard />
    </div>
  );
}

// ---- Security administrator ----

function Users() {
  const base = useBase();
  const q = useUsers();
  const users = q.data?.users ?? [];
  const locked = users.filter((u) => u.account_locked);
  const expired = users.filter((u) => u.password_expired);
  return (
    <Card id="users" title="Users" more={{ to: `${base}/access/users`, label: 'Users and access' }}>
      <QueryNote q={q} />
      {q.data && (
        <p data-testid="home-users-count">
          {users.length} account{users.length === 1 ? '' : 's'}
          {locked.length > 0 ? ` · ${locked.length} locked` : ''}
          {expired.length > 0 ? ` · ${expired.length} with an expired password` : ''}
        </p>
      )}
      <ul className="linklist">
        {users.slice(0, 10).map((u) => (
          <li key={`${u.user}@${u.host}`}>
            <span className="mono">
              {u.user}@{u.host}
            </span>{' '}
            <span className="muted small">
              {u.kind}
              {u.account_locked ? ' · locked' : ''}
              {u.password_expired ? ' · password expired' : ''}
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

function Lockouts() {
  const base = useBase();
  const q = useUsers();
  const locked = (q.data?.users ?? []).filter((u) => u.account_locked);
  return (
    <Card id="lockouts" title="Locked accounts" more={{ to: `${base}/access/users`, label: 'Users and access' }}>
      <QueryNote q={q} />
      {q.data && locked.length === 0 && <p className="muted">No account is locked.</p>}
      <ul className="linklist">
        {locked.map((u) => (
          <li key={`${u.user}@${u.host}`} className="mono">
            {u.user}@{u.host}
          </li>
        ))}
      </ul>
    </Card>
  );
}

export function SecurityAdminHome() {
  return (
    <div className="home-grid">
      <Users />
      <Lockouts />
    </div>
  );
}

// ---- Tenant administrator ----

function TenantTree() {
  const me = useSession((s) => s.me);
  const tenants = me?.tenants ?? [];
  return (
    <Card id="tree" title="Tenants">
      <ul className="linklist">
        {tenants.map((t) => (
          <li key={t.id ?? t.name}>
            <Link to={`/t/${encodeURIComponent(t.name ?? '')}/settings`}>{t.path ?? t.name}</Link>{' '}
            <span className="muted small">{t.relation === 'home' ? 'your tenant' : 'sub-tenant'}</span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

function AdminSummaries() {
  const base = useBase();
  const approvals = useOpenApprovals();
  const advice = useIndexAdvice();
  const users = useUsers();
  const list = users.data?.users ?? [];
  return (
    <Card id="summaries" title="At a glance">
      <dl className="kv">
        <dt>
          <Link to={`${base}/inbox`}>Pending approvals</Link>
        </dt>
        <dd>{approvals.data ? approvals.data.open : approvals.isLoading ? '…' : '—'}</dd>
        <dt>
          <Link to={`${base}/indexing?view=recommendations`}>Recommended indexes</Link>
        </dt>
        <dd>{advice.data ? advice.data.indexes.length : advice.isLoading ? '…' : '—'}</dd>
        <dt>
          <Link to={`${base}/access/users`}>Accounts</Link>
        </dt>
        <dd>{users.data ? list.length : users.isLoading ? '…' : '—'}</dd>
        <dt>Locked accounts</dt>
        <dd>{users.data ? list.filter((u) => u.account_locked).length : users.isLoading ? '…' : '—'}</dd>
      </dl>
    </Card>
  );
}

function TenantSettings() {
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const base = useBase();
  const q = useTenantSettings(tenant, me?.principal.tenant === tenant);
  const rows = q.data ?? [];
  return (
    <Card id="settings" title="Settings set on this tenant" more={{ to: `${base}/settings`, label: 'Settings' }}>
      <QueryNote q={q} />
      {q.data && rows.length === 0 && <p className="muted">Every setting is inherited or at its default.</p>}
      <dl className="kv">
        {rows.slice(0, 12).map((r) => (
          <div key={r.key} style={{ display: 'contents' }}>
            <dt className="mono">{r.key}</dt>
            <dd className="mono">{r.value}</dd>
          </div>
        ))}
      </dl>
    </Card>
  );
}

export function TenantAdminHome() {
  return (
    <div className="home-grid">
      <TenantTree />
      <AdminSummaries />
      <Load />
      <Approvals />
      <Users />
      <TenantSettings />
    </div>
  );
}
