// Operator mode's Incidents: the administration assistant's incidents
// (errors counted by code and place, triaged and advised) and health
// warnings, from the Admin API (/admin/v1/ai/incidents and /ai/health,
// through /v1/ops/). Advice is advisory: it lands in the approvals inbox
// and nothing changes by itself. "What was sent" shows the exact payloads.

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { errorText } from '../api/errors';
import { Markdown } from './Markdown';
import { sentCalls, type StoredMessage } from './thread';
import './assistant.css';

export type OpsFetch = <T>(path: string) => Promise<T>;

interface IncidentRow {
  fingerprint: string;
  code: number;
  code_name?: string;
  component: string;
  span: string;
  status: string;
  total: number;
  last_hour?: number;
  first_seen_ms?: number;
  last_seen_ms?: number;
  verdict?: string | null;
  severity?: string | null;
  summary?: string | null;
  advice_id?: string | null;
  held?: string | null;
}

interface Finding {
  key: string;
  rule: string;
  severity: string;
  text: string;
  status: string;
  reason?: string | null;
  first_found?: string;
  last_found?: string;
  advice_id?: string | null;
}

const when = (ms?: number) => (ms ? new Date(ms).toISOString().replace('T', ' ').slice(0, 16) : '—');

function sevClass(s?: string | null): string {
  const v = (s ?? '').toLowerCase();
  return v === 'high' || v === 'critical' ? 'sev high' : v === 'medium' || v === 'warn' ? 'sev warn' : 'sev info';
}

function Detail({ fp, fetch }: { fp: string; fetch: OpsFetch }) {
  const q = useQuery({
    queryKey: ['ops', 'ai', 'incident', fp],
    queryFn: () =>
      fetch<{
        incident: Record<string, unknown>;
        last_hour?: number;
        baseline_per_hour?: number;
        conversation?: { id: string; messages?: StoredMessage[]; unavailable?: string };
      }>(`ai/incidents/${encodeURIComponent(fp)}?payloads=true`),
  });
  if (q.isLoading) return <p className="muted">Loading…</p>;
  if (q.error) return <p className="error">{errorText(q.error)}</p>;
  if (!q.data) return null;
  const inc = q.data.incident;
  const s = (k: string): string => {
    const v = inc[k];
    return typeof v === 'string' ? v : typeof v === 'number' ? v.toString() : '';
  };
  const advice = s('advice') || s('advice_text') || s('summary');
  const calls = sentCalls(q.data.conversation?.messages ?? []);
  return (
    <section className="card stack" aria-label={`Incident ${fp}`} data-testid="incident-detail">
      <h2 style={{ margin: 0, fontSize: 16 }}>
        {s('code_name') || `Error ${s('code')}`} · {s('component')}
      </h2>
      <p className="small muted">
        Advice from the administration assistant · advisory only: nothing was changed
        {s('advice_id') ? ` · approvals item ${s('advice_id')}` : ''}
      </p>
      {advice ? <Markdown text={advice} /> : <p className="muted">No advice yet ({s('status') || 'counting'}).</p>}
      <dl className="kv small">
        <dt>Problem</dt>
        <dd className="mono">
          {s('code')} · {s('component')} · {s('span')} · {s('total')} in all
          {q.data.last_hour !== undefined ? ` · ${q.data.last_hour} in the last hour` : ''}
          {q.data.baseline_per_hour !== undefined ? ` · baseline ${q.data.baseline_per_hour.toFixed(1)}/h` : ''}
        </dd>
        <dt>Not sent</dt>
        <dd>tenant names, database and table names, statements, values, addresses</dd>
      </dl>
      {q.data.conversation?.unavailable && <p className="small muted">The conversation isn’t available: {q.data.conversation.unavailable}</p>}
      {calls.length > 0 && (
        <details>
          <summary>What was sent ({calls.length} call{calls.length === 1 ? '' : 's'})</summary>
          {calls.map((c, i) => (
            <div key={i} className="stack" style={{ marginBlockStart: 6 }}>
              <div className="small">
                <b>Call {i + 1}</b> · {c.provider ?? ''} · {c.model ?? ''} {c.sha256 ? `· sha256 ${c.sha256.slice(0, 16)}…` : ''}
              </div>
              {c.payload !== undefined && (
                <pre className="ai-payload" tabIndex={0} aria-label={`Request of call ${i + 1}`}>
                  {typeof c.payload === 'string' ? c.payload : JSON.stringify(c.payload, null, 2)}
                </pre>
              )}
            </div>
          ))}
        </details>
      )}
    </section>
  );
}

/** The Incidents area. */
export function Incidents({ fetch }: { fetch: OpsFetch }) {
  const me = useSession((s) => s.me);
  const [open, setOpen] = useState<string | null>(null);
  const inc = useQuery({
    queryKey: ['ops', 'ai', 'incidents'],
    queryFn: () => fetch<{ incidents: IncidentRow[] }>('ai/incidents'),
    refetchInterval: 30_000,
  });
  const health = useQuery({
    queryKey: ['ops', 'ai', 'health'],
    queryFn: () => fetch<{ findings: Finding[] }>('ai/health'),
    refetchInterval: 60_000,
  });
  const noModel = !(me?.ai?.roles ?? []).includes('admin');
  return (
    <div className="stack ai-page" data-testid="ops-incidents">
      {noModel && (
        <div className="card stack" data-testid="ai-connect-provider">
          <h2 style={{ margin: 0, fontSize: 16 }}>Connect an AI provider</h2>
          <p className="small">
            The administration assistant words advice for incidents and health warnings. Add a provider with{' '}
            <code>sprigctl ai provider add</code>, set <code>ai.model.admin</code> (and optionally <code>ai.model.triage</code>), then turn
            it on with <code>ai.admin.enabled</code>. Without a model, health warnings are still filed with the rule’s own words.
          </p>
        </div>
      )}
      <section className="stack" aria-labelledby="inc-h">
        <h2 id="inc-h" style={{ margin: 0, fontSize: 16 }}>
          Incidents
        </h2>
        {inc.error && <p className="error">{errorText(inc.error)}</p>}
        {inc.data && inc.data.incidents.length === 0 && <p className="muted">No incidents in this data center.</p>}
        {inc.data && inc.data.incidents.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th scope="col">Severity</th>
                <th scope="col">Problem</th>
                <th scope="col">Count</th>
                <th scope="col">Last seen</th>
                <th scope="col">Status</th>
              </tr>
            </thead>
            <tbody>
              {inc.data.incidents.map((r) => (
                <tr key={r.fingerprint}>
                  <td>
                    <span className={sevClass(r.severity)}>{(r.severity ?? 'info').toUpperCase()}</span>
                  </td>
                  <td>
                    <button className="link" aria-pressed={open === r.fingerprint} onClick={() => setOpen(r.fingerprint)}>
                      {r.summary || `${r.code_name ?? r.code} in ${r.component}`}
                    </button>
                    <div className="small muted mono">
                      {r.code} · {r.component} · {r.span}
                    </div>
                  </td>
                  <td>
                    {r.total.toLocaleString()}
                    {r.last_hour ? <span className="small muted"> ({r.last_hour} last hour)</span> : null}
                  </td>
                  <td className="small">{when(r.last_seen_ms)}</td>
                  <td className="small">
                    {r.status}
                    {r.held ? ` · held: ${r.held}` : ''}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {open && <Detail fp={open} fetch={fetch} />}
      </section>
      <section className="stack" aria-labelledby="health-h">
        <h2 id="health-h" style={{ margin: 0, fontSize: 16 }}>
          Health warnings
        </h2>
        {health.error && <p className="error">{errorText(health.error)}</p>}
        {health.data && health.data.findings.length === 0 && <p className="muted">No health findings.</p>}
        {health.data?.findings.map((f) => (
          <article key={f.key} className="card stack">
            <div className="row">
              <span className={sevClass(f.severity)}>{f.severity.toUpperCase()}</span>
              <b>{f.rule.replace(/_/g, ' ')}</b>
              <span className="small muted">
                {f.status}
                {f.last_found ? ` · last found ${f.last_found}` : ''}
                {f.advice_id ? ` · approvals item ${f.advice_id}` : ''}
              </span>
            </div>
            <Markdown text={f.text} />
            {f.reason && <p className="small muted">{f.reason}</p>}
          </article>
        ))}
      </section>
    </div>
  );
}
