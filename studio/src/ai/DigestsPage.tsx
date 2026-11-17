// The tenant's digests (ai.digest = weekly or daily): what changed in the
// period, built from statistics and worded by the data assistant. Tenant
// admins only, since a digest covers every database.

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import * as api from './api';
import { Markdown } from './Markdown';
import './assistant.css';

const day = (ms: number) => new Date(ms).toISOString().slice(0, 10);

function Detail({ id }: { id: string }) {
  const d = useQuery({ queryKey: ['ai', 'digest', id], queryFn: () => api.digest(id) });
  if (d.isLoading) return <p className="muted">Loading…</p>;
  if (d.error) return <p className="error">{errorText(d.error)}</p>;
  if (!d.data) return null;
  const x = d.data;
  return (
    <section className="card stack" aria-label={`Digest ${x.id}`} data-testid="digest-detail">
      <h2 style={{ margin: 0, fontSize: 16 }}>
        {x.cadence === 'weekly' ? 'Week' : 'Day'} of {day(x.from_ms)}
        {x.cadence === 'weekly' ? ` to ${day(x.to_ms - 1)}` : ''}
      </h2>
      <div className="small muted">
        {x.status === 'worded' ? `Worded by ${x.model ?? 'the data assistant'}` : 'Facts only'}
        {x.note ? ` · ${x.note}` : ''}
        {x.emailed_ms ? ` · emailed ${new Date(x.emailed_ms).toLocaleString()}` : ''}
      </div>
      <Markdown text={x.body} />
      <details>
        <summary>The facts it was built from</summary>
        <pre className="ai-payload" tabIndex={0} aria-label="Facts">
          {x.facts_text}
        </pre>
      </details>
      {(x.sent ?? []).length > 0 && (
        <details>
          <summary>What was sent to word it</summary>
          <pre className="ai-payload" tabIndex={0} aria-label="What was sent">
            {JSON.stringify(x.sent, null, 2)}
          </pre>
        </details>
      )}
    </section>
  );
}

export function DigestsPage() {
  const list = useQuery({ queryKey: ['ai', 'digests'], queryFn: () => api.digests(30) });
  const [open, setOpen] = useState<string | null>(null);
  const first = list.data?.digests[0]?.id ?? null;
  const shown = open ?? first;
  return (
    <div className="stack ai-page">
      <h1 style={{ margin: 0 }}>Digests</h1>
      <p className="muted">
        What changed in your databases each day or week, from statistics only. Turn them on with <code>ai.digest</code> (
        <code>weekly</code> or <code>daily</code>).
      </p>
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {list.data && list.data.digests.length === 0 && <p className="muted">No digests yet.</p>}
      {list.data && list.data.digests.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th scope="col">Period</th>
              <th scope="col">Notable</th>
              <th scope="col">Words</th>
            </tr>
          </thead>
          <tbody>
            {list.data.digests.map((d) => (
              <tr key={d.id}>
                <td>
                  <button className="link" onClick={() => setOpen(d.id)} aria-pressed={d.id === shown}>
                    {d.cadence} · {day(d.from_ms)}
                  </button>
                </td>
                <td className="small">{(d.notable ?? []).slice(0, 3).join('; ') || '—'}</td>
                <td className="small">{d.status === 'worded' ? (d.model ?? 'worded') : 'facts only'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {shown && <Detail id={shown} />}
    </div>
  );
}
