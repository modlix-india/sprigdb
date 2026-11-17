// The tenant glossary: business terms the assistants use exactly
// ("revenue excludes refunds"). Everyone reads it; tenant admins add,
// replace and delete terms, and accept or reject the assistants' proposals.

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import * as api from './api';
import './assistant.css';

function Editor({ initial, onDone }: { initial?: api.GlossaryTerm; onDone: () => void }) {
  const qc = useQueryClient();
  const [term, setTerm] = useState(initial?.term ?? '');
  const [definition, setDefinition] = useState(initial?.definition ?? '');
  const [sql, setSql] = useState(initial?.sql ?? '');
  const [tables, setTables] = useState((initial?.tables ?? []).join(', '));
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  return (
    <form
      className="card stack"
      aria-label={initial ? `Edit ${initial.term}` : 'New term'}
      onSubmit={(e) => {
        e.preventDefault();
        setBusy(true);
        api
          .putTerm({
            term: term.trim(),
            definition: definition.trim(),
            ...(sql.trim() ? { sql: sql.trim() } : {}),
            ...(tables.trim() ? { tables: tables.split(',').map((t) => t.trim()).filter(Boolean) } : {}),
          })
          .then(
            () => {
              void qc.invalidateQueries({ queryKey: ['ai', 'glossary'] });
              onDone();
            },
            (e: unknown) => setErr(errorText(e)),
          )
          .finally(() => setBusy(false));
      }}
    >
      <label className="stack">
        Term
        <input required value={term} onChange={(e) => setTerm(e.target.value)} readOnly={!!initial} maxLength={128} />
      </label>
      <label className="stack">
        Definition
        <textarea required rows={3} value={definition} onChange={(e) => setDefinition(e.target.value)} />
      </label>
      <label className="stack">
        SQL expression (optional)
        <input className="mono" value={sql} onChange={(e) => setSql(e.target.value)} placeholder="SUM(total) - SUM(refunded)" />
      </label>
      <label className="stack">
        Tables (optional, comma-separated)
        <input value={tables} onChange={(e) => setTables(e.target.value)} placeholder="shop.orders" />
      </label>
      {err && <p className="error">{err}</p>}
      <div className="row">
        <button type="submit" className="primary" disabled={busy || !term.trim() || !definition.trim()}>
          Save
        </button>
        <button type="button" onClick={onDone}>
          Cancel
        </button>
      </div>
    </form>
  );
}

export function GlossaryPage() {
  const qc = useQueryClient();
  const list = useQuery({ queryKey: ['ai', 'glossary'], queryFn: api.glossary });
  const [editing, setEditing] = useState<api.GlossaryTerm | 'new' | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const verb = (term: string, v: 'accept' | 'reject' | 'delete') =>
    api.termVerb(term, v).then(
      () => {
        setErr(null);
        void qc.invalidateQueries({ queryKey: ['ai', 'glossary'] });
      },
      (e: unknown) => setErr(errorText(e)),
    );
  const terms = list.data?.terms ?? [];
  const canEdit = list.data?.can_edit ?? false;
  const proposed = terms.filter((t) => t.status === 'proposed');
  const accepted = terms.filter((t) => t.status === 'accepted');
  return (
    <div className="stack ai-page">
      <div className="row">
        <h1 style={{ margin: 0, flex: 1 }}>Glossary</h1>
        {canEdit && !editing && (
          <button className="primary" onClick={() => setEditing('new')}>
            Add a term
          </button>
        )}
      </div>
      <p className="muted">
        Business terms the assistants use as written here, with their SQL. {canEdit ? '' : 'Tenant admins edit them.'}
      </p>
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {err && <p className="error" role="alert">{err}</p>}
      {editing && <Editor initial={editing === 'new' ? undefined : editing} onDone={() => setEditing(null)} />}
      {proposed.length > 0 && (
        <section className="stack" aria-labelledby="gl-proposed">
          <h2 id="gl-proposed" style={{ margin: 0, fontSize: 16 }}>
            Proposed by the assistants ({proposed.length})
          </h2>
          <table className="table">
            <thead>
              <tr>
                <th scope="col">Term</th>
                <th scope="col">Definition</th>
                <th scope="col">SQL</th>
                <th scope="col">From</th>
                {canEdit && <th scope="col">Decide</th>}
              </tr>
            </thead>
            <tbody>
              {proposed.map((t) => (
                <tr key={`p-${t.term}`}>
                  <td>
                    <b>{t.term}</b>
                  </td>
                  <td>{t.definition}</td>
                  <td className="mono small">{t.sql ?? ''}</td>
                  <td className="small">{t.author ?? ''}</td>
                  {canEdit && (
                    <td>
                      <div className="row" style={{ flexWrap: 'nowrap' }}>
                        <button onClick={() => void verb(t.term, 'accept')}>Accept</button>
                        <button className="ghost" onClick={() => void verb(t.term, 'reject')}>
                          Reject
                        </button>
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
      <section className="stack" aria-labelledby="gl-accepted">
        <h2 id="gl-accepted" style={{ margin: 0, fontSize: 16 }}>
          Terms ({accepted.length})
        </h2>
        {list.data && accepted.length === 0 && <p className="muted">No terms yet.</p>}
        {accepted.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th scope="col">Term</th>
                <th scope="col">Definition</th>
                <th scope="col">SQL</th>
                <th scope="col">Tables</th>
                {canEdit && <th scope="col">Change</th>}
              </tr>
            </thead>
            <tbody>
              {accepted.map((t) => (
                <tr key={`a-${t.term}`}>
                  <td>
                    <b>{t.term}</b>
                  </td>
                  <td>{t.definition}</td>
                  <td className="mono small">{t.sql ?? ''}</td>
                  <td className="small">{(t.tables ?? []).join(', ')}</td>
                  {canEdit && (
                    <td>
                      <div className="row" style={{ flexWrap: 'nowrap' }}>
                        <button onClick={() => setEditing(t)}>Edit</button>
                        <button
                          className="ghost"
                          onClick={() => {
                            if (window.confirm(`Delete the term “${t.term}”?`)) void verb(t.term, 'delete');
                          }}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
