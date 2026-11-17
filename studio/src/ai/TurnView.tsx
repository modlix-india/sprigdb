import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { useSession } from '../auth/session';
import { addTab, updateTab, useTabs } from '../editor/tabs';
import { classify } from '../lib/sql';
import { ConfirmDialog, type ConfirmRequest } from '../editor/ConfirmDialog';
import { ChevronDown, ChevronRight } from '../icons';
import { CodeView } from './CodeView';
import { Markdown } from './Markdown';
import { acceptProposal, dismissProposal, rate, setView, usedInEditor, useAssistant } from './store';
import { proposalSql, toolLabel, type AiResult, type Block, type Source, type Turn } from './thread';
import { docsRoute } from './docs';

const FIRST_ROWS = 20;

type ProposalBlock = Extract<Block, { type: 'proposal' }>;
type ToolBlock = Extract<Block, { type: 'tool' }>;

/** Text of a JSON value. */
function text(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') return v.toString();
  return JSON.stringify(v);
}

function cell(v: unknown): string {
  return v === null || v === undefined ? 'NULL' : text(v);
}

/** Opens `sql` in a query tab (the target tab of "Ask for a query", else a new one). */
export function useOpenInTab(): (sql: string, db: string | null, title?: string) => void {
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  const target = useAssistant((s) => s.targetTab);
  const tabs = useTabs((s) => s.tabs);
  return (sql, db, title) => {
    const t = target ? tabs.find((x) => x.id === target) : undefined;
    let id: string;
    if (t && !t.sql.trim()) {
      updateTab(t.id, { sql, db: db ?? t.db, language: 'sql' });
      id = t.id;
    } else {
      id = addTab({ sql, db, title: title ?? undefined }).id;
    }
    void navigate(`/t/${encodeURIComponent(tenant)}/q/${id}`);
  };
}

/** A result shown to the user (run_query): the first rows, and a way to
 * open the query in a tab. */
export function ResultTable({ result, caption, sql, db }: { result: AiResult; caption: string; sql?: string; db?: string | null }) {
  const open = useOpenInTab();
  const rows = result.rows.slice(0, FIRST_ROWS);
  const more = Math.max(0, result.row_count - rows.length);
  return (
    <div className="ai-result">
      <div className="table-scroll" tabIndex={0} role="region" aria-label={caption}>
        <table className="ai-table">
          <thead>
            <tr>
              {result.columns.map((c, i) => (
                <th key={i} scope="col" title={c.type}>
                  {c.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                {result.columns.map((_, j) => (
                  <td key={j} className={r[j] === null ? 'null' : undefined}>
                    {cell(r[j])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="row small muted">
        <span>
          {result.row_count.toLocaleString()} row{result.row_count === 1 ? '' : 's'}
          {more > 0 ? ` · first ${rows.length} shown` : ''}
          {result.truncated ? ' · cut at the cap' : ''}
        </span>
        {sql && (
          <button className="ghost small" onClick={() => open(sql, db ?? null)}>
            Open in tab
          </button>
        )}
      </div>
    </div>
  );
}

function ToolRow({ b }: { b: ToolBlock }) {
  const [open, setOpen] = useState(false);
  const { what, about } = toolLabel(b.name, b.input);
  const state = b.ok === undefined ? 'running' : b.ok ? '' : 'failed';
  return (
    <div className={`ai-tool${state ? ` ${state}` : ''}`}>
      <button className="ai-tool-head" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
        {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        <b>{what}</b>
        {about && <span className="muted"> · {about}</span>}
        {state === 'running' && <span className="muted"> · running…</span>}
        {state === 'failed' && <span className="error"> · failed</span>}
        {b.reduced && <span className="muted"> · rows withheld from the provider</span>}
      </button>
      {open && <pre className="ai-tool-detail">{JSON.stringify(b.input, null, 2)}</pre>}
    </div>
  );
}

const KIND_TITLE: Record<string, string> = {
  sql: 'Proposed statement',
  generate_rows: 'Proposed test rows',
  recommendation: 'Proposed recommendation',
  glossary: 'Proposed glossary term',
  tile: 'Proposed chart',
};

function str(v: unknown): string {
  return typeof v === 'string' ? v : '';
}

/** `checks` as lines people read: a summary, then the other fields. */
function checkLines(checks: unknown): [string, string][] {
  if (!checks || typeof checks !== 'object') return checks === undefined || checks === null ? [] : [['', text(checks)]];
  if (Array.isArray(checks)) return checks.map((c, i) => [String(i + 1), typeof c === 'string' ? c : JSON.stringify(c)]);
  return Object.entries(checks as Record<string, unknown>).map(([k, v]) => [
    k.replace(/_/g, ' '),
    typeof v === 'string' ? v : typeof v === 'number' || typeof v === 'boolean' ? String(v) : JSON.stringify(v),
  ]);
}

function generatedOf(p: ProposalBlock['proposal']): { sql: string; summary: [string, string][] } {
  const g = p.generated;
  if (!g || typeof g !== 'object') return { sql: '', summary: [] };
  const o = g as Record<string, unknown>;
  const summary = Object.entries(o)
    .filter(([k, v]) => k !== 'sql' && (typeof v === 'number' || typeof v === 'string'))
    .map(([k, v]) => [k.replace(/_/g, ' '), String(v)] as [string, string]);
  return { sql: str(o.sql), summary };
}

function Outcome({ b }: { b: ProposalBlock }) {
  const o = b.outcome as
    | { outcome?: string; ran?: { sql: string; results: Record<string, unknown>[] }[]; term?: string; approvals?: Record<string, unknown> }
    | undefined;
  if (b.state === 'failed') return <p className="error" role="status">{b.error ?? 'It failed.'}</p>;
  if (b.state !== 'accepted' || !o) return null;
  const last = o.ran?.[o.ran.length - 1];
  const res = last?.results?.[last.results.length - 1];
  const cols = Array.isArray(res?.columns) ? (res.columns as { name: string; type: string }[]) : [];
  return (
    <div className="stack ai-outcome" role="status">
      {o.ran && (
        <p className="small">
          Ran {o.ran.length} statement{o.ran.length === 1 ? '' : 's'}
          {res && !cols.length && res.affected_rows !== undefined ? ` · ${text(res.affected_rows)} rows affected` : ''}.
        </p>
      )}
      {res && cols.length > 0 && (
        <ResultTable
          result={{ columns: cols, rows: (res.rows as unknown[][]) ?? [], row_count: ((res.rows as unknown[]) ?? []).length, truncated: res.truncated === true }}
          caption="Result of the accepted statement"
        />
      )}
      {o.approvals && <p className="small">Filed in the approvals inbox{o.approvals.id ? ` as ${text(o.approvals.id)}` : ''}.</p>}
      {o.term && <p className="small">The glossary term “{o.term}” is accepted.</p>}
    </div>
  );
}

function ProposalCard({ b }: { b: ProposalBlock }) {
  const me = useSession((s) => s.me);
  const open = useOpenInTab();
  const [confirm, setConfirm] = useState<ConfirmRequest | null>(null);
  const p = b.proposal;
  const body = p.body ?? {};
  const { sql, db } = proposalSql(p);
  const gen = generatedOf(p);
  const cls = sql ? classify(sql) : '';
  const done = b.state === 'accepted' || b.state === 'dismissed';
  const busy = b.state === 'running';
  const isStatement = p.kind === 'sql' || p.kind === 'generate_rows';
  const titleSql = p.kind === 'tile' ? str(body.sql) : sql;
  const checks = checkLines(p.checks);
  const reviewAndRun = () =>
    setConfirm({
      sql: sql || titleSql,
      kind: cls || 'statement',
      target: `${me?.principal.tenant ?? ''}${db ? ` / ${db}` : ''}`,
      note: 'Proposed by the assistant. It runs as you, with your rights.',
    });
  return (
    <section className={`ai-card proposal${done ? ' done' : ''}`} aria-label={KIND_TITLE[p.kind] ?? 'Proposal'} data-testid="ai-proposal">
      <div className="ai-card-h">
        <span>{p.kind === 'tile' ? str(body.title) || KIND_TITLE.tile : KIND_TITLE[p.kind] ?? 'Proposal'}</span>
        <span className="grow" />
        {cls && <span className="badge">{cls.toUpperCase()}</span>}
        {b.state === 'accepted' && <span className="badge ok">Done</span>}
        {b.state === 'dismissed' && <span className="badge">Dismissed</span>}
      </div>
      <div className="ai-card-b stack">
        {str(body.why) && <p className="small">{str(body.why)}</p>}
        {p.kind === 'recommendation' && (
          <>
            <p className="small">
              <b>{str(body.subject)}</b> <span className="muted">({str(body.kind)})</span>
            </p>
            <Markdown text={str(body.body)} />
          </>
        )}
        {p.kind === 'glossary' && (
          <p className="small">
            <b>{str(body.term)}</b> — {str(body.definition)}
            {str(body.sql) && (
              <>
                {' '}
                <code>{str(body.sql)}</code>
              </>
            )}
          </p>
        )}
        {p.kind === 'generate_rows' && (
          <p className="small">
            {text(body.n)} rows for <b>{str(body.table)}</b>
            {str(body.rules) ? ` · ${str(body.rules)}` : ''}
          </p>
        )}
        {p.kind === 'tile' && (
          <p className="small muted">
            Chart: {(() => {
              try {
                return (JSON.parse(str(body.chart)) as { mark?: string }).mark ?? 'chart';
              } catch {
                return 'chart';
              }
            })()}
            {db ? ` · ${db}` : ''}
          </p>
        )}
        {titleSql && p.kind !== 'generate_rows' && <CodeView text={titleSql} label="Proposed statement" />}
        {str(body.estimate) && (
          <p className="ai-est small">
            <span className="muted">Estimate</span> {str(body.estimate)}
          </p>
        )}
        {checks.length > 0 && (
          <div className="ai-checks small" aria-label="What SprigDB checked">
            <span className="muted">Checked before it was shown</span>
            <dl className="kv">
              {checks.map(([k, v], i) => (
                <div key={i} className="contents">
                  <dt>{k || 'check'}</dt>
                  <dd>{v}</dd>
                </div>
              ))}
            </dl>
          </div>
        )}
        {p.kind === 'generate_rows' && (
          <details className="ai-generated">
            <summary>
              Preview the generated statements
              {gen.sql ? ` (${gen.sql.length.toLocaleString()} characters)` : ''}
            </summary>
            {gen.summary.length > 0 && (
              <dl className="kv small">
                {gen.summary.map(([k, v]) => (
                  <div key={k} className="contents">
                    <dt>{k}</dt>
                    <dd>{v}</dd>
                  </div>
                ))}
              </dl>
            )}
            <pre className="ai-code" tabIndex={0} aria-label="Generated statements">
              {gen.sql.length > 20_000 ? `${gen.sql.slice(0, 20_000)}\n…` : gen.sql}
            </pre>
            <p className="small muted">These rows were generated in SprigDB and never sent to the AI provider.</p>
          </details>
        )}
        {b.filed === 'glossary' && (
          <p className="small muted">Filed as a proposed glossary term; a tenant admin accepts it.</p>
        )}
        <Outcome b={b} />
      </div>
      {!done && (
        <div className="ai-card-f row">
          {isStatement && (
            <button className="primary" disabled={busy || !sql} onClick={reviewAndRun}>
              {busy ? 'Running…' : 'Review & run'}
            </button>
          )}
          {p.kind === 'recommendation' && (
            <button className="primary" disabled={busy} onClick={() => void acceptProposal(p.id)}>
              Send to approvals
            </button>
          )}
          {p.kind === 'glossary' && me?.privileges.tenant_admin && (
            <button className="primary" disabled={busy} onClick={() => void acceptProposal(p.id)}>
              Accept term
            </button>
          )}
          {p.kind === 'tile' && (
            <button className="primary" disabled={busy} onClick={() => void acceptProposal(p.id)}>
              Add to dashboard
            </button>
          )}
          {(isStatement || p.kind === 'tile') && (sql || titleSql) && (
            <button
              disabled={busy}
              onClick={() => {
                open(sql || titleSql, db, p.kind === 'tile' ? str(body.title) || undefined : undefined);
                usedInEditor(p.id);
              }}
            >
              Open in query tab
            </button>
          )}
          <button className="ghost" disabled={busy} onClick={() => void dismissProposal(p.id)}>
            Dismiss
          </button>
        </div>
      )}
      <ConfirmDialog
        request={confirm}
        onResult={(ok) => {
          setConfirm(null);
          if (ok) void acceptProposal(p.id);
        }}
      />
    </section>
  );
}

function SourceLink({ s }: { s: Source }) {
  const { tenant = '' } = useParams();
  const base = `/t/${encodeURIComponent(tenant)}`;
  if (s.kind === 'docs') {
    // Studio's own page of the release's docs (the website: from there).
    return <Link to={docsRoute(base, s.reference)}>{s.title}</Link>;
  }
  if (s.kind === 'table' && /^[^.]+\.[^.]+$/.test(s.reference)) {
    const [db, t] = s.reference.split('.') as [string, string];
    return <Link to={`${base}/db/${encodeURIComponent(db)}/obj/table/${encodeURIComponent(t)}`}>{s.title}</Link>;
  }
  if (s.kind === 'glossary') return <Link to={`${base}/glossary`}>{s.title}</Link>;
  return <span title={s.reference}>{s.title}</span>;
}

function Feedback({ t }: { t: Turn }) {
  const [asking, setAsking] = useState(false);
  const [note, setNote] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const go = (v: 'up' | 'down', n?: string) =>
    rate(t.key, v, n).then(
      () => setErr(null),
      (e: unknown) => setErr(e instanceof Error ? e.message : String(e)),
    );
  return (
    <>
      <span className="ai-thumbs" role="group" aria-label="Was this answer helpful?">
        <button
          className="icon-button"
          aria-label="Helpful"
          aria-pressed={t.vote === 'up'}
          disabled={!!t.vote}
          onClick={() => void go('up')}
        >
          👍
        </button>
        <button
          className="icon-button"
          aria-label="Not helpful"
          aria-pressed={t.vote === 'down'}
          disabled={!!t.vote}
          onClick={() => setAsking(true)}
        >
          👎
        </button>
      </span>
      {asking && !t.vote && (
        <form
          className="row ai-note"
          onSubmit={(e) => {
            e.preventDefault();
            setAsking(false);
            void go('down', note.trim() || undefined);
          }}
        >
          <label className="visually-hidden" htmlFor={`note-${t.key}`}>
            What was wrong? (optional)
          </label>
          <input
            id={`note-${t.key}`}
            autoFocus
            maxLength={500}
            placeholder="What was wrong? (optional)"
            value={note}
            onChange={(e) => setNote(e.target.value)}
          />
          <button type="submit">Send</button>
        </form>
      )}
      {err && <span className="error small">{err}</span>}
    </>
  );
}

/** One answer: its text, tool rows, results, proposals, sources and feedback. */
export function TurnView({ t, last }: { t: Turn; last: boolean }) {
  const streaming = t.status === 'streaming';
  const reduced = t.blocks.some((b) => b.type === 'tool' && b.name === 'run_query' && b.reduced);
  return (
    <article className="ai-turn" aria-label="Assistant answer" aria-busy={streaming}>
      {t.blocks.map((b, i) => {
        switch (b.type) {
          case 'text':
            return <Markdown key={i} text={b.text} />;
          case 'thinking':
            return (
              <details key={i} className="ai-thinking small">
                <summary className="muted">Reasoning</summary>
                <p className="muted">{b.text}</p>
              </details>
            );
          case 'tool':
            return <ToolRow key={i} b={b} />;
          case 'result':
            return (
              <div key={i} className="stack">
                {reduced && t.shareData === false && (
                  <p className="ai-shape small">
                    You see the rows. The assistant saw only their shape (columns, types and the row count); a tenant admin
                    can turn on data sharing for richer answers.
                  </p>
                )}
                <ResultTable result={b.result} caption="Query result" sql={b.sql} db={b.db} />
              </div>
            );
          case 'proposal':
            return <ProposalCard key={b.proposal.id} b={b} />;
        }
      })}
      {streaming && <span className="ai-typing" aria-hidden="true" />}
      {t.error && (
        <p className="error" role={last ? 'alert' : undefined}>
          {t.error.code ? `${t.error.code}${t.error.name ? ` ${t.error.name}` : ''}: ` : ''}
          {t.error.message}
        </p>
      )}
      {t.status === 'stopped' && <p className="muted small">Stopped.</p>}
      {t.stop === 'max_tokens' && <p className="muted small">The answer was cut off at its length limit.</p>}
      {t.detail === 'max_tool_rounds' && <p className="muted small">The answer reached its limit of tool rounds.</p>}
      {!streaming && (
        <div className="ai-sources small">
          {t.sources.length > 0 && <span className="muted">Sources:</span>}
          {t.sources.map((s, i) => (
            <SourceLink key={i} s={s} />
          ))}
          <button className="link" onClick={() => setView('sent')}>
            What was sent
          </button>
          <span className="grow" />
          {t.model && (
            <span className="muted" title={`${t.usage.input.toLocaleString()} tokens in, ${t.usage.output.toLocaleString()} out`}>
              {t.model}
            </span>
          )}
          {t.status !== 'failed' && <Feedback t={t} />}
        </div>
      )}
    </article>
  );
}
