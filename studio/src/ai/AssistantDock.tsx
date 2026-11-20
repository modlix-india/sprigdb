// The assistant dock: a right-hand panel opened with Ctrl/Cmd+I or the
// mascot in the status bar. One chat for the tenant's data specialist and
// DBA, and the administration assistant for platform operators. Nothing
// it proposes runs until the user presses Run in the usual confirm dialog.

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { useSnapshot } from '../catalog/snapshot';
import { errorText } from '../api/errors';
import { Mascot } from '../app/Mascot';
import { History, Plus, Share2, ShieldCheck, X } from '../icons';
import * as api from './api';
import { useChips } from './context';
import {
  closeDock,
  newConversation,
  openConversation,
  removeChip,
  send,
  setDraft,
  setRole,
  setView,
  stop,
  takePending,
  useAssistant,
  useRoles,
} from './store';
import { sentCalls, type AiRole, type Item } from './thread';
import { TurnView } from './TurnView';
import './assistant.css';
import { useWorkspaces } from '../workspace/current';
import type { WorkspaceId } from '../workspace/workspaces';

const ROLE_LABEL: Record<AiRole, string> = { data: 'Data', dba: 'DBA', admin: 'Admin' };

/** Three questions from the schema, for the first open: the workspace's
 * own when it has them for this role, else the role's. */
export function suggestions(role: AiRole, db: string | null, tables: string[], workspace?: WorkspaceId): string[] {
  const [a, b] = tables;
  const own = workspace && role !== 'admin' ? workspaceSuggestions(workspace, role, db, a, b) : null;
  if (own) return own;
  if (role === 'admin') {
    return ['Are any clusters unhealthy right now?', 'Why did the last failover happen?', 'What should I check before upgrading?'];
  }
  if (role === 'dba') {
    return [
      db ? `What's slow in ${db} right now?` : 'Which statements take the most time?',
      a ? `Does ${a} need an index?` : 'Are there missing indexes?',
      db ? `Which columns in ${db} hold personal data?` : 'Which columns hold personal data?',
    ];
  }
  return [
    a ? `What's in ${a}, and how does it relate to the other tables?` : 'What tables do I have, and how do they relate?',
    a ? `Show the 10 most recent rows of ${a}` : 'Show me something interesting in my data',
    a && b ? `How many ${a} does each ${b.replace(/s$/, '')} have?` : 'Which tables are the largest?',
  ];
}

/** Starting questions per workspace, in its users' words (the analyst's
 * avoid engine terms); null where the role's general ones fit. */
function workspaceSuggestions(
  ws: WorkspaceId,
  role: AiRole,
  db: string | null,
  a: string | undefined,
  b: string | undefined,
): string[] | null {
  const where = db ?? 'my data';
  switch (ws) {
    case 'analyst':
      return role === 'data'
        ? [
            a ? `What does ${a} hold, in plain words?` : 'Which tables hold my business data?',
            a ? `Show the 10 most recent rows of ${a}` : 'Show me something interesting in my data',
            a && b ? `Make a chart of ${a} by ${b.replace(/s$/, '')}` : 'Make a chart of my busiest month',
          ]
        : null;
    case 'developer':
      return role === 'data'
        ? [
            a && b ? `Write a query joining ${a} and ${b}` : 'Write a query that joins my two largest tables',
            a ? `Draft a trigger that keeps an audit row when ${a} changes` : 'Draft a trigger that keeps an audit trail',
            `How should I index ${where} for my most common queries?`,
          ]
        : null;
    case 'security_admin':
      return [
        a ? `Who can read ${a}, and through which roles?` : 'Which users can change data in every database?',
        a ? `Write a row-level security policy so users see only their own ${a}` : 'Write a row-level security policy for a table',
        db ? `Which columns in ${db} hold personal data?` : 'Which columns hold personal data?',
      ];
    case 'tenant_admin':
      return role === 'dba'
        ? [
            'What needs my attention in this tenant today?',
            db ? `What's slow in ${db} right now?` : 'Which statements take the most time?',
            'Which settings differ from the defaults, and why might they?',
          ]
        : null;
    default:
      return null;
  }
}

function SetupCard({ me }: { me: NonNullable<ReturnType<typeof useSession.getState>['me']> }) {
  const operator = me.platform_roles.includes('operator');
  const ai = me.ai;
  const unavailable = useAssistant((s) => s.unavailable);
  const providerMissing = !ai || ai.roles.filter((r) => r !== 'admin').length === 0;
  return (
    <div className="ai-empty" data-testid="ai-setup">
      <Mascot pose="sleeping" size={96} />
      {operator && providerMissing ? (
        <div className="ai-card stack" style={{ textAlign: 'start' }}>
          <h3 style={{ margin: 0 }}>Connect an AI provider</h3>
          <p className="small">
            The assistants need a provider and a model for each role. Add one with <code>sprigctl ai provider add</code>,
            then set <code>ai.model.data</code> and <code>ai.model.dba</code>; a tenant admin turns the assistants on with{' '}
            <code>ai.enabled</code>.
          </p>
        </div>
      ) : providerMissing ? (
        <p>Ask your operator to set up an AI provider.</p>
      ) : (
        <p>
          The assistants are off for this tenant. {me.privileges.tenant_admin ? 'Turn them on with ' : 'A tenant admin turns them on with '}
          <code>ai.enabled</code>.
        </p>
      )}
      {unavailable?.message && <p className="small muted">{unavailable.message}</p>}
    </div>
  );
}

function Empty({ role, db, onPick }: { role: AiRole; db: string | null; onPick: (q: string) => void }) {
  const snap = useSnapshot(db ?? undefined);
  const me = useSession((s) => s.me);
  const shared = useAssistant((s) => s.shareData) ?? me?.ai?.share_data ?? false;
  const tables = (snap.data?.objects ?? []).filter((o) => o.kind === 'table').map((o) => o.name);
  const { current } = useWorkspaces();
  const qs = suggestions(role, db, tables, current.id);
  return (
    <div className="ai-empty">
      <Mascot pose="waving" size={96} />
      <h3>{role === 'admin' ? 'Ask me about this deployment' : `Ask me about ${db ?? 'your data'}`}</h3>
      <p className="small muted">
        {role === 'admin'
          ? 'I see error codes, counts and the topology, never tenant data.'
          : role === 'dba'
            ? 'I can explain plans, suggest indexes, write policies and logic, and check schema changes.'
            : 'I can explain your data, write queries and build charts.'}{' '}
        {role === 'admin'
          ? ''
          : shared
            ? 'Query results may be shared with the AI provider for richer answers.'
            : 'I see your schema and statistics, not your rows; your admin can change that.'}
      </p>
      <div className="ai-suggest">
        {qs.map((q) => (
          <button key={q} onClick={() => onPick(q)}>
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}

/** The view's Back button, which takes the focus when the view opens. */
function Back() {
  const ref = useRef<HTMLButtonElement>(null);
  useEffect(() => ref.current?.focus(), []);
  return (
    <button ref={ref} onClick={() => setView('chat')}>
      Back
    </button>
  );
}

function ConversationList() {
  const list = useQuery({ queryKey: ['ai', 'conversations'], queryFn: () => api.conversations(50), staleTime: 0 });
  const current = useAssistant((s) => s.conversation);
  const [err, setErr] = useState<string | null>(null);
  return (
    <div className="ai-list" data-testid="ai-conversations">
      <div className="row">
        <h3 style={{ margin: 0, flex: 1 }}>Conversations</h3>
        <Back />
      </div>
      {list.isLoading && <p className="muted">Loading…</p>}
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {err && <p className="error">{err}</p>}
      {list.data?.admin_unavailable && (
        <p className="small muted">Administration conversations aren’t available: {list.data.admin_unavailable}</p>
      )}
      {list.data && list.data.conversations.length === 0 && <p className="muted">No conversations yet.</p>}
      <ul>
        {list.data?.conversations.map((c) => (
          <li key={c.id}>
            <button
              className="ai-conv"
              aria-current={c.id === current ? 'true' : undefined}
              onClick={() =>
                void openConversation(c.id, c.role).catch((e: unknown) => setErr(errorText(e)))
              }
            >
              <span className="ai-conv-title">{c.title}</span>
              <span className="small muted">
                {ROLE_LABEL[(c.role as AiRole) ?? 'data'] ?? c.role}
                {c.updated_ms ? ` · ${new Date(c.updated_ms).toLocaleString()}` : ''}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function SentView() {
  const conv = useAssistant((s) => s.conversation);
  const q = useQuery({
    queryKey: ['ai', 'sent', conv],
    queryFn: () => api.conversation(conv!, true),
    enabled: !!conv,
    staleTime: 0,
  });
  const calls = q.data ? sentCalls(q.data.messages) : [];
  return (
    <div className="ai-list" data-testid="ai-sent">
      <div className="row">
        <h3 style={{ margin: 0, flex: 1 }}>What was sent</h3>
        <Back />
      </div>
      <p className="small muted">
        Every call to the AI provider in this conversation, exactly as it left SprigDB. The payloads are kept for{' '}
        <code>ai.audit_payload_days</code>; after that only their SHA-256 remains.
      </p>
      {!conv && <p className="muted">Nothing was sent yet.</p>}
      {q.isLoading && <p className="muted">Loading…</p>}
      {q.error && <p className="error">{errorText(q.error)}</p>}
      {calls.map((c, i) => (
        <section key={i} className="ai-card stack" aria-label={`Call ${i + 1}`}>
          <div className="small">
            <b>Call {i + 1}</b> · {c.provider ?? 'provider'} · {c.model ?? ''}
            {c.usage && (
              <span className="muted">
                {' '}
                · {(c.usage.input_tokens ?? 0).toLocaleString()} in, {(c.usage.output_tokens ?? 0).toLocaleString()} out
              </span>
            )}
          </div>
          {c.question && <div className="small muted">For: “{c.question.length > 120 ? `${c.question.slice(0, 120)}…` : c.question}”</div>}
          {c.sha256 && (
            <div className="small mono" title="SHA-256 of the exact request body">
              sha256 {c.sha256.slice(0, 16)}…
            </div>
          )}
          {c.error && <p className="error small">{c.error}</p>}
          {c.payload !== undefined ? (
            <details>
              <summary>The exact request</summary>
              <pre className="ai-payload" tabIndex={0} aria-label={`Request of call ${i + 1}`}>
                {typeof c.payload === 'string' ? c.payload : JSON.stringify(c.payload, null, 2)}
              </pre>
            </details>
          ) : (
            <p className="small muted">The payload is no longer kept; its hash is above.</p>
          )}
        </section>
      ))}
    </div>
  );
}

function UserMsg({ it }: { it: Extract<Item, { kind: 'user' }> }) {
  return (
    <div className="ai-user">
      {it.text}
      {it.chips.length > 0 && <div className="small ai-user-chips">with {it.chips.join(', ')}</div>}
    </div>
  );
}

/** The dock. Rendered by the shell while open. */
export function AssistantDock() {
  const me = useSession((s) => s.me);
  const roles = useRoles();
  const role = useAssistant((s) => s.role);
  const items = useAssistant((s) => s.items);
  const view = useAssistant((s) => s.view);
  const running = useAssistant((s) => !!s.running);
  const draft = useAssistant((s) => s.draft);
  const announce = useAssistant((s) => s.announce);
  const focusTick = useAssistant((s) => s.focusTick);
  const pending = useAssistant((s) => s.pending);
  const shareData = useAssistant((s) => s.shareData);
  const { auto, extra, all, database } = useChips();
  const composer = useRef<HTMLTextAreaElement>(null);
  const thread = useRef<HTMLDivElement>(null);
  const returnTo = useRef<Element | null>(null);
  const dock = useRef<HTMLElement>(null);
  // Closing with the shortcut while focus is inside: give it back.
  useLayoutEffect(
    () => () => {
      const r = returnTo.current;
      if (dock.current?.contains(document.activeElement) && r instanceof HTMLElement && document.contains(r)) r.focus();
    },
    [],
  );

  // The role in force: the chosen one when allowed, else the first.
  const effective: AiRole | null = roles.includes(role) ? role : (roles[0] ?? null);
  useEffect(() => {
    if (effective && effective !== role) setRole(effective);
  }, [effective, role]);

  useEffect(() => {
    if (!returnTo.current) returnTo.current = document.activeElement;
    composer.current?.focus();
  }, [focusTick]);

  // An entry point's question: sent with the chips in force.
  useEffect(() => {
    if (pending && effective) {
      const q = takePending();
      if (q) void send(q, all, database);
    }
  }, [pending, effective, all, database]);

  // Follow the answer as it streams, unless the user scrolled up.
  const atEnd = useRef(true);
  useEffect(() => {
    const el = thread.current;
    if (el && atEnd.current) el.scrollTop = el.scrollHeight;
  }, [items]);

  const close = () => {
    closeDock();
    const r = returnTo.current;
    returnTo.current = null;
    if (r instanceof HTMLElement && document.contains(r)) r.focus();
  };
  // Escape with nothing focused (some browsers don't focus a clicked button).
  const closeRef = useRef(close);
  useEffect(() => {
    closeRef.current = close;
  });
  useEffect(() => {
    const on = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !e.defaultPrevented && document.activeElement === document.body && !document.querySelector('dialog[open]')) {
        closeRef.current();
      }
    };
    window.addEventListener('keydown', on);
    return () => window.removeEventListener('keydown', on);
  }, []);

  const shared = role !== 'admin' && (shareData ?? me?.ai?.share_data ?? false);
  const badge =
    role === 'admin'
      ? { text: 'No tenant data', title: 'The administration assistant gets codes, counts and the topology, never tenant data.' }
      : shared
        ? { text: 'Data shared', title: 'ai.share_data is on: query results and sample values may be sent to the AI provider.' }
        : { text: 'Schema only', title: 'Only schema, plans and statistics leave SprigDB; rows and values stay home (ai.share_data is off).' };

  const placeholder = useMemo(
    () =>
      role === 'admin'
        ? 'Ask about this deployment… (Shift+Enter for a new line)'
        : `Ask about ${database ?? 'your data'}… (Shift+Enter for a new line)`,
    [role, database],
  );

  const submit = () => {
    if (!effective || running || !draft.trim()) return;
    atEnd.current = true;
    void send(draft, all, database);
  };

  return (
    <aside
      ref={dock}
      className="ai-dock"
      aria-label="Assistant"
      data-testid="assistant-dock"
      onKeyDown={(e) => {
        if (e.key === 'Escape' && !(e.target as HTMLElement).closest('dialog')) {
          e.preventDefault();
          close();
        }
      }}
    >
      <div className="ai-dock-h">
        <Mascot pose={running ? 'working' : 'avatar'} size={22} />
        <h2 className="ai-title">Assistant</h2>
        <span className="grow" />
        {roles.length > 1 && (
          <div className="ai-roles" role="group" aria-label="Assistant role">
            {roles.map((r) => (
              <button key={r} aria-pressed={r === role} onClick={() => setRole(r)} disabled={running}>
                {ROLE_LABEL[r]}
              </button>
            ))}
          </div>
        )}
        {roles.length > 0 && (
          <span className={`badge ai-badge${shared ? ' shared' : ''}`} title={badge.title} data-testid="ai-data-badge">
            {shared ? <Share2 size={11} /> : <ShieldCheck size={11} />} {badge.text}
          </span>
        )}
        {roles.length > 0 && (
          <>
            <button className="icon-button" aria-label="Conversations" title="Conversations" onClick={() => setView(view === 'list' ? 'chat' : 'list')}>
              <History size={15} />
            </button>
            <button className="icon-button" aria-label="New conversation" title="New conversation" onClick={newConversation}>
              <Plus size={15} />
            </button>
          </>
        )}
        <button className="icon-button" aria-label="Close the assistant" title="Close (Esc)" onClick={close}>
          <X size={15} />
        </button>
      </div>
      {!me || roles.length === 0 ? (
        me && <SetupCard me={me} />
      ) : (
        <>
          {view === 'chat' && all.length > 0 && (
            <div className="ai-chips" role="list" aria-label="Context sent with your question">
              {[...auto.map((c) => ({ c, extra: false })), ...extra.map((c) => ({ c, extra: true }))].map(({ c, extra: x }) => (
                <span key={c.id} className="ai-chip" role="listitem" title={c.kind === 'query' || c.kind === 'note' ? c.ref : undefined}>
                  <span className="muted">{c.kind === 'database' ? 'db' : c.kind}</span> {c.label}
                  <button className="ai-chip-x" aria-label={`Remove ${c.label}`} onClick={() => removeChip(c.id, x)}>
                    <X size={11} />
                  </button>
                </span>
              ))}
            </div>
          )}
          {view === 'list' && <ConversationList />}
          {view === 'sent' && <SentView />}
          {view === 'chat' && (
            <div
              className="ai-thread"
              ref={thread}
              onScroll={(e) => {
                const el = e.currentTarget;
                atEnd.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
              }}
            >
              {items.length === 0 && effective && (
                <Empty
                  role={effective}
                  db={database}
                  onPick={(q) => {
                    atEnd.current = true;
                    void send(q, all, database);
                  }}
                />
              )}
              {items.map((it, i) =>
                it.kind === 'user' ? <UserMsg key={it.key} it={it} /> : <TurnView key={it.key} t={it} last={i === items.length - 1} />,
              )}
            </div>
          )}
          {view === 'chat' && (
            <form
              className="ai-composer"
              onSubmit={(e) => {
                e.preventDefault();
                submit();
              }}
            >
              <label className="visually-hidden" htmlFor="ai-input">
                Ask the assistant
              </label>
              <textarea
                id="ai-input"
                ref={composer}
                rows={2}
                value={draft}
                placeholder={placeholder}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
                    e.preventDefault();
                    submit();
                  }
                }}
              />
              {running ? (
                <button type="button" onClick={stop} data-testid="ai-stop">
                  Stop
                </button>
              ) : (
                <button type="submit" className="primary" disabled={!draft.trim()}>
                  Send
                </button>
              )}
            </form>
          )}
          <p className="ai-hint">Ctrl/⌘ I toggles · Esc closes · nothing changes until you press Run</p>
        </>
      )}
      <div className="visually-hidden" aria-live="polite" role="status" data-testid="ai-live">
        {announce}
      </div>
    </aside>
  );
}
