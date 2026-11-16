// Ctrl/Cmd+I in a query tab: ask the data assistant to change the query
// in the editor. Its proposal comes back as a diff over the editor's text:
// Tab puts it in the editor (nothing runs), Esc discards it.

import { useEffect, useRef, useState } from 'react';
import { errorText } from '../api/errors';
import * as api from './api';
import { lineDiff } from './diff';
import { proposalSql, type Proposal } from './thread';

type Phase =
  | { k: 'asking' }
  | { k: 'waiting'; ac: AbortController }
  | { k: 'diff'; proposal: Proposal; sql: string; text: string }
  | { k: 'none'; text: string }
  | { k: 'error'; message: string };

/** The message an inline ask sends. */
export function inlineMessage(instruction: string, hasQuery: boolean): string {
  return hasQuery
    ? `Change the query in my editor: ${instruction}\nPropose the whole new query with propose_sql. Don't run it.`
    : `Write a query: ${instruction}\nPropose it with propose_sql. Don't run it.`;
}

export function InlineAsk({
  sql,
  db,
  onApply,
  onClose,
}: {
  sql: string;
  db: string | null;
  onApply: (sql: string) => void;
  onClose: () => void;
}) {
  const [phase, setPhase] = useState<Phase>({ k: 'asking' });
  const [q, setQ] = useState('');
  const input = useRef<HTMLInputElement>(null);
  const diffBox = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (phase.k === 'asking') input.current?.focus();
    if (phase.k === 'diff' || phase.k === 'none' || phase.k === 'error') diffBox.current?.focus();
  }, [phase.k]);
  useEffect(() => () => {
    if (phase.k === 'waiting') phase.ac.abort();
  }, [phase]);

  const go = async () => {
    const instruction = q.trim();
    if (!instruction) return;
    const ac = new AbortController();
    setPhase({ k: 'waiting', ac });
    let found: Proposal | null = null;
    let text = '';
    try {
      await api.chat(
        {
          role: 'data',
          message: inlineMessage(instruction, !!sql.trim()),
          ...(db ? { database: db } : {}),
          ...(sql.trim() ? { context: [{ kind: 'query' as const, ref: sql }] } : {}),
        },
        (e) => {
          if (e.type === 'proposal' && e.proposal.kind === 'sql' && !found) found = e.proposal;
          if (e.type === 'text') text += e.text;
          if (e.type === 'error') text += `\n${e.error.message ?? ''}`;
        },
        ac.signal,
      );
      const p = found as Proposal | null;
      if (p) setPhase({ k: 'diff', proposal: p, sql: proposalSql(p).sql, text });
      else setPhase({ k: 'none', text: text.trim() || 'The assistant proposed no query.' });
    } catch (e) {
      if (!ac.signal.aborted) setPhase({ k: 'error', message: errorText(e) });
    }
  };

  const discard = () => {
    if (phase.k === 'waiting') phase.ac.abort();
    if (phase.k === 'diff') void api.dismiss(phase.proposal.id).catch(() => undefined);
    onClose();
  };
  const apply = () => {
    if (phase.k !== 'diff') return;
    onApply(phase.sql);
    void api.outcome(phase.proposal.id, 'accepted', 'put in the editor, not run').catch(() => undefined);
    onClose();
  };

  return (
    <div className="ai-inline" role="group" aria-label="Ask the assistant to change this query" data-testid="ai-inline">
      {(phase.k === 'asking' || phase.k === 'waiting') && (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void go();
          }}
        >
          <label className="visually-hidden" htmlFor="ai-inline-q">
            What should change?
          </label>
          <input
            id="ai-inline-q"
            ref={input}
            value={q}
            disabled={phase.k === 'waiting'}
            placeholder={sql.trim() ? 'Change this query: add a filter, join a table…' : 'Describe the query you want…'}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                e.preventDefault();
                discard();
              }
            }}
          />
          <button type="submit" className="primary" disabled={phase.k === 'waiting' || !q.trim()}>
            {phase.k === 'waiting' ? 'Asking…' : 'Ask'}
          </button>
          <button type="button" onClick={discard}>
            {phase.k === 'waiting' ? 'Stop' : 'Cancel'}
          </button>
        </form>
      )}
      {phase.k !== 'asking' && phase.k !== 'waiting' && (
        <div
          ref={diffBox}
          tabIndex={0}
          className="stack"
          aria-label={phase.k === 'diff' ? 'Proposed change: Tab accepts, Escape discards' : 'The assistant’s answer'}
          onKeyDown={(e) => {
            if (e.key === 'Tab' && phase.k === 'diff') {
              e.preventDefault();
              apply();
            } else if (e.key === 'Escape') {
              e.preventDefault();
              discard();
            }
          }}
        >
          {phase.k === 'diff' && (
            <>
              <div className="ai-diff" data-testid="ai-diff">
                {lineDiff(sql, phase.sql).map((l, i) => (
                  <div key={i} className={l.op === 'same' ? undefined : l.op}>
                    <span aria-hidden="true">{l.op === 'add' ? '+ ' : l.op === 'del' ? '- ' : '  '}</span>
                    <span className="visually-hidden">{l.op === 'add' ? 'added: ' : l.op === 'del' ? 'removed: ' : ''}</span>
                    {l.text || ' '}
                  </div>
                ))}
              </div>
              {typeof phase.proposal.body.why === 'string' && <p className="small muted">{phase.proposal.body.why}</p>}
              <div className="row small">
                <button className="primary" onClick={apply}>
                  Accept <kbd>Tab</kbd>
                </button>
                <button onClick={discard}>
                  Discard <kbd>Esc</kbd>
                </button>
                <span className="muted">Accepting puts it in the editor; nothing runs.</span>
              </div>
            </>
          )}
          {phase.k === 'none' && (
            <>
              <p className="small">{phase.text}</p>
              <button onClick={onClose}>Close</button>
            </>
          )}
          {phase.k === 'error' && (
            <>
              <p className="error small">{phase.message}</p>
              <button onClick={onClose}>Close</button>
            </>
          )}
        </div>
      )}
    </div>
  );
}
