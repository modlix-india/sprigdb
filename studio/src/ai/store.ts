// The assistant dock's state: open or closed, the role, the thread, the
// chips, and the turn that is streaming. Entry points elsewhere in Studio
// (errors, Performance, tables, recommendations, the palette) call ask().

import { create } from 'zustand';
import { useSession } from '../auth/session';
import { isApiError, type ErrorBody } from '../api/errors';
import * as api from './api';
import {
  answerId,
  applyEvent,
  itemKey,
  newTurn,
  rebuild,
  updateProposal,
  type AiRole,
  type Block,
  type Item,
  type Turn,
} from './thread';

export type ChipKind = 'database' | 'table' | 'query' | 'note' | 'result';

/** Something the next question is about: removable, sent as context. */
export interface Chip {
  id: string;
  kind: ChipKind;
  label: string;
  /** What is sent: a database name, `db.table`, a statement, a note. */
  ref: string;
}

/** Codes that mean the assistants aren't available to this user. */
export const SETUP_CODES = new Set([39990, 39991]);

export interface AssistantState {
  open: boolean;
  view: 'chat' | 'list' | 'sent';
  role: AiRole;
  items: Item[];
  conversation: string | null;
  running: AbortController | null;
  draft: string;
  /** Chips an entry point added (an error, a statement, a recommendation). */
  extra: Chip[];
  /** Ids of the automatic chips the user removed. */
  hidden: string[];
  /** The query tab "Open in query tab" writes into (Ask for a query). */
  targetTab: string | null;
  /** A question an entry point asked, sent by the dock with its chips. */
  pending: string | null;
  /** The polite live region's text. */
  announce: string;
  /** The gateway said the assistants aren't available (39990, 39991). */
  unavailable: ErrorBody | null;
  /** ai.share_data as the last turn applied it (null before any turn). */
  shareData: boolean | null;
  /** Bumped to move focus to the composer. */
  focusTick: number;
}

const initial: Omit<AssistantState, 'role'> = {
  open: false,
  view: 'chat',
  items: [],
  conversation: null,
  running: null,
  draft: '',
  extra: [],
  hidden: [],
  targetTab: null,
  pending: null,
  announce: '',
  unavailable: null,
  shareData: null,
  focusTick: 0,
};

export const useAssistant = create<AssistantState>(() => ({ ...initial, role: 'data' }));

const set = useAssistant.setState;
const get = useAssistant.getState;

/** The roles this user may chat with now. */
export function availableRoles(ai: { enabled: boolean; roles: AiRole[] } | undefined): AiRole[] {
  if (!ai) return [];
  return ai.roles.filter((r) => r === 'admin' || ai.enabled);
}

/** The roles available, from the session (a hook). */
export function useRoles(): AiRole[] {
  const ai = useSession((s) => s.me?.ai);
  const unavailable = useAssistant((s) => s.unavailable);
  const roles = availableRoles(ai);
  return unavailable ? roles.filter((r) => r === 'admin') : roles;
}

export function toggleDock(): void {
  const open = !get().open;
  set({ open, focusTick: open ? get().focusTick + 1 : get().focusTick });
}

export function openDock(): void {
  set({ open: true, focusTick: get().focusTick + 1 });
}

export function closeDock(): void {
  set({ open: false });
}

export function setView(view: AssistantState['view']): void {
  set({ view });
}

export function setDraft(draft: string): void {
  set({ draft });
}

/** A fresh conversation (the current one stays in the list). */
export function newConversation(): void {
  get().running?.abort();
  set({ items: [], conversation: null, running: null, view: 'chat', extra: [], targetTab: null, announce: 'New conversation' });
}

/** Switches the assistant; a conversation belongs to one, so a new one starts. */
export function setRole(role: AiRole): void {
  if (role === get().role) return;
  if (get().items.length) newConversation();
  set({ role });
}

export function removeChip(id: string, extra: boolean): void {
  if (extra) set({ extra: get().extra.filter((c) => c.id !== id) });
  else set({ hidden: [...get().hidden, id] });
}

export interface AskOptions {
  role?: AiRole;
  /** The question; sent at once unless `draft` is set. */
  text?: string;
  /** Put the text in the composer instead of sending it. */
  draft?: boolean;
  chips?: Chip[];
  targetTab?: string | null;
  /** Start a new conversation first. */
  fresh?: boolean;
}

/** Opens the dock for an entry point: a role, chips, and a question. */
export function ask(o: AskOptions): void {
  if (get().running) get().running?.abort();
  if (o.fresh !== false && (get().items.length || (o.role && o.role !== get().role))) newConversation();
  set({
    open: true,
    view: 'chat',
    role: o.role ?? get().role,
    extra: o.chips ?? [],
    hidden: [],
    targetTab: o.targetTab ?? null,
    draft: o.draft ? (o.text ?? '') : get().draft,
    pending: !o.draft && o.text ? o.text : null,
    focusTick: get().focusTick + 1,
  });
}

/** The ask() text the dock is about to send (taken once). */
export function takePending(): string | null {
  const p = get().pending;
  if (p !== null) set({ pending: null });
  return p;
}

function patchTurn(key: string, f: (t: Turn) => Turn): void {
  set({ items: get().items.map((it) => (it.kind === 'turn' && it.key === key ? f(it) : it)) });
}

function questions(items: Item[]): number {
  return items.filter((i) => i.kind === 'user').length;
}

/** Sends a question with the chips in force and streams the answer. */
export async function send(text: string, chips: Chip[], database: string | null): Promise<void> {
  const message = text.trim();
  if (!message || get().running) return;
  const role = get().role;
  const ac = new AbortController();
  const turn = newTurn(role);
  set({
    items: [...get().items, { kind: 'user', key: itemKey('u'), text: message, chips: chips.map((c) => c.label) }, turn],
    running: ac,
    draft: '',
    announce: 'The assistant is answering',
  });
  const context = chips
    .filter((c) => c.kind !== 'database' && c.ref.trim())
    .map((c) => ({ kind: c.kind as api.ContextKind, ref: c.ref }));
  const conv = get().conversation;
  let proposals = 0;
  try {
    await api.chat(
      {
        role,
        message,
        ...(conv ? { conversation: conv } : {}),
        ...(database ? { database } : {}),
        ...(context.length ? { context } : {}),
      },
      (e) => {
        if (e.type === 'start') {
          set({ conversation: e.conversation ?? get().conversation, shareData: e.share_data });
        }
        if (e.type === 'proposal') {
          proposals += 1;
          set({ announce: 'A proposal is ready to review' });
        }
        patchTurn(turn.key, (t) => applyEvent(t, e));
      },
      ac.signal,
    );
    patchTurn(turn.key, (t) => (t.status === 'streaming' ? { ...t, status: 'done' } : t));
    const done = get().items.find((i) => i.key === turn.key) as Turn | undefined;
    set({
      announce:
        done?.status === 'failed'
          ? `The answer failed: ${done.error?.message ?? 'an error'}`
          : proposals
            ? `Answer finished, with ${proposals} proposal${proposals === 1 ? '' : 's'} to review`
            : 'Answer finished',
    });
  } catch (e) {
    if (ac.signal.aborted) {
      patchTurn(turn.key, (t) => ({ ...t, status: 'stopped' }));
      set({ announce: 'Stopped' });
    } else {
      const err: ErrorBody = isApiError(e)
        ? { code: e.code, name: e.errorName, message: e.message, trace_id: e.traceId }
        : { message: e instanceof Error ? e.message : String(e) };
      patchTurn(turn.key, (t) => ({ ...t, status: 'failed', error: err }));
      if (err.code && SETUP_CODES.has(err.code) && role !== 'admin') set({ unavailable: err });
      // The question stays editable after a failure.
      if (!get().draft) set({ draft: message });
      set({ announce: `The answer failed: ${err.message ?? 'an error'}` });
    }
  } finally {
    if (get().running === ac) set({ running: null });
  }
}

/** Stops the streaming turn (closing the stream stops it in the gateway). */
export function stop(): void {
  get().running?.abort();
}

/** The stored id of a turn's answer: the stream's end line names it; for
 * a stream without it (an older gateway), it is looked up in the stored
 * conversation by the question's position. */
async function messageIdOf(turn: Turn): Promise<string | undefined> {
  if (turn.messageId) return turn.messageId;
  const conv = get().conversation;
  if (!conv) return undefined;
  const items = get().items;
  const at = items.findIndex((i) => i.key === turn.key);
  const n = questions(items.slice(0, at)) - 1;
  const c = await api.conversation(conv, false);
  const id = answerId(c.messages, n);
  if (id) patchTurn(turn.key, (t) => ({ ...t, messageId: id }));
  return id;
}

/** 👍 or 👎 on an answer, with an optional note. */
export async function rate(turnKey: string, v: 'up' | 'down', note?: string): Promise<void> {
  const turn = get().items.find((i) => i.kind === 'turn' && i.key === turnKey) as Turn | undefined;
  if (!turn) return;
  const id = await messageIdOf(turn);
  if (!id) throw new Error('This answer isn’t stored, so it can’t be rated.');
  await api.vote(id, v, note);
  patchTurn(turnKey, (t) => ({ ...t, vote: v }));
  set({ announce: 'Thanks: feedback sent' });
}

type ProposalBlock = Extract<Block, { type: 'proposal' }>;

function patchProposal(id: string, patch: Partial<ProposalBlock>): void {
  set({ items: updateProposal(get().items, id, patch) });
}

/** Accepts a proposal after the user confirmed it: the gateway runs the
 * statement as the user, files the advice or accepts the term. */
export async function acceptProposal(id: string): Promise<api.ProposalOutcome | null> {
  patchProposal(id, { state: 'running', error: undefined });
  try {
    const r = await api.accept(id, true);
    const failed = r.outcome === 'failed';
    const err = failed ? errorMessage(r.error) : undefined;
    patchProposal(id, { state: failed ? 'failed' : 'accepted', outcome: r, error: err });
    set({ announce: failed ? `It failed: ${err ?? ''}` : 'Done: the proposal was applied' });
    return r;
  } catch (e) {
    const msg = isApiError(e) ? e.describe() : e instanceof Error ? e.message : String(e);
    patchProposal(id, { state: 'failed', error: msg });
    set({ announce: `It failed: ${msg}` });
    return null;
  }
}

function errorMessage(v: unknown): string | undefined {
  if (v && typeof v === 'object' && 'message' in v) {
    const o = v as { code?: number; name?: string; message?: string };
    return `${o.code ? `${o.code}${o.name ? ` ${o.name}` : ''}: ` : ''}${o.message ?? ''}`;
  }
  return undefined;
}

export async function dismissProposal(id: string): Promise<void> {
  try {
    await api.dismiss(id);
  } catch {
    // Dismissing is a note to the assistants; the card goes either way.
  }
  patchProposal(id, { state: 'dismissed' });
  set({ announce: 'Dismissed' });
}

/** Records that a proposal went into the editor (nothing ran). */
export function usedInEditor(id: string): void {
  void api.outcome(id, 'accepted', 'opened in a query tab, not run').catch(() => undefined);
}

/** Reopens a stored conversation. */
export async function openConversation(id: string, role: string): Promise<void> {
  get().running?.abort();
  const c = await api.conversation(id, false);
  const r: AiRole = role === 'dba' || role === 'admin' ? role : 'data';
  set({ role: r, conversation: id, items: rebuild(r, c.messages), view: 'chat', running: null, announce: 'Conversation opened' });
}

/** At sign-out and tenant switch: forget everything. */
export function resetAssistant(): void {
  get().running?.abort();
  set({ ...initial, role: 'data' });
}

if (typeof window !== 'undefined') {
  window.addEventListener('sprig-signed-out', resetAssistant);
  window.addEventListener('sprig-identity-changed', resetAssistant);
  window.addEventListener('sprig-assistant-toggle', toggleDock);
}
