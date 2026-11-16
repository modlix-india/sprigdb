// The assistant's chat as Studio keeps it: the NDJSON events of
// POST /v1/ai/chat folded into turns of blocks (text, tool rows, results,
// proposals), and a stored conversation (GET /v1/ai/conversations/{id})
// rebuilt into the same shape. Pure, so it is tested without a browser.

import type { ErrorBody } from '../api/errors';

export type AiRole = 'data' | 'dba' | 'admin';

export interface AiColumn {
  name: string;
  type: string;
}

/** A run_query result, whole, for the user only. */
export interface AiResult {
  columns: AiColumn[];
  rows: unknown[][];
  row_count: number;
  truncated?: boolean;
}

export type ProposalKind = 'sql' | 'recommendation' | 'tile' | 'glossary' | 'generate_rows';

export interface Proposal {
  id: string;
  kind: ProposalKind;
  tool_use_id?: string;
  /** The tool's input, as the model gave it. */
  body: Record<string, unknown>;
  /** What SprigDB checked before showing it: counts, compile results. */
  checks?: unknown;
  /** What only the user gets: generated statements (test rows). */
  generated?: unknown;
}

export interface Source {
  kind: string;
  title: string;
  reference: string;
}

export interface Usage {
  input_tokens?: number;
  output_tokens?: number;
  cache_read_tokens?: number;
  cache_write_tokens?: number;
}

/** One line of the chat stream. */
export type ChatEvent =
  | { type: 'start'; conversation: string | null; role: string; model: string; share_data: boolean }
  | { type: 'text'; text: string }
  | { type: 'thinking'; text: string }
  | { type: 'tool_start'; id: string; name: string; input: Record<string, unknown> }
  | { type: 'tool_end'; id: string; name: string; ok: boolean; reduced: boolean }
  | { type: 'result'; id: string; name: string; result: AiResult }
  | { type: 'proposal'; proposal: Proposal; filed: string | null }
  | { type: 'usage'; usage: Usage }
  | { type: 'sources'; sources: Source[] }
  | { type: 'error'; error: ErrorBody }
  | { type: 'end'; stop: string; detail?: string };

export type ProposalState = 'shown' | 'running' | 'accepted' | 'failed' | 'dismissed';

export type Block =
  | { type: 'text'; text: string }
  | { type: 'thinking'; text: string }
  | {
      type: 'tool';
      id: string;
      name: string;
      input: Record<string, unknown>;
      /** Undefined while it runs. */
      ok?: boolean;
      /** Rows or values were withheld from the provider. */
      reduced?: boolean;
    }
  | { type: 'result'; id: string; name: string; result: AiResult; sql?: string; db?: string }
  | {
      type: 'proposal';
      proposal: Proposal;
      filed: string | null;
      state: ProposalState;
      /** What accepting it did (the accept route's reply), or why it failed. */
      outcome?: unknown;
      error?: string;
    };

export type TurnStatus = 'streaming' | 'done' | 'stopped' | 'failed';

export interface Turn {
  kind: 'turn';
  key: string;
  role: AiRole;
  model?: string;
  shareData?: boolean;
  blocks: Block[];
  sources: Source[];
  usage: { input: number; output: number };
  error?: ErrorBody;
  stop?: string;
  detail?: string;
  status: TurnStatus;
  /** The answer's message id, for feedback (known once stored). */
  messageId?: string;
  vote?: 'up' | 'down';
}

export interface UserItem {
  kind: 'user';
  key: string;
  text: string;
  /** The context chips sent with it, as labels. */
  chips: string[];
}

export type Item = UserItem | Turn;

let seq = 0;
/** A key for a new item. */
export function itemKey(prefix: string): string {
  seq += 1;
  return `${prefix}-${Date.now().toString(36)}-${seq}`;
}

export function newTurn(role: AiRole): Turn {
  return { kind: 'turn', key: itemKey('t'), role, blocks: [], sources: [], usage: { input: 0, output: 0 }, status: 'streaming' };
}

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** A parsed line as a chat event, or null for one Studio doesn't know. */
export function asEvent(v: unknown): ChatEvent | null {
  if (!isObj(v) || typeof v.type !== 'string') return null;
  switch (v.type) {
    case 'start':
    case 'text':
    case 'thinking':
    case 'tool_start':
    case 'tool_end':
    case 'result':
    case 'proposal':
    case 'usage':
    case 'sources':
    case 'error':
    case 'end':
      return v as unknown as ChatEvent;
    default:
      return null;
  }
}

function appendText(blocks: Block[], type: 'text' | 'thinking', text: string): Block[] {
  const last = blocks[blocks.length - 1];
  if (last && last.type === type) return [...blocks.slice(0, -1), { type, text: last.text + text }];
  return [...blocks, { type, text }];
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' ? v : undefined;
}

/** `t` after one event. */
export function applyEvent(t: Turn, e: ChatEvent): Turn {
  switch (e.type) {
    case 'start':
      return {
        ...t,
        model: e.model,
        shareData: e.share_data,
        role: e.role === 'dba' || e.role === 'admin' || e.role === 'data' ? e.role : t.role,
      };
    case 'text':
      return e.text ? { ...t, blocks: appendText(t.blocks, 'text', e.text) } : t;
    case 'thinking':
      return e.text ? { ...t, blocks: appendText(t.blocks, 'thinking', e.text) } : t;
    case 'tool_start':
      return { ...t, blocks: [...t.blocks, { type: 'tool', id: e.id, name: e.name, input: isObj(e.input) ? e.input : {} }] };
    case 'tool_end':
      return {
        ...t,
        blocks: t.blocks.map((b) => (b.type === 'tool' && b.id === e.id ? { ...b, ok: e.ok, reduced: e.reduced } : b)),
      };
    case 'result': {
      const call = t.blocks.find((b) => b.type === 'tool' && b.id === e.id);
      const input = call && call.type === 'tool' ? call.input : {};
      return {
        ...t,
        blocks: [...t.blocks, { type: 'result', id: e.id, name: e.name, result: e.result, sql: str(input.sql), db: str(input.db) }],
      };
    }
    case 'proposal':
      if (t.blocks.some((b) => b.type === 'proposal' && b.proposal.id === e.proposal.id)) return t;
      return { ...t, blocks: [...t.blocks, { type: 'proposal', proposal: e.proposal, filed: e.filed ?? null, state: 'shown' }] };
    case 'usage':
      return {
        ...t,
        usage: {
          input: t.usage.input + (e.usage.input_tokens ?? 0) + (e.usage.cache_write_tokens ?? 0),
          output: t.usage.output + (e.usage.output_tokens ?? 0),
        },
      };
    case 'sources': {
      const seen = new Set(t.sources.map((s) => `${s.kind}\u0000${s.reference}`));
      const add = (e.sources ?? []).filter((s) => {
        const k = `${s.kind}\u0000${s.reference}`;
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      });
      return add.length ? { ...t, sources: [...t.sources, ...add] } : t;
    }
    case 'error':
      return { ...t, error: e.error, status: 'failed' };
    case 'end':
      return {
        ...t,
        stop: e.stop,
        detail: e.detail,
        status: t.status === 'failed' ? 'failed' : e.detail === 'stopped' ? 'stopped' : 'done',
      };
  }
}

/** Updates the proposal `id` in `items`. */
export function updateProposal(
  items: Item[],
  id: string,
  patch: Partial<Extract<Block, { type: 'proposal' }>>,
): Item[] {
  return items.map((it) =>
    it.kind !== 'turn' || !it.blocks.some((b) => b.type === 'proposal' && b.proposal.id === id)
      ? it
      : { ...it, blocks: it.blocks.map((b) => (b.type === 'proposal' && b.proposal.id === id ? { ...b, ...patch } : b)) },
  );
}

// ---- Stored conversations ----

export interface StoredMessage {
  id: string;
  kind: 'user' | 'assistant' | 'tool_call' | 'tool_result' | 'proposal' | 'system_note';
  body: Record<string, unknown>;
  sent_hash?: string | null;
  created_ms?: number;
}

const CONTEXT_LABELS = [
  "The query in the user's editor",
  'The table the user is looking at',
  'A note from the user',
  'Rows the user has on screen',
];

/** A stored user message without the context the client appended. */
export function questionOf(text: string): string {
  let cut = text.length;
  for (const l of CONTEXT_LABELS) {
    const at = text.indexOf(`\n\n[${l}]\n`);
    if (at >= 0 && at < cut) cut = at;
  }
  return text.slice(0, cut);
}

/** The context labels a stored user message carried. */
function chipsOf(text: string): string[] {
  const out: string[] = [];
  if (text.includes("\n\n[The query in the user's editor]\n")) out.push('query');
  if (text.includes('\n\n[The table the user is looking at]\n')) out.push('table');
  if (text.includes('\n\n[A note from the user]\n')) out.push('note');
  if (text.includes('\n\n[Rows the user has on screen]\n')) out.push('result');
  return out;
}

/** A stored conversation as items, for reopening it. Rows shown to the
 * user aren't stored, so results come back as their query only. */
export function rebuild(role: AiRole, messages: StoredMessage[]): Item[] {
  const items: Item[] = [];
  let turn: Turn | null = null;
  const close = () => {
    if (turn) {
      items.push(turn.status === 'streaming' ? { ...turn, status: 'done' } : turn);
      turn = null;
    }
  };
  const cur = (): Turn => {
    if (!turn) turn = { ...newTurn(role), key: itemKey('t') };
    return turn;
  };
  for (const m of messages) {
    const b = m.body ?? {};
    switch (m.kind) {
      case 'user': {
        close();
        const text = typeof b.text === 'string' ? b.text : '';
        items.push({ kind: 'user', key: `u-${m.id}`, text: questionOf(text), chips: chipsOf(text) });
        break;
      }
      case 'system_note': {
        const t = cur();
        if (typeof b.model === 'string') t.model = b.model;
        if (isObj(b.usage)) {
          const u = b.usage as Usage;
          t.usage = {
            input: t.usage.input + (u.input_tokens ?? 0) + (u.cache_write_tokens ?? 0),
            output: t.usage.output + (u.output_tokens ?? 0),
          };
        }
        if (typeof b.error === 'string') {
          t.error = { message: b.error, code: typeof b.code === 'number' ? b.code : undefined };
          t.status = 'failed';
        }
        break;
      }
      case 'assistant': {
        const t = cur();
        const content = Array.isArray(b.content) ? (b.content as unknown[]) : [];
        for (const c of content) {
          if (!isObj(c)) continue;
          if (c.type === 'text' && typeof c.text === 'string' && c.text) {
            const prev = t.blocks[t.blocks.length - 1];
            t.blocks = appendText(t.blocks, 'text', prev?.type === 'text' ? `\n\n${c.text}` : c.text);
          }
        }
        t.messageId = m.id;
        break;
      }
      case 'tool_call': {
        const t = cur();
        t.blocks = [
          ...t.blocks,
          { type: 'tool', id: str(b.id) ?? '', name: str(b.name) ?? '', input: isObj(b.input) ? b.input : {} },
        ];
        break;
      }
      case 'tool_result': {
        const t = cur();
        t.blocks = t.blocks.map((x) =>
          x.type === 'tool' && x.id === (str(b.id) ?? '') ? { ...x, ok: b.ok !== false, reduced: b.reduced === true } : x,
        );
        break;
      }
      case 'proposal': {
        const t = cur();
        if (typeof b.id === 'string' && typeof b.kind === 'string') {
          t.blocks = [...t.blocks, { type: 'proposal', proposal: b as unknown as Proposal, filed: null, state: 'shown' }];
        }
        break;
      }
    }
  }
  close();
  return items;
}

/** The id of the answer to the `n`th question (0-based) of a stored
 * conversation: its last assistant message before the next question. */
export function answerId(messages: StoredMessage[], n: number): string | undefined {
  let q = -1;
  let found: string | undefined;
  for (const m of messages) {
    if (m.kind === 'user') {
      if (q === n) break;
      q += 1;
    } else if (m.kind === 'assistant' && q === n) {
      found = m.id;
    }
  }
  return found;
}

/** One provider call of a stored conversation: "What was sent". */
export interface SentCall {
  /** The question it answered. */
  question: string;
  provider?: string;
  model?: string;
  sha256?: string;
  usage?: Usage;
  stop?: unknown;
  /** The exact body, while ai.audit_payload_days keeps it. */
  payload?: unknown;
  error?: string;
}

/** What a stored conversation sent to the provider, call by call. */
export function sentCalls(messages: StoredMessage[]): SentCall[] {
  const out: SentCall[] = [];
  let question = '';
  for (const m of messages) {
    const b = m.body ?? {};
    if (m.kind === 'user') question = questionOf(typeof b.text === 'string' ? b.text : '');
    if (m.kind !== 'system_note') continue;
    out.push({
      question,
      provider: str(b.provider),
      model: str(b.model),
      sha256: str(b.sha256) ?? m.sent_hash ?? undefined,
      usage: isObj(b.usage) ? b.usage : undefined,
      stop: b.stop,
      payload: b.payload,
      error: str(b.error),
    });
  }
  return out;
}

// ---- Tool rows ----

const TOOL_LABELS: Record<string, string> = {
  search_docs: 'Searched docs',
  catalog: 'Read catalog',
  describe: 'Described',
  relationships: 'Read relationships',
  workload: 'Read workload',
  fingerprint: 'Read a statement’s workload',
  explain: 'Explained',
  indexes: 'Checked indexes',
  auto_indexes: 'Checked automatic indexes',
  settings: 'Read settings',
  shards: 'Read shards',
  advisor_runs: 'Read advisor runs',
  approvals: 'Read approvals',
  column_stats: 'Read column statistics',
  run_query: 'Ran read-only query',
  test_policy: 'Tried a policy',
  compile_logic: 'Compiled logic',
  sensitive_columns: 'Scanned for sensitive data',
  check_ddl: 'Checked schema statements',
  migration_check: 'Checked a migration',
  decision_log: 'Read the decision log',
  metrics_trend: 'Read metric history',
  propose_sql: 'Proposed a statement',
  propose_recommendation: 'Proposed a recommendation',
  propose_tile: 'Proposed a chart',
  propose_glossary: 'Proposed a glossary term',
  generate_rows: 'Generated test rows',
  propose_policy: 'Proposed a policy',
  propose_logic: 'Proposed logic',
};

/** A tool call as a quiet row: what it did, and what it was about. */
export function toolLabel(name: string, input: Record<string, unknown>): { what: string; about: string } {
  const what = TOOL_LABELS[name] ?? name.replace(/_/g, ' ');
  const parts: string[] = [];
  const db = str(input.db);
  const table = str(input.table);
  if (table) parts.push(db ? `${db}.${table}` : table);
  else if (db) parts.push(db);
  const q = str(input.query) ?? str(input.subject) ?? str(input.term) ?? str(input.title) ?? str(input.key);
  if (q) parts.push(q.length > 60 ? `${q.slice(0, 60)}…` : q);
  return { what, about: parts.join(' · ') };
}

/** The statement a proposal runs, and where. */
export function proposalSql(p: Proposal): { sql: string; db: string | null } {
  const b = p.body ?? {};
  if (p.kind === 'generate_rows') {
    const g = isObj(p.generated) ? p.generated : {};
    return { sql: str(g.sql) ?? '', db: str(b.db) ?? null };
  }
  return { sql: str(b.sql) ?? '', db: str(b.db) ?? null };
}

/** Whether a statement is a plain read (the accept route runs those
 * without `confirm`). Studio confirms every proposal anyway. */
export function isRead(sql: string): boolean {
  const s = sql.replace(/^(\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, '').toUpperCase();
  return /^(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN|WITH)\b/.test(s) && !/;\s*\S/.test(sql.trim().replace(/;\s*$/, ''));
}
