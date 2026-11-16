import { describe, expect, it } from 'vitest';
import {
  answerId,
  applyEvent,
  isRead,
  newTurn,
  proposalSql,
  questionOf,
  rebuild,
  sentCalls,
  toolLabel,
  updateProposal,
  type ChatEvent,
  type StoredMessage,
  type Turn,
} from './thread';

const fold = (events: ChatEvent[]): Turn => events.reduce(applyEvent, newTurn('data'));

describe('applyEvent', () => {
  it('folds a streamed turn into text, tool rows, a result, a proposal and sources', () => {
    const t = fold([
      { type: 'start', conversation: '12', role: 'dba', model: 'claude-opus-5-5', share_data: false },
      { type: 'text', text: 'Let me ' },
      { type: 'text', text: 'look.' },
      { type: 'tool_start', id: 'c1', name: 'run_query', input: { db: 'shop', sql: 'SELECT 1' } },
      { type: 'tool_end', id: 'c1', name: 'run_query', ok: true, reduced: true },
      { type: 'result', id: 'c1', name: 'run_query', result: { columns: [{ name: 'x', type: 'BIGINT' }], rows: [[1]], row_count: 1 } },
      { type: 'text', text: '\n\nDone.' },
      { type: 'proposal', proposal: { id: 'p1', kind: 'sql', body: { db: 'shop', sql: 'CREATE INDEX i ON t (a)' } }, filed: null },
      // A repeated proposal line is ignored.
      { type: 'proposal', proposal: { id: 'p1', kind: 'sql', body: {} }, filed: null },
      { type: 'usage', usage: { input_tokens: 100, output_tokens: 20, cache_write_tokens: 5 } },
      { type: 'sources', sources: [{ kind: 'docs', title: 'Indexes', reference: 'tools/indexes.md' }] },
      { type: 'sources', sources: [{ kind: 'docs', title: 'Indexes', reference: 'tools/indexes.md' }] },
      { type: 'end', stop: 'end_turn' },
    ]);
    expect(t.role).toBe('dba');
    expect(t.model).toBe('claude-opus-5-5');
    expect(t.shareData).toBe(false);
    expect(t.status).toBe('done');
    expect(t.blocks.map((b) => b.type)).toEqual(['text', 'tool', 'result', 'text', 'proposal']);
    expect(t.blocks[0]).toEqual({ type: 'text', text: 'Let me look.' });
    expect(t.blocks[1]).toMatchObject({ ok: true, reduced: true });
    expect(t.blocks[2]).toMatchObject({ sql: 'SELECT 1', db: 'shop' });
    expect(t.usage).toEqual({ input: 105, output: 20 });
    expect(t.sources).toHaveLength(1);
  });

  it('marks errors, refusals and stops', () => {
    const failed = fold([
      { type: 'text', text: 'x' },
      { type: 'error', error: { code: 39994, name: 'ER_SPRIG_AI_REFUSED', message: 'refused' } },
      { type: 'end', stop: 'refusal' },
    ]);
    expect(failed.status).toBe('failed');
    expect(failed.error?.code).toBe(39994);
    const stopped = fold([{ type: 'end', stop: 'other', detail: 'stopped' }]);
    expect(stopped.status).toBe('stopped');
  });

  it('updates one proposal in place', () => {
    const t = fold([{ type: 'proposal', proposal: { id: 'p9', kind: 'sql', body: {} }, filed: null }]);
    const items = updateProposal([t], 'p9', { state: 'accepted' });
    const b = (items[0] as Turn).blocks[0];
    expect(b?.type === 'proposal' && b.state).toBe('accepted');
  });
});

const stored: StoredMessage[] = [
  { id: '1', kind: 'user', body: { text: "why slow?\n\n[The query in the user's editor]\nSELECT * FROM t" } },
  { id: '2', kind: 'system_note', body: { sha256: 'ab', provider: 'anthropic', model: 'm', usage: { input_tokens: 10, output_tokens: 2 }, payload: { a: 1 } } },
  { id: '3', kind: 'assistant', body: { role: 'assistant', content: [{ type: 'text', text: 'Checking.' }, { type: 'tool_use', id: 'c', name: 'explain', input: {} }] } },
  { id: '4', kind: 'tool_call', body: { id: 'c', name: 'explain', input: { db: 'shop', sql: 'SELECT * FROM t' } } },
  { id: '5', kind: 'tool_result', body: { id: 'c', name: 'explain', ok: true, reduced: false } },
  { id: '6', kind: 'system_note', body: { sha256: 'cd', provider: 'anthropic', model: 'm' } },
  { id: '7', kind: 'assistant', body: { role: 'assistant', content: [{ type: 'text', text: 'Add an index.' }] } },
  { id: '8', kind: 'proposal', body: { id: 'p', kind: 'sql', body: { db: 'shop', sql: 'CREATE INDEX i ON t (a)' } } },
  { id: '9', kind: 'user', body: { text: 'thanks' } },
  { id: '10', kind: 'assistant', body: { role: 'assistant', content: [{ type: 'text', text: 'Welcome.' }] } },
];

describe('stored conversations', () => {
  it('rebuilds questions and answers, without the appended context', () => {
    const items = rebuild('dba', stored);
    expect(items.map((i) => i.kind)).toEqual(['user', 'turn', 'user', 'turn']);
    expect(items[0]).toMatchObject({ text: 'why slow?', chips: ['query'] });
    const t = items[1] as Turn;
    expect(t.blocks.map((b) => b.type)).toEqual(['text', 'tool', 'text', 'proposal']);
    expect(t.blocks[2]).toEqual({ type: 'text', text: 'Add an index.' });
    expect(t.messageId).toBe('7');
    expect(t.usage.input).toBe(10);
  });

  it('finds the answer to the nth question', () => {
    expect(answerId(stored, 0)).toBe('7');
    expect(answerId(stored, 1)).toBe('10');
    expect(answerId(stored, 2)).toBeUndefined();
  });

  it('lists what was sent, call by call', () => {
    const calls = sentCalls(stored);
    expect(calls).toHaveLength(2);
    expect(calls[0]).toMatchObject({ question: 'why slow?', sha256: 'ab', payload: { a: 1 } });
    expect(calls[1]!.payload).toBeUndefined();
  });

  it('strips context labels from a question', () => {
    expect(questionOf('a\n\n[A note from the user]\nb')).toBe('a');
    expect(questionOf('plain')).toBe('plain');
  });
});

describe('helpers', () => {
  it('labels tool rows quietly', () => {
    expect(toolLabel('describe', { db: 'shop', table: 'orders' })).toEqual({ what: 'Described', about: 'shop.orders' });
    expect(toolLabel('search_docs', { query: 'automatic indexing' }).what).toBe('Searched docs');
    expect(toolLabel('new_tool', {}).what).toBe('new tool');
  });

  it('finds what a proposal runs', () => {
    expect(proposalSql({ id: 'a', kind: 'sql', body: { db: 'shop', sql: 'SELECT 1' } })).toEqual({ sql: 'SELECT 1', db: 'shop' });
    expect(proposalSql({ id: 'b', kind: 'generate_rows', body: { db: 'shop' }, generated: { sql: 'INSERT …' } }).sql).toBe('INSERT …');
  });

  it('tells reads from changes', () => {
    expect(isRead('SELECT 1')).toBe(true);
    expect(isRead('  -- note\nWITH x AS (SELECT 1) SELECT * FROM x;')).toBe(true);
    expect(isRead('DELETE FROM t')).toBe(false);
    expect(isRead('SELECT 1; DROP TABLE t')).toBe(false);
  });
});
