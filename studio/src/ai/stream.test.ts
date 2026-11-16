import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { readEvents } from './api';
import { useAssistant, send, stop, newConversation, resetAssistant, rate, acceptProposal } from './store';
import type { ChatEvent, Turn } from './thread';

/** A stream that delivers `text` in chunks of `size` bytes (lines and
 * UTF-8 sequences split across chunks). */
function chunked(text: string, size: number, hold = false): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text);
  let at = 0;
  return new ReadableStream({
    pull(c) {
      if (at >= bytes.length) {
        if (!hold) c.close();
        return;
      }
      c.enqueue(bytes.slice(at, at + size));
      at += size;
    },
  });
}

const lines = (evs: unknown[]) => evs.map((e) => JSON.stringify(e)).join('\n') + '\n';

const TURN: unknown[] = [
  { type: 'start', conversation: '41', role: 'data', model: 'stub-model', share_data: false },
  { type: 'text', text: 'Revenue é ' },
  { type: 'tool_start', id: 't1', name: 'run_query', input: { db: 'shop', sql: 'SELECT 1' } },
  { type: 'tool_end', id: 't1', name: 'run_query', ok: true, reduced: true },
  { type: 'result', id: 't1', name: 'run_query', result: { columns: [{ name: 'n', type: 'BIGINT' }], rows: [['1']], row_count: 1 } },
  { type: 'some_future_event', x: 1 },
  { type: 'proposal', proposal: { id: 'p1', kind: 'sql', body: { db: 'shop', sql: 'CREATE INDEX i ON t (a)', why: 'faster' } }, filed: null },
  { type: 'sources', sources: [{ kind: 'docs', title: 'Indexes', reference: 'tools/indexes.md' }] },
  { type: 'end', stop: 'end_turn' },
];

describe('readEvents', () => {
  it('reads events split anywhere, skipping unknown types', async () => {
    for (const size of [1, 3, 7, 64, 4096]) {
      const got: ChatEvent[] = [];
      await readEvents(chunked(lines(TURN), size), (e) => got.push(e));
      expect(got.map((e) => e.type)).toEqual(['start', 'text', 'tool_start', 'tool_end', 'result', 'proposal', 'sources', 'end']);
      expect(got[1]).toEqual({ type: 'text', text: 'Revenue é ' });
    }
  });

  it('reads a last line without a newline', async () => {
    const got: ChatEvent[] = [];
    await readEvents(chunked('{"type":"text","text":"a"}\n{"type":"end","stop":"end_turn"}', 5), (e) => got.push(e));
    expect(got.map((e) => e.type)).toEqual(['text', 'end']);
  });
});

describe('the dock store', () => {
  const calls: { url: string; body: unknown }[] = [];
  beforeEach(() => {
    resetAssistant();
    calls.length = 0;
  });
  afterEach(() => vi.unstubAllGlobals());

  const stubFetch = (respond: (url: string, body: unknown, signal?: AbortSignal | null) => Response) =>
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string, init: RequestInit) => {
        const body: unknown = typeof init.body === 'string' ? JSON.parse(init.body) : undefined;
        calls.push({ url, body });
        return Promise.resolve(respond(url, body, init.signal));
      }),
    );

  it('streams a turn: chips as context, the conversation kept, then feedback and accept', async () => {
    stubFetch((url) => {
      if (url === '/v1/ai/chat') return new Response(chunked(lines(TURN), 9), { status: 200, headers: { 'content-type': 'application/x-ndjson' } });
      if (url.startsWith('/v1/ai/conversations/41'))
        return Response.json({
          conversation: { id: '41', role: 'data', title: 'q' },
          messages: [
            { id: '100', kind: 'user', body: { text: 'q' } },
            { id: '101', kind: 'assistant', body: { content: [{ type: 'text', text: 'Revenue' }] } },
          ],
        });
      if (url === '/v1/ai/feedback') return Response.json({ recorded: true });
      if (url === '/v1/ai/proposals/p1:accept')
        return Response.json({ proposal: 'p1', kind: 'sql', outcome: 'accepted', ran: [{ sql: 'CREATE INDEX i ON t (a)', results: [{ affected_rows: '0' }] }] });
      return new Response('{}', { status: 404 });
    });
    await send(
      'How much revenue?',
      [
        { id: 'db:shop', kind: 'database', label: 'shop', ref: 'shop' },
        { id: 'query:1', kind: 'query', label: 'Query 1', ref: 'SELECT 2' },
      ],
      'shop',
    );
    const s = useAssistant.getState();
    expect(calls[0]!.body).toEqual({
      role: 'data',
      message: 'How much revenue?',
      database: 'shop',
      context: [{ kind: 'query', ref: 'SELECT 2' }],
    });
    expect(s.conversation).toBe('41');
    expect(s.shareData).toBe(false);
    expect(s.running).toBeNull();
    expect(s.items.map((i) => i.kind)).toEqual(['user', 'turn']);
    const t = s.items[1] as Turn;
    expect(t.status).toBe('done');
    expect(t.blocks.map((b) => b.type)).toEqual(['text', 'tool', 'result', 'proposal']);
    expect(s.announce).toMatch(/1 proposal to review/);

    // The next question continues the conversation.
    await send('And last week?', [], null);
    expect(calls.find((c, i) => i > 0 && c.url === '/v1/ai/chat')!.body).toMatchObject({ conversation: '41' });

    // 👍 looks the answer's id up once, then sends the vote.
    await rate(t.key, 'up');
    expect(calls.find((c) => c.url === '/v1/ai/feedback')!.body).toEqual({ message_id: '101', vote: 'up' });

    const r = await acceptProposal('p1');
    expect(r?.outcome).toBe('accepted');
    expect(calls.find((c) => c.url === '/v1/ai/proposals/p1:accept')!.body).toEqual({ confirm: true });
    const b = (useAssistant.getState().items[1] as Turn).blocks.find((x) => x.type === 'proposal');
    expect(b && b.type === 'proposal' && b.state).toBe('accepted');
  });

  it('remembers that the assistants are not set up (39990) and keeps the question', async () => {
    stubFetch(() => Response.json({ code: 39990, name: 'ER_SPRIG_AI_NOT_CONFIGURED', message: 'no model for data' }, { status: 503 }));
    await send('hello', [], null);
    const s = useAssistant.getState();
    expect(s.unavailable?.code).toBe(39990);
    expect((s.items[1] as Turn).status).toBe('failed');
    expect(s.draft).toBe('hello');
  });

  it('stops a streaming turn by closing the stream', async () => {
    // As a browser does: aborting the fetch errors its body.
    stubFetch((_url, _b, signal) => {
      const body = new ReadableStream<Uint8Array>({
        start(c) {
          c.enqueue(new TextEncoder().encode(lines([TURN[0], TURN[1]])));
          signal?.addEventListener('abort', () => c.error(new DOMException('Aborted', 'AbortError')));
        },
      });
      return new Response(body, { status: 200 });
    });
    const p = send('long question', [], null);
    await new Promise((r) => setTimeout(r, 20));
    expect(useAssistant.getState().running).not.toBeNull();
    stop();
    await p;
    const t = useAssistant.getState().items[1] as Turn;
    expect(t.status).toBe('stopped');
    expect(useAssistant.getState().running).toBeNull();
    newConversation();
    expect(useAssistant.getState().items).toEqual([]);
  });
});
