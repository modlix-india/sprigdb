// The assistants' routes, through the one call wrapper (token, trace,
// refresh). The chat is NDJSON: one event per line, read as it arrives;
// closing the stream (abort) stops the turn in the gateway.

import { json, request } from '../api/client';
import { NdjsonDecoder } from '../api/ndjson';
import type { Schemas } from '../api/types';
import { asEvent, type AiRole, type ChatEvent, type StoredMessage } from './thread';

export type ContextKind = 'query' | 'table' | 'note' | 'result';

export interface ChatRequest {
  role: AiRole;
  message: string;
  conversation?: string;
  database?: string;
  context?: { kind: ContextKind; ref: string }[];
}

/** Reads an NDJSON body, calling `on` for each event it knows. */
export async function readEvents(body: ReadableStream<Uint8Array>, on: (e: ChatEvent) => void): Promise<void> {
  const reader = body.getReader();
  const dec = new NdjsonDecoder();
  const emit = (vs: unknown[]) => {
    for (const v of vs) {
      const e = asEvent(v);
      if (e) on(e);
    }
  };
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) emit(dec.push(value));
    }
    emit(dec.end());
  } finally {
    reader.releaseLock();
  }
}

/** One turn. Errors before the stream starts (39990, 39991, 39992, 39995, a
 * role the caller may not use) reject; later ones arrive as `error` events. */
export async function chat(req: ChatRequest, on: (e: ChatEvent) => void, signal?: AbortSignal): Promise<void> {
  const r = await request('/v1/ai/chat', { method: 'POST', body: req, accept: 'application/x-ndjson', signal });
  if (!r.body) {
    const text = await r.text();
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      const e = asEvent(JSON.parse(line));
      if (e) on(e);
    }
    return;
  }
  await readEvents(r.body, on);
}

export type ConversationHeader = Schemas['AiConversationHeader'];

export interface Conversations {
  conversations: ConversationHeader[];
  admin_unavailable?: string;
}

export function conversations(limit = 50): Promise<Conversations> {
  return json<Conversations>(`/v1/ai/conversations?limit=${limit}`);
}

export interface Conversation {
  conversation: ConversationHeader;
  messages: StoredMessage[];
}

export function conversation(id: string, payloads = false): Promise<Conversation> {
  return json<Conversation>(`/v1/ai/conversations/${encodeURIComponent(id)}?payloads=${payloads}`);
}

export function vote(messageId: string, v: 'up' | 'down', note?: string): Promise<unknown> {
  return json('/v1/ai/feedback', { method: 'POST', body: { message_id: messageId, vote: v, ...(note ? { note } : {}) } });
}

export function outcome(proposalId: string, o: 'accepted' | 'dismissed' | 'failed', note?: string): Promise<unknown> {
  return json('/v1/ai/feedback', { method: 'POST', body: { proposal_id: proposalId, outcome: o, ...(note ? { note } : {}) } });
}

export type ProposalOutcome = Schemas['AiProposalOutcome'];

export function accept(id: string, confirm: boolean): Promise<ProposalOutcome> {
  return json<ProposalOutcome>(`/v1/ai/proposals/${encodeURIComponent(id)}:accept`, { method: 'POST', body: { confirm } });
}

export function dismiss(id: string, note?: string): Promise<ProposalOutcome> {
  return json<ProposalOutcome>(`/v1/ai/proposals/${encodeURIComponent(id)}:dismiss`, {
    method: 'POST',
    body: note ? { note } : {},
  });
}

// ---- Glossary and digests ----

export type Glossary = Schemas['AiGlossary'];
export type GlossaryTerm = Glossary['terms'][number];

export function glossary(): Promise<Glossary> {
  return json<Glossary>('/v1/ai/glossary?status=all');
}

export function putTerm(t: { term: string; definition: string; sql?: string; tables?: string[] }): Promise<unknown> {
  return json('/v1/ai/glossary', { method: 'POST', body: t });
}

export function termVerb(term: string, verb: 'accept' | 'reject' | 'delete'): Promise<unknown> {
  return json(`/v1/ai/glossary/${encodeURIComponent(term)}:${verb}`, { method: 'POST', body: {} });
}

export type DigestHeader = Schemas['AiDigestHeader'];
export type Digest = Schemas['AiDigest'];

export function digests(limit = 20): Promise<{ digests: DigestHeader[] }> {
  return json<{ digests: DigestHeader[] }>(`/v1/ai/digests?limit=${limit}`);
}

export function digest(id: string): Promise<Digest> {
  return json<Digest>(`/v1/ai/digests/${encodeURIComponent(id)}`);
}

// ---- Docs ----

export type DocsHit = Schemas['DocsSearchHit'];
export type DocsReply = Schemas['DocsSearchReply'];

export function searchDocs(query: string, limit = 12, audience?: 'user' | 'dba' | 'operator'): Promise<DocsReply> {
  return json<DocsReply>('/v1/docs:search', { method: 'POST', body: { query, limit, ...(audience ? { audience } : {}) } });
}

export type DocsPage = Schemas['DocsPage'];

/** One docs page of this release, rebuilt from the docs pack. */
export function docsPage(page: string, version?: string): Promise<DocsPage> {
  const path = page.split('/').map(encodeURIComponent).join('/');
  return json<DocsPage>(`/v1/docs/pages/${path}${version ? `?version=${encodeURIComponent(version)}` : ''}`);
}
