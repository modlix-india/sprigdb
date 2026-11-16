// The Docs panel: the product documentation's search (POST /v1/docs:search,
// the same index the assistants cite), with links to the published pages.

import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { BookOpen } from '../icons';
import { searchDocs } from './api';
import { docsUrl } from './docs';
import { Markdown } from './Markdown';
import { AskButton } from './AskButton';
import './assistant.css';

const CUT = 700;

function Hit({ h }: { h: Awaited<ReturnType<typeof searchDocs>>['hits'][number] }) {
  const [all, setAll] = useState(false);
  const heading = h.heading_path.split(' > ').filter(Boolean).pop();
  const long = h.text.length > CUT;
  return (
    <article className="ai-hit stack" data-testid="docs-hit">
      <h3>
        <a href={docsUrl(h.page, heading)} target="_blank" rel="noopener noreferrer">
          {h.title}
          {heading && heading !== h.title ? ` › ${heading}` : ''}
        </a>
      </h3>
      <div className="small muted">
        docs/{h.page.replace(/\.md$/i, '')}.md
        {h.heading_path ? ` · ${h.heading_path}` : ''}
      </div>
      <Markdown text={all || !long ? h.text : `${h.text.slice(0, CUT)}…`} />
      {long && (
        <button className="link" onClick={() => setAll((a) => !a)} aria-expanded={all}>
          {all ? 'Show less' : 'Show more'}
        </button>
      )}
    </article>
  );
}

export function DocsPage() {
  const [params, setParams] = useSearchParams();
  const q = params.get('q') ?? '';
  const [text, setText] = useState(q);
  const hits = useQuery({ queryKey: ['docs', q], queryFn: () => searchDocs(q, 12), enabled: !!q.trim(), staleTime: 300_000 });
  return (
    <div className="stack ai-page">
      <div className="row">
        <BookOpen size={20} aria-hidden="true" />
        <h1 style={{ margin: 0 }}>Docs</h1>
      </div>
      <form
        className="row"
        role="search"
        onSubmit={(e) => {
          e.preventDefault();
          setParams(text.trim() ? { q: text.trim() } : {});
        }}
      >
        <label className="visually-hidden" htmlFor="docs-q">
          Search the documentation
        </label>
        <input id="docs-q" value={text} onChange={(e) => setText(e.target.value)} placeholder="Search the documentation…" style={{ flex: 1 }} />
        <button type="submit" className="primary">
          Search
        </button>
        {q && (
          <AskButton
            label="Ask the assistant"
            role="data"
            options={() => ({ text: q })}
          />
        )}
      </form>
      {!q && <p className="muted">Search this version’s documentation: the same pages the assistants cite.</p>}
      {hits.isLoading && <p className="muted">Searching…</p>}
      {hits.error && <p className="error">{errorText(hits.error)}</p>}
      {hits.data && (
        <p className="small muted" role="status">
          {hits.data.hits.length} result{hits.data.hits.length === 1 ? '' : 's'} · version {hits.data.version} ·{' '}
          {hits.data.mode === 'hybrid' ? 'keywords and meaning' : 'keywords'}
        </p>
      )}
      {hits.data?.hits.map((h) => <Hit key={h.chunk_id} h={h} />)}
    </div>
  );
}
