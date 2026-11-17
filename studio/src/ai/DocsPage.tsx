// The Docs panel: the product documentation's search (POST /v1/docs:search,
// the same index the assistants cite), and its pages, read in Studio:
// rebuilt from the release's docs pack (GET /v1/docs/pages/{page}), so
// they work without the public website and match the running release.
// Each page also links to the website.

import { useEffect, useState } from 'react';
import { Link, useLocation, useNavigate, useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { BookOpen } from '../icons';
import { docsPage, searchDocs } from './api';
import { docsRoute, docsTarget, docsUrl, pagePath, slug } from './docs';
import { Markdown } from './Markdown';
import { AskButton } from './AskButton';
import './assistant.css';

const CUT = 700;

function Hit({ h, base }: { h: Awaited<ReturnType<typeof searchDocs>>['hits'][number]; base: string }) {
  const [all, setAll] = useState(false);
  const heading = h.heading_path.split(' > ').filter(Boolean).pop();
  const long = h.text.length > CUT;
  const anchor = heading && heading !== h.title ? slug(heading) : undefined;
  const name = `${h.title}${heading && heading !== h.title ? ` › ${heading}` : ''}`;
  return (
    <article className="ai-hit stack" data-testid="docs-hit">
      <h3>
        <Link to={docsRoute(base, h.page, anchor)}>{name}</Link>
      </h3>
      <div className="small muted">
        docs/{pagePath(h.page)}
        {h.heading_path ? ` · ${h.heading_path}` : ''} ·{' '}
        <a href={docsUrl(h.page, heading)} target="_blank" rel="noopener noreferrer" aria-label={`${name} on the website`}>
          website ↗
        </a>
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

/** The Markdown without its first `# title` line (the page shows the title). */
function body(markdown: string): string {
  return markdown.replace(/^#[ \t]+[^\n]*\n+/, '');
}

function PageView({ page, base, q }: { page: string; base: string; q: string }) {
  const p = pagePath(page);
  const doc = useQuery({ queryKey: ['docs-page', p], queryFn: () => docsPage(p), staleTime: 300_000 });
  const { hash } = useLocation();
  // An anchor in the address: its heading, once the page is drawn.
  useEffect(() => {
    if (!doc.data) return;
    if (!hash) {
      window.scrollTo?.(0, 0);
      return;
    }
    document.getElementById(decodeURIComponent(hash.slice(1)))?.scrollIntoView?.({ block: 'start' });
  }, [doc.data, hash]);
  const link = (href: string) => {
    const t = docsTarget(p, href);
    return t ? docsRoute(base, t.page, t.anchor) : null;
  };
  return (
    <article className="stack docs-article" aria-labelledby="docs-title" data-testid="docs-page">
      <div className="row small">
        <Link to={q ? `${base}/docs?q=${encodeURIComponent(q)}` : `${base}/docs`}>← {q ? 'Search results' : 'Search'}</Link>
        <span className="muted">docs/{p}</span>
        <a href={docsUrl(p)} target="_blank" rel="noopener noreferrer" style={{ marginInlineStart: 'auto' }} data-testid="docs-website">
          Open on the website ↗
        </a>
      </div>
      {doc.isLoading && <p className="muted">Loading the page…</p>}
      {doc.error && (
        <p className="error" role="alert">
          {errorText(doc.error)}
        </p>
      )}
      {doc.data && (
        <>
          <h2 id="docs-title" style={{ margin: 0 }}>
            {doc.data.title}
          </h2>
          <p className="small muted" style={{ margin: 0 }}>
            Version {doc.data.version}
            {doc.data.audience.length ? ` · for ${doc.data.audience.join(', ')}` : ''}
          </p>
          {!doc.data.ordered && (
            <p className="banner small" role="note" data-testid="docs-unordered">
              This release’s docs pack doesn’t record the order of a page’s sections, so they may be out of order here. The website has the page as written.
            </p>
          )}
          <Markdown text={body(doc.data.markdown)} headings link={link} />
        </>
      )}
    </article>
  );
}

export function DocsPage() {
  const [params] = useSearchParams();
  const { tenant = '', '*': page } = useParams();
  const base = `/t/${encodeURIComponent(tenant)}`;
  const navigate = useNavigate();
  const q = params.get('q') ?? '';
  const [text, setText] = useState(q);
  const hits = useQuery({ queryKey: ['docs', q], queryFn: () => searchDocs(q, 12), enabled: !page && !!q.trim(), staleTime: 300_000 });
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
          const t = text.trim();
          void navigate(t ? `${base}/docs?q=${encodeURIComponent(t)}` : `${base}/docs`);
        }}
      >
        <label className="visually-hidden" htmlFor="docs-q">
          Search the documentation
        </label>
        <input id="docs-q" value={text} onChange={(e) => setText(e.target.value)} placeholder="Search the documentation…" style={{ flex: 1 }} />
        <button type="submit" className="primary">
          Search
        </button>
        {q && !page && <AskButton label="Ask the assistant" role="data" options={() => ({ text: q })} />}
      </form>
      {page !== undefined ? (
        <PageView page={page} base={base} q={q} />
      ) : (
        <>
          {!q && (
            <p className="muted">
              Search this version’s documentation: the same pages the assistants cite. Or start at the <Link to={docsRoute(base, 'README.md')}>documentation’s first page</Link>.
            </p>
          )}
          {hits.isLoading && <p className="muted">Searching…</p>}
          {hits.error && <p className="error">{errorText(hits.error)}</p>}
          {hits.data && (
            <p className="small muted" role="status">
              {hits.data.hits.length} result{hits.data.hits.length === 1 ? '' : 's'} · version {hits.data.version} ·{' '}
              {hits.data.mode === 'hybrid' ? 'keywords and meaning' : 'keywords'}
            </p>
          )}
          {hits.data?.hits.map((h) => <Hit key={h.chunk_id} h={h} base={base} />)}
        </>
      )}
    </div>
  );
}
