import { Fragment, type ReactNode } from 'react';
import { Link, useParams } from 'react-router';
import { CodeView } from './CodeView';
import { docsRoute, docsTarget, slug } from './docs';
import { parseInline, parseMarkdown } from './mdparse';

/** Where a link goes inside Studio, or null for a link that leaves it. */
export type LinkTo = (href: string) => string | null;

function Lines({ text }: { text: string }) {
  return (
    <>
      {text.split('\n').map((l, j) => (
        <Fragment key={j}>
          {j > 0 && <br />}
          {l}
        </Fragment>
      ))}
    </>
  );
}

function InlineText({ text, to, relative }: { text: string; to: LinkTo; relative: boolean }) {
  return (
    <>
      {parseInline(text, relative).map((p, i) => {
        switch (p.type) {
          case 'code':
            return <code key={i}>{p.text}</code>;
          case 'strong':
            return <strong key={i}>{p.text}</strong>;
          case 'em':
            return <em key={i}>{p.text}</em>;
          case 'image':
            return (
              <span key={i} className="muted">
                [image{p.alt ? `: ${p.alt}` : ''}]
              </span>
            );
          case 'link': {
            const inside = to(p.href);
            if (inside !== null) {
              return (
                <Link key={i} to={inside}>
                  {p.text}
                </Link>
              );
            }
            return /^https?:\/\//i.test(p.href) ? (
              <a key={i} href={p.href} target="_blank" rel="noopener noreferrer">
                {p.text}
              </a>
            ) : (
              <Fragment key={i}>{p.text}</Fragment>
            );
          }
          default:
            return <Lines key={i} text={p.text} />;
        }
      })}
    </>
  );
}

const SQLISH = new Set(['sql', 'mysql', 'mariadb', '']);

/**
 * Text as Markdown: an answer's (headings drawn as bold lines), or a docs
 * page's (`headings`: real headings, a level below the page's own, with
 * anchors). Links to the documentation website open Studio's own docs
 * page; `link` maps the other links (a docs page's relative ones).
 */
export function Markdown({ text, headings = false, link }: { text: string; headings?: boolean; link?: LinkTo }) {
  const { tenant } = useParams();
  const base = tenant !== undefined ? `/t/${encodeURIComponent(tenant)}` : null;
  const to: LinkTo = (href) => {
    const own = link?.(href);
    if (own) return own;
    // The website's docs pages open in Studio.
    if (base !== null && /^https?:\/\//i.test(href)) {
      const t = docsTarget('README.md', href);
      if (t) return docsRoute(base, t.page, t.anchor);
    }
    return null;
  };
  const relative = !!link;
  const ids = new Map<string, number>();
  const idOf = (t: string) => {
    const s = slug(t);
    const n = ids.get(s) ?? 0;
    ids.set(s, n + 1);
    return n ? `${s}-${n}` : s;
  };
  const inline = (t: string) => <InlineText text={t} to={to} relative={relative} />;
  return (
    <div className={headings ? 'ai-md ai-md-doc' : 'ai-md'}>
      {parseMarkdown(text).map((b, i): ReactNode => {
        switch (b.type) {
          case 'p':
            return <p key={i}>{inline(b.text)}</p>;
          case 'h': {
            if (!headings) {
              return (
                <p key={i} className="ai-md-h">
                  <strong>{inline(b.text)}</strong>
                </p>
              );
            }
            const H = `h${Math.min(6, Math.max(2, b.level + 1))}` as 'h2';
            const plain = b.text.replace(/\[([^\]]+)\]\([^)]*\)/g, '$1');
            return (
              <H key={i} id={idOf(plain)}>
                {inline(b.text)}
              </H>
            );
          }
          case 'ul':
          case 'ol': {
            const L = b.type;
            return (
              <L key={i}>
                {b.items.map((it, j) => (
                  <li key={j}>{inline(it)}</li>
                ))}
              </L>
            );
          }
          case 'quote':
            return <blockquote key={i}>{inline(b.text)}</blockquote>;
          case 'table':
            return (
              <div key={i} className="ai-md-table" role="region" aria-label={`Table: ${b.head.join(', ')}`} tabIndex={0}>
                <table>
                  <thead>
                    <tr>
                      {b.head.map((h, j) => (
                        <th key={j} scope="col">
                          {inline(h)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {b.rows.map((r, j) => (
                      <tr key={j}>
                        {r.map((c, k) => (
                          <td key={k}>{inline(c)}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            );
          case 'code': {
            const label = `Code${b.lang ? ` (${b.lang})` : ''}`;
            if (SQLISH.has(b.lang)) return <CodeView key={i} text={b.text} label={label} />;
            if (b.lang === 'js' || b.lang === 'javascript' || b.lang === 'json')
              return <CodeView key={i} text={b.text} language="javascript" label={label} />;
            return (
              <pre key={i} className="ai-code" aria-label={label} tabIndex={0}>
                {b.text}
              </pre>
            );
          }
        }
      })}
    </div>
  );
}
