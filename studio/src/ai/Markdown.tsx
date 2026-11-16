import { Fragment } from 'react';
import { CodeView } from './CodeView';
import { parseInline, parseMarkdown } from './mdparse';

function InlineText({ text }: { text: string }) {
  return (
    <>
      {parseInline(text).map((p, i) => {
        switch (p.type) {
          case 'code':
            return <code key={i}>{p.text}</code>;
          case 'strong':
            return <strong key={i}>{p.text}</strong>;
          case 'em':
            return <em key={i}>{p.text}</em>;
          case 'link':
            return (
              <a key={i} href={p.href} target="_blank" rel="noopener noreferrer">
                {p.text}
              </a>
            );
          default:
            return (
              <Fragment key={i}>
                {p.text.split('\n').map((l, j) => (
                  <Fragment key={j}>
                    {j > 0 && <br />}
                    {l}
                  </Fragment>
                ))}
              </Fragment>
            );
        }
      })}
    </>
  );
}

const SQLISH = new Set(['sql', 'mysql', 'mariadb', '']);

/** An answer's text as Markdown. */
export function Markdown({ text }: { text: string }) {
  return (
    <div className="ai-md">
      {parseMarkdown(text).map((b, i) => {
        switch (b.type) {
          case 'p':
            return (
              <p key={i}>
                <InlineText text={b.text} />
              </p>
            );
          case 'h':
            return (
              <p key={i} className="ai-md-h">
                <strong>
                  <InlineText text={b.text} />
                </strong>
              </p>
            );
          case 'ul':
          case 'ol': {
            const L = b.type;
            return (
              <L key={i}>
                {b.items.map((it, j) => (
                  <li key={j}>
                    <InlineText text={it} />
                  </li>
                ))}
              </L>
            );
          }
          case 'quote':
            return (
              <blockquote key={i}>
                <InlineText text={b.text} />
              </blockquote>
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
