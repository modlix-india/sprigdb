import { useEffect, useState } from 'react';
import type { EditorLanguage, TokenLine } from '../editor/Monaco';

/** A Monaco token type (`keyword.sql`) as one of the editor theme's colours. */
export function tokenClass(type: string): string | undefined {
  const t = type.split('.')[0] ?? '';
  if (t === 'keyword' || t === 'string' || t === 'number' || t === 'comment' || t === 'predefined') return `tk-${t}`;
  if (t === 'operator' || t === 'delimiter') return 'tk-operator';
  return undefined;
}

/** Read-only code, coloured by Monaco's tokenizer once it has loaded
 * (plain text until then, and if it can't load). */
export function CodeView({ text, language = 'mysql', label }: { text: string; language?: EditorLanguage; label: string }) {
  const [lines, setLines] = useState<{ text: string; lines: TokenLine[] } | null>(null);
  useEffect(() => {
    let live = true;
    import('../editor/Monaco')
      .then((m) => m.tokenLines(text, language))
      .then((l) => {
        if (live) setLines({ text, lines: l });
      })
      .catch(() => undefined);
    return () => {
      live = false;
    };
  }, [text, language]);
  return (
    <pre className="ai-code" aria-label={label} tabIndex={0}>
      {lines && lines.text === text
        ? lines.lines.map((l, i) => (
            <span key={i}>
              {i > 0 && '\n'}
              {l.map(([s, type], j) => (
                <span key={j} className={tokenClass(type)}>
                  {s}
                </span>
              ))}
            </span>
          ))
        : text}
    </pre>
  );
}
