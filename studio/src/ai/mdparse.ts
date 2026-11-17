// The small part of Markdown the assistants and the docs write:
// paragraphs, headings, lists, quotes, tables, fenced code (also while a
// fence is still streaming), and inline code, bold, italics and links.
// Parsed to data and drawn as React elements, so neither model text nor
// a docs page ever becomes HTML.

export type MdBlock =
  | { type: 'p'; text: string }
  | { type: 'h'; level: number; text: string }
  | { type: 'ul' | 'ol'; items: string[] }
  | { type: 'code'; lang: string; text: string }
  | { type: 'quote'; text: string }
  | { type: 'table'; head: string[]; rows: string[][] };

/** A table row's cells: split on `|` outside code spans (`\|` stays a
 * pipe), and `<br>` as a line break. */
export function tableCells(line: string): string[] {
  const t = line.trim().replace(/^\|/, '').replace(/\|$/, '');
  const cells: string[] = [];
  let cur = '';
  let code = false;
  for (let i = 0; i < t.length; i++) {
    const c = t[i]!;
    if (c === '\\' && t[i + 1] === '|') {
      cur += '|';
      i++;
    } else if (c === '`') {
      code = !code;
      cur += c;
    } else if (c === '|' && !code) {
      cells.push(cur);
      cur = '';
    } else cur += c;
  }
  cells.push(cur);
  return cells.map((x) => x.trim().replace(/<br\s*\/?>/gi, '\n'));
}

const isDelimiterRow = (line: string) => /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/.test(line);

export function parseMarkdown(src: string): MdBlock[] {
  const out: MdBlock[] = [];
  const lines = src.replace(/\r\n/g, '\n').split('\n');
  let para: string[] = [];
  const flush = () => {
    if (para.length) out.push({ type: 'p', text: para.join('\n') });
    para = [];
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    // A fence and its info string (`sql test`): the language is its first word.
    const fence = /^\s*(`{3,}|~{3,})\s*([\w+-]*)[^`]*$/.exec(line);
    if (fence) {
      flush();
      const close = fence[1]!;
      const body: string[] = [];
      i++;
      while (i < lines.length && !lines[i]!.trim().startsWith(close)) {
        body.push(lines[i]!);
        i++;
      }
      out.push({ type: 'code', lang: fence[2]!.toLowerCase(), text: body.join('\n') });
      continue;
    }
    if (/^\s*\|/.test(line) && i + 1 < lines.length && isDelimiterRow(lines[i + 1]!)) {
      flush();
      const head = tableCells(line);
      const rows: string[][] = [];
      i += 2;
      while (i < lines.length && /^\s*\|/.test(lines[i]!)) {
        rows.push(tableCells(lines[i]!));
        i++;
      }
      i--;
      out.push({ type: 'table', head, rows });
      continue;
    }
    const h = /^(#{1,6})\s+(.*)$/.exec(line);
    if (h) {
      flush();
      out.push({ type: 'h', level: h[1]!.length, text: h[2]!.trim() });
      continue;
    }
    const ul = /^\s*[-*+]\s+(.*)$/.exec(line);
    const ol = /^\s*\d+[.)]\s+(.*)$/.exec(line);
    if (ul || ol) {
      flush();
      const type = ul ? 'ul' : 'ol';
      const last = out[out.length - 1];
      const item = (ul ?? ol)![1]!;
      if (last && last.type === type) last.items.push(item);
      else out.push({ type, items: [item] });
      continue;
    }
    if (/^\s*>\s?/.test(line)) {
      flush();
      const text = line.replace(/^\s*>\s?/, '');
      const last = out[out.length - 1];
      if (last && last.type === 'quote') last.text += `\n${text}`;
      else out.push({ type: 'quote', text });
      continue;
    }
    if (!line.trim()) {
      flush();
      continue;
    }
    // A continuation line of a list item.
    const last = out[out.length - 1];
    if (!para.length && last && (last.type === 'ul' || last.type === 'ol') && /^\s{2,}\S/.test(line)) {
      last.items[last.items.length - 1] += ` ${line.trim()}`;
      continue;
    }
    para.push(line);
  }
  flush();
  return out;
}

export type Inline =
  | { type: 'text'; text: string }
  | { type: 'code'; text: string }
  | { type: 'strong'; text: string }
  | { type: 'em'; text: string }
  | { type: 'link'; text: string; href: string }
  | { type: 'image'; alt: string };

/** Inline marks of one paragraph or item. Links to http(s) only, unless
 * `relative` also allows links without a scheme (a docs page's
 * `../x.md#y` and `#y`); any other link stays text. Images become their
 * description. */
export function parseInline(src: string, relative = false): Inline[] {
  const out: Inline[] = [];
  const text = (t: string) => {
    const last = out[out.length - 1];
    if (last?.type === 'text') last.text += t;
    else out.push({ type: 'text', text: t });
  };
  const re = /`([^`]+)`|\*\*([^*]+)\*\*|__([^_]+)__|\*([^*\s][^*]*)\*|!\[([^\]]*)\]\(([^)\s]+)\)|\[([^\]]+)\]\(([^)\s]+)\)/g;
  let at = 0;
  for (let m = re.exec(src); m; m = re.exec(src)) {
    if (m.index > at) text(src.slice(at, m.index));
    if (m[1] !== undefined) out.push({ type: 'code', text: m[1] });
    else if (m[2] !== undefined || m[3] !== undefined) out.push({ type: 'strong', text: (m[2] ?? m[3])! });
    else if (m[4] !== undefined) out.push({ type: 'em', text: m[4] });
    else if (m[5] !== undefined) out.push({ type: 'image', alt: m[5] });
    else {
      const href = m[8]!;
      const web = /^https?:\/\//i.test(href);
      const local = relative && !/^[a-z][a-z0-9+.-]*:/i.test(href) && !href.startsWith('//');
      if (web || local) out.push({ type: 'link', text: m[7]!, href });
      else text(m[0]);
    }
    at = m.index + m[0].length;
  }
  if (at < src.length) text(src.slice(at));
  return out;
}
