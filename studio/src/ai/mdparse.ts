// The small part of Markdown the assistants write: paragraphs, headings,
// lists, fenced code (also while a fence is still streaming), and inline
// code, bold, italics and links. Parsed to data and drawn as React
// elements, so model text never becomes HTML.

export type MdBlock =
  | { type: 'p'; text: string }
  | { type: 'h'; level: number; text: string }
  | { type: 'ul' | 'ol'; items: string[] }
  | { type: 'code'; lang: string; text: string }
  | { type: 'quote'; text: string };

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
    const fence = /^\s*(```|~~~)\s*([\w+-]*)\s*$/.exec(line);
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
  | { type: 'link'; text: string; href: string };

/** Inline marks of one paragraph or item. Links only to http(s). */
export function parseInline(src: string): Inline[] {
  const out: Inline[] = [];
  const re = /`([^`]+)`|\*\*([^*]+)\*\*|__([^_]+)__|\*([^*\s][^*]*)\*|\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g;
  let at = 0;
  for (let m = re.exec(src); m; m = re.exec(src)) {
    if (m.index > at) out.push({ type: 'text', text: src.slice(at, m.index) });
    if (m[1] !== undefined) out.push({ type: 'code', text: m[1] });
    else if (m[2] !== undefined || m[3] !== undefined) out.push({ type: 'strong', text: (m[2] ?? m[3])! });
    else if (m[4] !== undefined) out.push({ type: 'em', text: m[4] });
    else out.push({ type: 'link', text: m[5]!, href: m[6]! });
    at = m.index + m[0].length;
  }
  if (at < src.length) out.push({ type: 'text', text: src.slice(at) });
  return out;
}
