// A line diff (longest common subsequence) for the inline ask's preview.

export type DiffLine = { op: 'same' | 'add' | 'del'; text: string };

export function lineDiff(before: string, after: string): DiffLine[] {
  const a = before.replace(/\r\n/g, '\n').split('\n');
  const b = after.replace(/\r\n/g, '\n').split('\n');
  // Long texts: no table, just old then new.
  if (a.length * b.length > 250_000) {
    return [...a.map((text) => ({ op: 'del' as const, text })), ...b.map((text) => ({ op: 'add' as const, text }))];
  }
  const n = a.length;
  const m = b.length;
  const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i]![j] = a[i] === b[j] ? lcs[i + 1]![j + 1]! + 1 : Math.max(lcs[i + 1]![j]!, lcs[i]![j + 1]!);
    }
  }
  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      out.push({ op: 'same', text: a[i]! });
      i++;
      j++;
    } else if (lcs[i + 1]![j]! >= lcs[i]![j + 1]!) {
      out.push({ op: 'del', text: a[i]! });
      i++;
    } else {
      out.push({ op: 'add', text: b[j]! });
      j++;
    }
  }
  while (i < n) out.push({ op: 'del', text: a[i++]! });
  while (j < m) out.push({ op: 'add', text: b[j++]! });
  return out;
}
