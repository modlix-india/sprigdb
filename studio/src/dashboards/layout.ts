// Tiles on the 12-column grid: keeping a moved or resized tile inside the
// grid and pushing the tiles it lands on downwards, so none overlap.

import type { Pos } from './api';

export const COLUMNS = 12;
export const MAX_H = 24;

export function clampPos(p: Pos): Pos {
  const w = Math.max(1, Math.min(COLUMNS, Math.round(p.w)));
  const h = Math.max(1, Math.min(MAX_H, Math.round(p.h)));
  const x = Math.max(0, Math.min(COLUMNS - w, Math.round(p.x)));
  const y = Math.max(0, Math.round(p.y));
  return { x, y, w, h };
}

export function overlaps(a: Pos, b: Pos): boolean {
  return a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
}

/**
 * The layout after `moved` took its new place: tiles it overlaps move down
 * just below it, and so on, until nothing overlaps. The moved tile keeps
 * its place; the others keep their order.
 */
export function settle<T extends { id: string; pos: Pos }>(tiles: T[], moved: string): T[] {
  const out = tiles.map((t) => ({ ...t, pos: t.id === moved ? clampPos(t.pos) : { ...t.pos } }));
  const fixed: T[] = out.filter((t) => t.id === moved);
  const rest = out.filter((t) => t.id !== moved).sort((a, b) => a.pos.y - b.pos.y || a.pos.x - b.pos.x);
  for (const t of rest) {
    for (;;) {
      const hit = fixed.find((f) => overlaps(f.pos, t.pos));
      if (!hit) break;
      t.pos = { ...t.pos, y: hit.pos.y + hit.pos.h };
    }
    fixed.push(t);
  }
  return out;
}

/** Tiles whose place changed between two layouts. */
export function moves(before: { id: string; pos: Pos }[], after: { id: string; pos: Pos }[]): { tile: string; pos: Pos }[] {
  return after
    .filter((a) => {
      const b = before.find((x) => x.id === a.id);
      return !b || b.pos.x !== a.pos.x || b.pos.y !== a.pos.y || b.pos.w !== a.pos.w || b.pos.h !== a.pos.h;
    })
    .map((a) => ({ tile: a.id, pos: a.pos }));
}
