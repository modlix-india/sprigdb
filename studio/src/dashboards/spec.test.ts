import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import RULES from './chart-rules.json';
import { PRESETS, applyPreset, best, counts, fits, normalize, presetFits, presetOf, roleOf, suggest, toStored } from './spec';
import { placeholders } from './api';
import { settle, moves, clampPos } from './layout';

describe('chart rules', () => {
  it('are the gateway’s rules, word for word', () => {
    const crate = JSON.parse(readFileSync(join(__dirname, '../../../crates/sprig-ai/src/tools/chart-rules.json'), 'utf8')) as unknown;
    expect(RULES).toEqual(crate);
  });
});

describe('Show Me', () => {
  const c = (s: Parameters<typeof counts>[0]) => counts(s);
  it('follows the rules of thumb, as the tile check does', () => {
    const date = c({ mark: 'bar', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(date).toEqual({ dims: 1, measures: 1, dates: 1, geo: false, unknown: 0 });
    expect(best(date).mark).toBe('line');
    const one = c({ mark: 'bar', columns: [{ field: 'status' }], rows: [{ field: 'n', agg: 'count' }] });
    expect(best(one).mark).toBe('bar');
    expect(fits('pie', one) && fits('donut', one)).toBe(true);
    expect(fits('point', { ...one, unknown: 0 })).toBe(false);
    // A bare name might be a measure: the spec alone doesn't rule a scatter out.
    expect(fits('point', one)).toBe(true);
    expect(best(c({ mark: 'bar', columns: [{ field: 'a', agg: 'sum' }], rows: [{ field: 'b', agg: 'sum' }] })).mark).toBe('point');
    expect(best(c({ mark: 'bar', rows: [{ field: 'a', agg: 'sum' }] })).mark).toBe('kpi');
    expect(best(c({ mark: 'bar', columns: ['day'], rows: ['hour'], color: { field: 'n', agg: 'count' } })).mark).toBe('heatmap');
    // A map would win for a place, but Studio doesn't draw maps yet: the next rule.
    expect(best(c({ mark: 'bar', columns: [{ field: 'country' }], rows: [{ field: 'n', agg: 'sum' }] })).mark).toBe('bar');
    expect(best(c({ mark: 'bar' })).mark).toBe('text-table');
  });

  it('greys out presets that need other fields', () => {
    const one = { ...c({ mark: 'bar', columns: [{ field: 'region' }], rows: [{ field: 'revenue', agg: 'sum' }] }), unknown: 0 };
    const ok = PRESETS.filter((p) => presetFits(p, one)).map((p) => p.id);
    expect(ok).toEqual(expect.arrayContaining(['bar', 'hbar', 'line', 'pie', 'donut', 'treemap', 'funnel', 'text-table', 'pivot', 'waterfall']));
    expect(ok).not.toContain('point');
    expect(ok).not.toContain('combo');
    expect(ok).not.toContain('stacked');
    const two = { ...c({ mark: 'bar', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }, { field: 'orders', agg: 'count' }] }), unknown: 0 };
    expect(PRESETS.filter((p) => presetFits(p, two)).map((p) => p.id)).toEqual(expect.arrayContaining(['combo', 'stacked', 'line']));
  });

  it('applies presets by moving fields, never dropping them', () => {
    const s = normalize({ mark: 'bar', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }, { field: 'orders', agg: 'count' }] });
    const combo = applyPreset(s, PRESETS.find((p) => p.id === 'combo')!);
    expect(combo.rows[1]).toEqual({ field: 'orders', agg: 'count', axis: 'right', mark: 'line' });
    expect(presetOf(combo)).toBe('combo');
    const h = applyPreset(s, PRESETS.find((p) => p.id === 'hbar')!);
    expect(h.columns.map((f) => f.field)).toEqual(['revenue', 'orders']);
    expect(h.rows.map((f) => f.field)).toEqual(['week']);
    expect(presetOf(h)).toBe('hbar');
    const back = applyPreset(h, PRESETS.find((p) => p.id === 'bar')!);
    expect(back.rows.map((f) => f.field)).toEqual(['revenue', 'orders']);
    expect(presetOf(applyPreset(s, PRESETS.find((p) => p.id === 'percent')!))).toBe('percent');
  });

  it('reads roles from column types and fills bare measures', () => {
    expect(roleOf({ name: 'total', type: 'DECIMAL' })).toBe('measure');
    expect(roleOf({ name: 'customer_id', type: 'BIGINT' })).toBe('dimension');
    expect(roleOf({ name: 'is_test', type: 'TINYINT' })).toBe('dimension');
    expect(roleOf({ name: 'day', type: 'DATE' })).toBe('date');
    expect(roleOf({ name: 'region', type: 'VARCHAR' })).toBe('dimension');
    const cols = [
      { name: 'category', type: 'VARCHAR' },
      { name: 'revenue', type: 'DECIMAL' },
    ];
    // The assistant's grouped-query tile: bare names.
    const s = normalize({ mark: 'bar', columns: ['category'], rows: ['revenue'] }, cols);
    expect(s.rows[0]).toEqual({ field: 'revenue', agg: 'sum' });
    expect(toStored(s)).toEqual({ mark: 'bar', columns: [{ field: 'category' }], rows: [{ field: 'revenue', agg: 'sum' }] });
  });

  it('suggests a first chart for a result', () => {
    expect(suggest([{ name: 'day', type: 'DATE' }, { name: 'n', type: 'BIGINT' }]).mark).toBe('line');
    expect(suggest([{ name: 'status', type: 'VARCHAR' }, { name: 'n', type: 'BIGINT' }]).mark).toBe('bar');
    expect(suggest([{ name: 'total', type: 'DECIMAL' }]).mark).toBe('kpi');
    const sc = suggest([{ name: 'a', type: 'DOUBLE' }, { name: 'b', type: 'DOUBLE' }]);
    expect(sc.mark).toBe('point');
    expect(sc.columns[0]?.field).toBe('a');
    expect(sc.rows[0]?.field).toBe('b');
    expect(suggest([{ name: 'name', type: 'VARCHAR' }]).mark).toBe('text-table');
  });
});

describe('filters in queries', () => {
  it('finds {{names}} outside quotes and comments', () => {
    expect(placeholders("SELECT * FROM o WHERE d >= {{from}} AND r = {{ region }} AND x = '{{not}}' -- {{nope}}\n AND y IN {{from}}")).toEqual(['from', 'region']);
  });
});

describe('the grid', () => {
  it('pushes tiles a moved one lands on downwards', () => {
    const tiles = [
      { id: 'a', pos: { x: 0, y: 0, w: 6, h: 4 } },
      { id: 'b', pos: { x: 6, y: 0, w: 6, h: 4 } },
      { id: 'c', pos: { x: 0, y: 4, w: 12, h: 3 } },
    ];
    const after = settle(tiles.map((t) => (t.id === 'b' ? { ...t, pos: { x: 0, y: 2, w: 12, h: 4 } } : t)), 'b');
    expect(after.find((t) => t.id === 'b')!.pos).toEqual({ x: 0, y: 2, w: 12, h: 4 });
    expect(after.find((t) => t.id === 'a')!.pos.y).toBe(6);
    expect(after.find((t) => t.id === 'c')!.pos.y).toBe(10);
    expect(moves(tiles, after).map((m) => m.tile).sort()).toEqual(['a', 'b', 'c']);
    expect(clampPos({ x: 10, y: -2, w: 6, h: 40 })).toEqual({ x: 6, y: 0, w: 6, h: 24 });
  });
});
