import { describe, expect, it } from 'vitest';
import type { FingerprintRow } from '../indexing/Fingerprints';
import type { ObjectInfo } from '../catalog/snapshot';
import { loadSummary, modelCounts, searchHome, setOnTenant } from './data';

const obj = (name: string, kind: ObjectInfo['kind']): ObjectInfo => ({ name, kind, rows: null, engine: null, comment: '' });

describe('home page data', () => {
  const dbs = ['shop', 'crm'];
  const snaps = [
    { objects: [obj('orders', 'table'), obj('open_orders', 'view'), obj('total', 'function'), obj('ties', 'graph')] },
    undefined,
  ];

  it('counts objects of each model per database', () => {
    expect(modelCounts(dbs, snaps)).toEqual([
      { db: 'shop', tables: 1, views: 1, graphs: 1, routines: 1 },
      { db: 'crm', tables: 0, views: 0, graphs: 0, routines: 0 },
    ]);
  });

  it('searches readable objects, then saved queries', () => {
    const saved = [
      { id: 'saved:big orders', name: 'big orders', sql: 'SELECT * FROM orders WHERE total > 100', db: 'shop' },
      { id: 'saved:leads', name: 'leads', sql: 'SELECT 1', db: null },
    ];
    const found = searchHome('ORDER', dbs, snaps, saved);
    expect(found.map((f) => [f.kind, f.name])).toEqual([
      ['object', 'orders'],
      ['object', 'open_orders'],
      ['saved', 'big orders'],
    ]);
    expect(found[0]!.rel).toBe('db/shop/obj/table/orders');
    // Routines aren't tables to open.
    expect(searchHome('total', dbs, snaps, [])).toEqual([]);
    expect(searchHome('  ', dbs, snaps, saved)).toEqual([]);
    expect(searchHome('o', dbs, snaps, saved, 2)).toHaveLength(2);
  });

  it('ranks fingerprints by total time and by p95', () => {
    const fp = (id: string, total_ms: number, p95_ms: number) => ({ id, total_ms, p95_ms }) as FingerprintRow;
    const s = loadSummary([fp('a', 10, 900), fp('b', 500, 5), fp('c', 50, 50)], 2);
    expect(s.heaviest.map((r) => r.id)).toEqual(['b', 'c']);
    expect(s.slowest.map((r) => r.id)).toEqual(['a', 'c']);
  });

  it("keeps the settings set on the tenant itself", () => {
    const rows = [
      { SETTING_NAME: 'studio.history_max', VALUE: '50', SOURCE_SCOPE: 'tenant' },
      { SETTING_NAME: 'studio.view_as', VALUE: 'on', SOURCE_SCOPE: 'default' },
      { SETTING_NAME: 'auth.x', VALUE: '1', SOURCE_SCOPE: 'ancestor acme' },
    ];
    expect(setOnTenant(rows)).toEqual([{ key: 'studio.history_max', value: '50' }]);
  });
});
