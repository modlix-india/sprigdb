import { describe, expect, it } from 'vitest';
import { actionsFor, columnsText, newestFirst, pendingFor, tableOf, type IndexRecord } from './records';

const rec = (state: string, pinned = false): IndexRecord => ({
  name: 'sprig_auto_aaaa0001',
  spec: { db: 'shop', table: 'orders', cols: [{ column: 'k' }, { column: 'at', desc: true }] },
  origin: 'auto',
  state,
  serves: [1],
  est_before: 20000,
  est_after: 1,
  benefit_ms_per_hour: 12.5,
  reason: 'top_total_time',
  created_us: 1,
  changed_us: 1,
  pinned,
});

describe('automatic index records', () => {
  it('offer the actions the controller allows in each state', () => {
    expect(actionsFor(rec('active'))).toEqual(['pin', 'drop']);
    expect(actionsFor(rec('active', true))).toEqual(['unpin', 'drop']);
    expect(actionsFor(rec('recommended'))).toEqual(['pin', 'drop']);
    expect(actionsFor(rec('dropped'))).toEqual(['restore']);
    expect(actionsFor(rec('failed'))).toEqual(['restore']);
    expect(actionsFor(rec('rejected'))).toEqual([]);
    // A restored index comes back pinned; once dropped again it can be restored and unpinned.
    expect(actionsFor(rec('dropped', true))).toEqual(['unpin', 'restore']);
  });

  it('show their columns, table and pending request', () => {
    const r = rec('active');
    expect(columnsText(r)).toBe('k, at DESC');
    expect(tableOf(r)).toBe('shop.orders');
    const reqs = [{ index: 'SPRIG_AUTO_AAAA0001', action: 'pin' as const, actor: 'op:x', reason: '', at_us: 5 }];
    expect(pendingFor(reqs, r.name)?.action).toBe('pin');
    expect(pendingFor(reqs, 'sprig_auto_other')).toBeUndefined();
  });

  it('list events newest first', () => {
    const e = (at_us: number) => ({ at_us, kind: 'built', index: 'x', def: '', table: 'shop.orders', fingerprints: [], reason: '' });
    expect(newestFirst([e(1), e(3), e(2)]).map((x) => x.at_us)).toEqual([3, 2, 1]);
  });
});
