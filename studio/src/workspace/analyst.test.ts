import { describe, expect, it } from 'vitest';
import { classify } from '../lib/sql';
import { suggestions } from '../ai/AssistantDock';
import { CHECKLISTS, shows, steps } from './checklist';
import {
  WORKSPACES,
  objectTabs,
  openWorkspaces,
  readOnlyByDefault,
  showsPage,
  stopsInReadOnly,
  treeObjects,
  workspaceById,
  workspaceFor,
  type WorkspaceId,
} from './workspaces';

const ws = (id: WorkspaceId) => workspaceById(id)!;
const objs = [
  { name: 'orders', kind: 'table' as const },
  { name: 'v_paid', kind: 'view' as const },
  { name: 'total', kind: 'function' as const },
  { name: 'refund', kind: 'procedure' as const },
  { name: 'ties', kind: 'graph' as const },
];
const ALL_TABLE_TABS = ['data', 'columns', 'indexes', 'relations', 'triggers', 'policies', 'inheritance', 'shards', 'ddl'];

describe("the analyst's workspace", () => {
  it('lists tables, views and collections only in the tree', () => {
    expect(treeObjects(ws('analyst'), objs).map((o) => o.name)).toEqual(['orders', 'v_paid']);
    expect(treeObjects(ws('developer'), objs)).toHaveLength(5);
  });

  it('shows data, columns and relations on object pages, without indexes, triggers, shards or definitions', () => {
    expect(objectTabs(ws('analyst'), 'table', ALL_TABLE_TABS)).toEqual(['data', 'columns', 'relations']);
    expect(objectTabs(ws('analyst'), 'view', ['data', 'columns', 'policies', 'ddl'])).toEqual(['data', 'columns']);
    expect(objectTabs(ws('db_admin'), 'table', ALL_TABLE_TABS)).toEqual(ALL_TABLE_TABS);
  });

  it('starts query tabs read-only, and a read-only tab sends reads only', () => {
    expect(readOnlyByDefault(ws('analyst'))).toBe(true);
    for (const w of WORKSPACES.filter((x) => x.id !== 'analyst')) expect(readOnlyByDefault(w)).toBe(false);
    const stopped = (sql: string) => stopsInReadOnly(classify(sql));
    expect(stopped('SELECT * FROM orders')).toBe(false);
    expect(stopped('SHOW TABLES')).toBe(false);
    expect(stopped('WITH x AS (SELECT 1) SELECT * FROM x')).toBe(false);
    expect(stopped('START TRANSACTION')).toBe(false);
    expect(stopped('INSERT INTO orders VALUES (1)')).toBe(true);
    expect(stopped('WITH x AS (SELECT 1) DELETE FROM orders')).toBe(true);
    expect(stopped('CREATE TABLE t (a INT)')).toBe(true);
    expect(stopped('GRANT SELECT ON *.* TO u')).toBe(true);
    expect(stopped('OPTIMIZE TABLE orders')).toBe(true);
    // MongoDB and Cypher classes use the same words.
    expect(stopsInReadOnly('read')).toBe(false);
    expect(stopsInReadOnly('write')).toBe(true);
  });

  it('shows a note instead of pages of sections it does not offer, and offers a workspace that has them', () => {
    const a = ws('analyst');
    expect(showsPage(a, '')).toBe(true);
    expect(showsPage(a, 'db/shop/obj/table/orders')).toBe(true);
    expect(showsPage(a, 'q/abc')).toBe(true);
    expect(showsPage(a, 'd')).toBe(true);
    expect(showsPage(a, 'docs')).toBe(true);
    expect(showsPage(a, 'security')).toBe(true); // your own sign-in factors
    for (const rel of ['logic', 'logic/shop/function/total', 'messaging', 'indexing', 'inbox', 'settings', 'access/users', 'policies', 'inheritance']) {
      expect(showsPage(a, rel)).toBe(false);
    }
    // Every other workspace shows every page, whatever its sections.
    expect(showsPage(ws('security_admin'), 'logic')).toBe(true);
    const open = openWorkspaces({
      read_data: true,
      change_schemas: true,
      manage_logic: false,
      manage_users: false,
      approve_changes: false,
      administer_tenant: false,
      operator: false,
    });
    expect(workspaceFor(open, 'logic')?.id).toBe('developer');
    expect(workspaceFor(open, 'access/users')?.id).toBe('developer');
    expect(workspaceFor([ws('analyst')], 'logic')).toBeUndefined();
  });
});

describe('first-run checklists', () => {
  const none = { ranQuery: false, savedQuery: false, openedTable: false, openedDatabase: false };

  it('has a short list for every tenant workspace', () => {
    for (const [id, items] of Object.entries(CHECKLISTS)) {
      expect(items.length, id).toBeGreaterThanOrEqual(3);
      expect(items.length, id).toBeLessThanOrEqual(5);
      expect(new Set(items.map((i) => i.id)).size, id).toBe(items.length);
    }
  });

  it('ticks steps Studio can see done, and the ones ticked by hand', () => {
    const s = steps(CHECKLISTS.analyst, { ...none, ranQuery: true }, ['dashboard']);
    expect(s.filter((x) => x.done).map((x) => x.id)).toEqual(['run-query', 'dashboard']);
    expect(shows(s, { ticked: [], hidden: false })).toBe(true);
    expect(shows(s, { ticked: [], hidden: true })).toBe(false);
    const all = steps(CHECKLISTS.analyst, { ranQuery: true, savedQuery: true, openedTable: true, openedDatabase: false }, ['dashboard']);
    expect(shows(all, { ticked: ['dashboard'], hidden: false })).toBe(false);
  });
});

describe("the assistant's starting questions per workspace", () => {
  it("uses each workspace's own, without engine terms for the analyst", () => {
    const a = suggestions('data', 'shop', ['orders', 'customers'], 'analyst');
    expect(a[0]).toBe('What does orders hold, in plain words?');
    for (const q of a) expect(q).not.toMatch(/\b(shard|unit|lease|index)/i);
    expect(suggestions('data', 'shop', ['orders', 'customers'], 'developer')[0]).toBe('Write a query joining orders and customers');
    expect(suggestions('dba', 'shop', ['orders'], 'security_admin')[0]).toBe('Who can read orders, and through which roles?');
    expect(suggestions('dba', 'shop', ['orders'], 'tenant_admin')[0]).toBe('What needs my attention in this tenant today?');
    // The database administrator's are the DBA role's own; without a
    // workspace nothing changes.
    expect(suggestions('dba', 'shop', ['orders'], 'db_admin')).toEqual(suggestions('dba', 'shop', ['orders']));
    expect(suggestions('admin', null, [], 'analyst')[0]).toBe('Are any clusters unhealthy right now?');
  });
});
