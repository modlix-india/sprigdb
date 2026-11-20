import { describe, expect, it } from 'vitest';
import { connectionLines } from './connect';
import { bytesText, failedLogins, grantTarget, longRunning, storageRows } from './data';

describe('home page items from the gateway and catalog', () => {
  it('writes a connection line per listener, never a password', () => {
    const lines = connectionLines('db.example.com', { mysql: 13306, mongo: 27018, redis: 16379, http: 13080, peer: 1, kafka: 19092 }, 'acme.app', null);
    expect(lines.map((l) => l.protocol)).toEqual(['mysql', 'mongo', 'redis', 'kafka', 'http']);
    expect(lines[0]!.text).toBe('mysql -h db.example.com -P 13306 -u acme.app -p');
    expect(lines[1]!.text).toBe('mongodb://acme.app@db.example.com:27018/?authSource=admin');
    expect(connectionLines('h', { mysql: 1 }, 'a.b', 'shop')[0]!.text).toBe('mysql -h h -P 1 -u a.b -p shop');
    expect(connectionLines('::1', { http: 80 }, 'a.b', null)[0]!.text).toBe('http://[::1]:80/v1/');
    expect(connectionLines('h', {}, 'a.b', null)).toEqual([]);
  });

  it('sums storage per database and words bytes', () => {
    expect(
      storageRows([
        { db: 'shop', tables: '5', bytes: '147456' },
        { db: 'information_schema', tables: '70', bytes: '0' },
        { db: 'crm', tables: '1', bytes: '2000000' },
      ]),
    ).toEqual([
      { db: 'crm', tables: 1, bytes: 2000000 },
      { db: 'shop', tables: 5, bytes: 147456 },
    ]);
    expect(bytesText(512)).toBe('512 bytes');
    expect(bytesText(147456)).toBe('144 KB');
    expect(bytesText(1536 * 1024)).toBe('1.5 MB');
  });

  it('keeps statements running 5 s or more, longest first, without the PROCESSLIST itself', () => {
    const r = longRunning([
      { Id: '1', User: 'app', db: 'shop', Command: 'Query', Time: '12', Info: 'SELECT SLEEP(20)' },
      { Id: '2', User: 'app', db: null, Command: 'Sleep', Time: '900', Info: null },
      { Id: '3', User: 'app', db: null, Command: 'Query', Time: '30', Info: 'SHOW FULL PROCESSLIST' },
      { Id: '4', User: 'bi', db: 'crm', Command: 'Query', Time: '40', Info: 'SELECT * FROM big' },
      { Id: '5', User: 'bi', db: 'crm', Command: 'Query', Time: '2', Info: 'SELECT 1' },
    ]);
    expect(r.map((x) => x.id)).toEqual(['4', '1']);
  });

  it('lists failed logins and writes grant targets as GRANT does', () => {
    const u = (user: string, n: number, at: string) => ({
      user,
      host: '%',
      kind: 'user',
      account_locked: false,
      password_expired: false,
      failed_logins: n,
      last_failed_login_ms: at,
    });
    expect(failedLogins([u('a', 0, '0'), u('b', 2, '100'), u('c', 5, '50'), u('d', 2, '200')]).map((x) => x.user)).toEqual(['c', 'd', 'b']);
    expect(grantTarget({ level: 'tenant', database: null, object: null, column: null })).toBe('*.*');
    expect(grantTarget({ level: 'database', database: 'shop', object: null, column: null })).toBe('shop.*');
    expect(grantTarget({ level: 'column', database: 'shop', object: 'orders', column: 'total' })).toBe('shop.orders (total)');
  });
});
