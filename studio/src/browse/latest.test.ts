import { describe, expect, it } from 'vitest';
import { latestOnly } from './latest';

/** A promise and the functions that settle it. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((ok, no) => {
    resolve = ok;
    reject = no;
  });
  return { promise, resolve, reject };
}

describe('loads where only the newest counts', () => {
  it('drop an older load that finishes last (a reload after a save, overtaken by a filter)', async () => {
    const l = latestOnly();
    const shown: string[] = [];
    const reload = deferred<string>();
    const filtered = deferred<string>();
    const a = l.run(() => reload.promise, (v) => shown.push(v), () => shown.push('error'));
    const b = l.run(() => filtered.promise, (v) => shown.push(v), () => shown.push('error'));
    filtered.resolve('row 17');
    reload.resolve('every row');
    await Promise.all([a, b]);
    expect(shown).toEqual(['row 17']);
  });

  it('apply the newest even when it finishes first, and drop an older failure', async () => {
    const l = latestOnly();
    const shown: string[] = [];
    const old = deferred<string>();
    const a = l.run(() => old.promise, (v) => shown.push(v), () => shown.push('error'));
    const b = l.run(() => Promise.resolve('new'), (v) => shown.push(v), () => shown.push('error'));
    await b;
    old.reject(new Error('late'));
    await a;
    expect(shown).toEqual(['new']);
  });

  it('drop what is running when cancelled', async () => {
    const l = latestOnly();
    const shown: string[] = [];
    const p = deferred<string>();
    const a = l.run(() => p.promise, (v) => shown.push(v), () => shown.push('error'));
    l.cancel();
    p.resolve('late');
    await a;
    expect(shown).toEqual([]);
  });
});
