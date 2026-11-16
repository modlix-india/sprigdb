import { describe, expect, it } from 'vitest';
import { parseInline, parseMarkdown } from './mdparse';
import { lineDiff } from './diff';
import { docsUrl, slug } from './docs';
import { chipsFor } from './context';
import { availableRoles } from './store';
import { suggestions } from './AssistantDock';
import { inlineMessage } from './InlineAsk';
import { tokenClass } from './CodeView';

describe('markdown', () => {
  it('parses paragraphs, lists, headings, quotes and fences (open ones too)', () => {
    const md = '# Plan\nFirst line\nsecond\n\n- one\n- two\n  more\n1. a\n2. b\n> quoted\n```sql\nSELECT 1\n```\nafter\n```\nunfinished';
    expect(parseMarkdown(md)).toEqual([
      { type: 'h', level: 1, text: 'Plan' },
      { type: 'p', text: 'First line\nsecond' },
      { type: 'ul', items: ['one', 'two more'] },
      { type: 'ol', items: ['a', 'b'] },
      { type: 'quote', text: 'quoted' },
      { type: 'code', lang: 'sql', text: 'SELECT 1' },
      { type: 'p', text: 'after' },
      { type: 'code', lang: '', text: 'unfinished' },
    ]);
  });

  it('parses inline marks and only http links', () => {
    expect(parseInline('use `idx` **now** and *then* [docs](https://x.example/a) [bad](javascript:alert(1))')).toEqual([
      { type: 'text', text: 'use ' },
      { type: 'code', text: 'idx' },
      { type: 'text', text: ' ' },
      { type: 'strong', text: 'now' },
      { type: 'text', text: ' and ' },
      { type: 'em', text: 'then' },
      { type: 'text', text: ' ' },
      { type: 'link', text: 'docs', href: 'https://x.example/a' },
      { type: 'text', text: ' [bad](javascript:alert(1))' },
    ]);
  });
});

describe('lineDiff', () => {
  it('keeps common lines and marks changes', () => {
    expect(lineDiff('SELECT a\nFROM t\nLIMIT 5', 'SELECT a, b\nFROM t\nWHERE b > 0\nLIMIT 5')).toEqual([
      { op: 'del', text: 'SELECT a' },
      { op: 'add', text: 'SELECT a, b' },
      { op: 'same', text: 'FROM t' },
      { op: 'add', text: 'WHERE b > 0' },
      { op: 'same', text: 'LIMIT 5' },
    ]);
    expect(lineDiff('', 'SELECT 1')).toEqual([
      { op: 'del', text: '' },
      { op: 'add', text: 'SELECT 1' },
    ]);
  });
});

describe('docs links', () => {
  it('maps docs paths to published pages and anchors', () => {
    expect(docsUrl('tools/studio.md', 'The assistant dock')).toBe('https://modlix-india.github.io/sprigdb/docs/tools/studio/#the-assistant-dock');
    expect(docsUrl('operate/monitoring')).toBe('https://modlix-india.github.io/sprigdb/docs/operate/monitoring/');
    expect(docsUrl('README.md')).toBe('https://modlix-india.github.io/sprigdb/docs/');
    expect(slug('`ai.share_data` and you!')).toBe('aishare_data-and-you');
  });
});

describe('context chips', () => {
  const tabs = [{ id: 'q1', title: 'Query 1', sql: 'SELECT 1', db: 'shop' }];
  it('offers the database, table and query tab of the page', () => {
    expect(chipsFor('db/shop/obj/table/orders', tabs).map((c) => c.id)).toEqual(['db:shop', 'table:shop.orders']);
    expect(chipsFor('q/q1', tabs).map((c) => [c.kind, c.ref])).toEqual([
      ['database', 'shop'],
      ['query', 'SELECT 1'],
    ]);
    expect(chipsFor('settings', tabs)).toEqual([]);
  });
});

describe('roles and prompts', () => {
  it('offers tenant roles only while the assistants are on', () => {
    expect(availableRoles(undefined)).toEqual([]);
    expect(availableRoles({ enabled: false, roles: ['data', 'dba', 'admin'] })).toEqual(['admin']);
    expect(availableRoles({ enabled: true, roles: ['data', 'dba'] })).toEqual(['data', 'dba']);
  });

  it('suggests questions from the schema', () => {
    expect(suggestions('data', 'shop', ['orders', 'customers'])[2]).toBe('How many orders does each customer have?');
    expect(suggestions('dba', 'shop', ['orders'])[0]).toBe("What's slow in shop right now?");
    expect(inlineMessage('add a filter', true)).toMatch(/^Change the query in my editor: add a filter/);
  });

  it('maps Monaco token types to the editor colours', () => {
    expect(tokenClass('keyword.sql')).toBe('tk-keyword');
    expect(tokenClass('delimiter.parenthesis.sql')).toBe('tk-operator');
    expect(tokenClass('identifier.sql')).toBeUndefined();
  });
});
