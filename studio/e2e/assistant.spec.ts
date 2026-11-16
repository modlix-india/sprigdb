// The assistant dock, entry points and inline ask, against a stubbed API:
// every /v1 call is answered here (the chat from a fixture NDJSON file), so
// no gateway or AI provider is needed. Run it against Vite's dev server:
//
//   cd studio
//   pnpm exec vite --port 26473 --strictPort &
//   STUDIO_URL=http://127.0.0.1:26473 pnpm exec playwright test e2e/assistant.spec.ts --project chromium
//
// With DOCS_SHOTS=1 it also writes the documentation's screenshots of the
// dock and a proposal card (docs/img/studio/), and of the TUI's assistant
// pane when SPRIG_TUI_HTML names the HTML snapshot the sprig-shell test
// `html_snapshots` wrote (docs/img/sprig/).
import { readFileSync } from 'node:fs';
import { expect, test, type Page, type Route } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const FIXTURE = readFileSync(new URL('./fixtures/ai-chat-dba.ndjson', import.meta.url), 'utf8');
const PROPOSAL = '0193f0aa-7c1e-7b2a-9e41-5d3c2b1a0f01';

interface Ai {
  enabled: boolean;
  share_data: boolean;
  roles: string[];
}

interface Seen {
  method: string;
  path: string;
  body: unknown;
}

const ME = (ai: Ai | undefined) => ({
  principal: { tenant: 'acme', tenant_id: '7', user: 'app', host: '%', login: 'acme.app' },
  amr: ['pwd'],
  session: { id: 's1', client: 'studio', created_at_ms: String(Date.now()), idle_minutes: 60, max_hours: 12, access_ttl_seconds: 900 },
  tenants: [{ name: 'acme', id: '7', path: 'acme', relation: 'home' }],
  grants: ['GRANT ALL PRIVILEGES ON *.* TO `app`@`%`'],
  privileges: { tenant_admin: true, settings_admin: true, create_user: true, grant_option: true, index_all: true, index_dbs: [] },
  platform_roles: [],
  studio: {
    session_idle_minutes: 60,
    session_max_hours: 12,
    persist_drafts: false,
    history_max: 100,
    max_result_rows: 1000,
    max_result_mb: 16,
    statement_timeout_ms: 30000,
    confirm_writes: 'ddl',
    export_ticket_seconds: 60,
    import_batch_rows: 500,
    import_max_mb: 16,
    feed_view_max_events: 100,
    graph_max_elements: 500,
    map_tile_url: '',
    view_as: 'off',
    operator_mode: 'off',
  },
  gateway: { id: 'gw-1', dc: 'dc-a', studio_build: 'dev' },
  ...(ai ? { ai } : {}),
});

const SNAPSHOT = {
  db: 'shop',
  objects: [
    { name: 'orders', kind: 'table', rows: 2104880 },
    { name: 'customers', kind: 'table', rows: 40210 },
    { name: 'order_items', kind: 'table', rows: 6100000 },
  ],
  columns: {
    orders: [
      { name: 'id', type: 'bigint' },
      { name: 'status', type: 'varchar(20)' },
      { name: 'total', type: 'decimal(10,2)' },
      { name: 'created_at', type: 'datetime' },
    ],
  },
};

const STORED = {
  conversation: { id: '41', role: 'dba', title: 'Why is this report slow?', created_ms: Date.now(), updated_ms: Date.now() },
  messages: [
    { id: '900', kind: 'user', body: { text: 'Why is this report slow?' } },
    {
      id: '901',
      kind: 'system_note',
      body: {
        sha256: '9f2c4e1ab0d34c7f8e5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f80912',
        provider: 'anthropic',
        model: 'claude-opus-5-5',
        usage: { input_tokens: 5120, output_tokens: 412 },
        payload: {
          model: 'claude-opus-5-5',
          system: [{ type: 'text', text: 'You are the DBA assistant of SprigDB…' }],
          messages: [{ role: 'user', content: [{ type: 'text', text: 'Why is this report slow?' }] }],
        },
      },
      sent_hash: '9f2c4e1ab0d34c7f8e5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f80912',
    },
    { id: '902', kind: 'assistant', body: { role: 'assistant', content: [{ type: 'text', text: 'The query reads all 2.1 M orders…' }] } },
  ],
};

/** Answers every /v1 call; returns what was asked. */
async function stubApi(page: Page, o: { ai?: Ai; chat?: (body: Record<string, unknown>) => string } = {}): Promise<Seen[]> {
  const seen: Seen[] = [];
  await page.route('**/studio/version.json', (r) => r.fulfill({ json: { build_version: 'dev' } }));
  await page.route('**/studio/_/errors', (r) => r.fulfill({ status: 204 }));
  await page.route('**/v1/**', async (route: Route) => {
    const req = route.request();
    const url = new URL(req.url());
    const path = url.pathname;
    let body: unknown;
    try {
      body = req.postDataJSON();
    } catch {
      body = req.postData();
    }
    seen.push({ method: req.method(), path: path + url.search, body });
    const ok = (json: unknown) => route.fulfill({ json });
    if (path === '/v1/auth/refresh') return ok({ access_token: 'tok', token_type: 'Bearer', expires_in: 3600, session_id: 's1' });
    if (path === '/v1/auth/me') return ok(ME(o.ai));
    if (path === '/v1/catalog') return ok({ databases: ['shop'] });
    if (path === '/v1/databases/shop/catalog') return ok(SNAPSHOT);
    if (path.startsWith('/v1/library')) return ok({ saved: [], history: [] });
    if (path.startsWith('/v1/recommendations')) return ok({ items: [], recommendations: [] });
    if (path.endsWith('/sql:check')) return ok({ statements: [] });
    if (path.endsWith('/sql:query')) {
      return route.fulfill({
        status: 400,
        json: { code: 1054, name: 'ER_BAD_FIELD_ERROR', message: "Unknown column 'totl' in 'field list'", sqlstate: '42S22' },
      });
    }
    if (path === '/v1/ai/chat') {
      const text = o.chat ? o.chat(body as Record<string, unknown>) : FIXTURE;
      return route.fulfill({ status: 200, headers: { 'content-type': 'application/x-ndjson' }, body: text });
    }
    if (path === '/v1/ai/conversations') return ok({ conversations: [STORED.conversation] });
    if (path.startsWith('/v1/ai/conversations/41')) return ok(STORED);
    if (path === '/v1/ai/feedback') return ok({ recorded: true });
    if (path === `/v1/ai/proposals/${PROPOSAL}:accept`) {
      return ok({
        proposal: PROPOSAL,
        kind: 'sql',
        outcome: 'accepted',
        database: 'shop',
        ran: [{ sql: 'CREATE INDEX orders_status_created ON shop.orders (status, created_at) ALGORITHM=INPLACE LOCK=NONE', results: [{ columns: [], rows: [], affected_rows: '0' }] }],
      });
    }
    if (path.includes('/v1/ai/proposals/') && path.endsWith(':dismiss')) return ok({ proposal: 'x', kind: 'sql', outcome: 'dismissed' });
    if (path === '/v1/docs:search') {
      return ok({
        version: '0.9.0',
        mode: 'hybrid',
        hits: [
          {
            chunk_id: 'c1',
            page: 'tools/automatic-indexing.md',
            title: 'Automatic indexing',
            heading_path: 'Automatic indexing > When an index is proposed',
            audience: ['user', 'dba'],
            text: 'SprigDB proposes an index when a statement runs **50 times** at p95 above 100 ms over 24 hours.',
            score: 0.82,
          },
        ],
      });
    }
    return ok({});
  });
  return seen;
}

async function open(page: Page, path = '/studio/'): Promise<void> {
  await page.goto(path);
  await expect(page.getByTestId('whoami')).toHaveText('acme.app');
}

async function axe(page: Page): Promise<void> {
  const r = await new AxeBuilder({ page })
    .include('[data-testid=assistant-dock]')
    .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
    .analyze();
  expect(r.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

const ON: Ai = { enabled: true, share_data: false, roles: ['data', 'dba'] };

test('the dock streams an answer; a proposal is confirmed and accepted; feedback and what was sent', async ({ page }) => {
  const seen = await stubApi(page, { ai: ON });
  await open(page, '/studio/t/acme/db/shop');
  // Ctrl/Cmd+I opens it, with the page's database as a chip and an empty state.
  await page.keyboard.press('ControlOrMeta+i');
  const dock = page.getByTestId('assistant-dock');
  await expect(dock).toBeVisible();
  await expect(dock.getByRole('textbox', { name: 'Ask the assistant' })).toBeFocused();
  await expect(dock.getByTestId('ai-data-badge')).toHaveText(/Schema only/);
  await expect(dock.getByRole('listitem').filter({ hasText: 'shop' })).toBeVisible();
  await expect(dock.getByText('Ask me about shop')).toBeVisible();
  await axe(page);

  await dock.getByRole('button', { name: 'DBA', exact: true }).click();
  await dock.getByRole('textbox', { name: 'Ask the assistant' }).fill('Why is this report slow?');
  await page.keyboard.press('Enter');
  await expect(dock.getByText(/no index starts with/)).toBeVisible();
  const chat = seen.find((s) => s.path === '/v1/ai/chat')!;
  expect(chat.body).toEqual({ role: 'dba', message: 'Why is this report slow?', database: 'shop' });
  // Tool calls as quiet rows; the result shown to the user; the shape note.
  await expect(dock.getByRole('button', { name: /Ran read-only query/ })).toBeVisible();
  await expect(dock.getByText('The assistant saw only their shape', { exact: false })).toBeVisible();
  await expect(dock.getByRole('region', { name: 'Query result' }).getByRole('cell', { name: 'shipped' })).toBeVisible();
  // The proposal card: statement, estimate and what was checked.
  const card = dock.getByTestId('ai-proposal');
  await expect(card).toContainText('CREATE INDEX orders_status_created');
  await expect(card).toContainText('p95 4.2 s → ~40 ms');
  await expect(card).toContainText('passed: 1 statement');
  await expect(page.getByTestId('ai-live')).toHaveText(/1 proposal to review/);
  await axe(page);

  // Review & run: the usual confirm dialog, then :accept with confirm.
  await card.getByRole('button', { name: 'Review & run' }).click();
  const dialog = page.getByTestId('confirm-dialog');
  await expect(dialog).toBeVisible();
  await expect(dialog).toContainText('CREATE INDEX orders_status_created');
  await expect(dialog.getByRole('button', { name: 'Don’t run' })).toBeFocused();
  await dialog.getByRole('button', { name: 'Run it' }).click();
  await expect(card).toContainText('Done');
  const accept = seen.find((s) => s.path === `/v1/ai/proposals/${PROPOSAL}:accept`)!;
  expect(accept.body).toEqual({ confirm: true });

  // 👍: the answer's stored id is looked up, then the vote is sent.
  await dock.getByRole('button', { name: 'Helpful', exact: true }).click();
  await expect(dock.getByRole('button', { name: 'Helpful', exact: true })).toHaveAttribute('aria-pressed', 'true');
  expect(seen.find((s) => s.path === '/v1/ai/feedback')!.body).toEqual({ message_id: '902', vote: 'up' });

  // What was sent: the exact payload and its hash.
  await dock.getByRole('button', { name: 'What was sent' }).click();
  const sent = dock.getByTestId('ai-sent');
  await expect(sent).toContainText('sha256 9f2c4e1ab0d34c7f');
  await sent.getByText('The exact request').click();
  await expect(sent.getByLabel('Request of call 1')).toContainText('You are the DBA assistant');
  expect(seen.some((s) => s.path === '/v1/ai/conversations/41?payloads=true')).toBe(true);
  await axe(page);
  await sent.getByRole('button', { name: 'Back' }).click();

  // Conversations, then Esc closes and gives focus back.
  await dock.getByRole('button', { name: 'Conversations' }).click();
  await expect(dock.getByTestId('ai-conversations')).toContainText('Why is this report slow?');
  await page.keyboard.press('Escape');
  await expect(dock).toBeHidden();
});

test('dark theme: the dock passes axe', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await stubApi(page, { ai: { ...ON, share_data: true } });
  await open(page, '/studio/t/acme/db/shop');
  await page.getByTestId('status-assistant').click();
  const dock = page.getByTestId('assistant-dock');
  await expect(dock.getByTestId('ai-data-badge')).toHaveText(/Data shared/);
  await dock.getByRole('button', { name: 'DBA', exact: true }).click();
  await dock.getByRole('textbox', { name: 'Ask the assistant' }).fill('Why is this report slow?');
  await page.keyboard.press('Enter');
  await expect(dock.getByTestId('ai-proposal')).toBeVisible();
  await axe(page);
});

test('not configured: one line, and no AI controls elsewhere', async ({ page }) => {
  await stubApi(page, { ai: { enabled: false, share_data: false, roles: [] } });
  await open(page, '/studio/t/acme/q/new');
  await expect(page.getByTestId('sql-editor')).toBeVisible();
  await expect(page.getByTestId('ask-for-query')).toHaveCount(0);
  await page.getByTestId('status-assistant').click();
  const dock = page.getByTestId('assistant-dock');
  await expect(dock.getByTestId('ai-setup')).toContainText('Ask your operator to set up an AI provider.');
  await expect(dock.getByRole('textbox')).toHaveCount(0);
  await axe(page);
  // The palette has no "Ask the assistant" row, but still searches the docs.
  await page.keyboard.press('Escape');
  await page.keyboard.press('ControlOrMeta+k');
  await page.getByRole('combobox', { name: /Type to search/ }).fill('indexes');
  await expect(page.getByRole('option', { name: /Ask the assistant/ })).toHaveCount(0);
  await expect(page.getByRole('option', { name: /Search docs: indexes/ })).toBeVisible();
});

test('entry points: palette, Explain this error, Ask for a query, docs', async ({ page }) => {
  const seen = await stubApi(page, { ai: ON });
  await open(page, '/studio/t/acme/q/new');
  // Ask for a query, in an empty tab: the composer gets the focus.
  await page.getByTestId('ask-for-query').click();
  const dock = page.getByTestId('assistant-dock');
  await expect(dock.getByRole('textbox', { name: 'Ask the assistant' })).toBeFocused();
  await page.keyboard.press('Escape');

  // The palette's "Ask the assistant: …" sends the question.
  await page.keyboard.press('ControlOrMeta+k');
  await page.getByRole('combobox', { name: /Type to search/ }).fill('?top customers by revenue');
  await page.getByRole('option', { name: 'Ask the assistant: top customers by revenue' }).click();
  await expect(dock.getByText(/no index starts with/)).toBeVisible();
  expect(seen.filter((s) => s.path === '/v1/ai/chat').at(-1)!.body).toMatchObject({ role: 'data', message: 'top customers by revenue' });

  // Explain this error: the statement and the error go along as chips.
  await page.keyboard.press('Escape');
  await page.getByTestId('db-select').selectOption('shop');
  const ed = page.getByTestId('sql-editor');
  await ed.click();
  await page.keyboard.insertText('SELECT totl FROM orders');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('query-error')).toContainText('Unknown column');
  await page.getByTestId('ask-explain-error').click();
  await expect(dock.getByText(/no index starts with/)).toBeVisible();
  const asked = seen.filter((s) => s.path === '/v1/ai/chat').at(-1)!.body as { message: string; context: { kind: string; ref: string }[] };
  expect(asked.message).toBe('Explain this error and how to fix my query.');
  expect(asked.context.map((c) => c.kind)).toEqual(['query', 'note']);
  expect(asked.context[1]!.ref).toContain("Unknown column 'totl'");

  // Search docs: the Docs panel with links to the published pages.
  await page.keyboard.press('Escape');
  await page.keyboard.press('ControlOrMeta+k');
  await page.getByRole('combobox', { name: /Type to search/ }).fill('automatic indexing');
  await page.getByRole('option', { name: 'Search docs: automatic indexing' }).click();
  const hit = page.getByTestId('docs-hit');
  await expect(hit).toContainText('50 times');
  await expect(hit.getByRole('link')).toHaveAttribute('href', /\/docs\/tools\/automatic-indexing\/#when-an-index-is-proposed$/);
});

test('Ctrl/Cmd+I in a query tab proposes a diff: Tab accepts it', async ({ page }) => {
  const newSql = "SELECT id, total\nFROM orders\nWHERE status = 'paid'";
  await stubApi(page, {
    ai: ON,
    chat: () =>
      [
        { type: 'start', conversation: '50', role: 'data', model: 'stub', share_data: false },
        { type: 'proposal', proposal: { id: 'p-inline', kind: 'sql', body: { db: 'shop', sql: newSql, why: 'Only paid orders.' } }, filed: null },
        { type: 'end', stop: 'end_turn' },
      ]
        .map((e) => JSON.stringify(e))
        .join('\n') + '\n',
  });
  await open(page, '/studio/t/acme/q/new');
  const ed = page.getByTestId('sql-editor');
  await ed.click();
  await page.keyboard.insertText('SELECT id, total\nFROM orders');
  await page.keyboard.press('ControlOrMeta+i');
  const inline = page.getByTestId('ai-inline');
  await inline.getByRole('textbox').fill('only paid orders');
  await page.keyboard.press('Enter');
  const diff = page.getByTestId('ai-diff');
  await expect(diff).toContainText("WHERE status = 'paid'");
  await expect(diff.locator('.add')).toHaveCount(1);
  await page.keyboard.press('Tab');
  await expect(inline).toBeHidden();
  await expect(ed).toContainText("WHERE status = 'paid'");
});

test('docs screenshots of the dock and a proposal card', async ({ page }) => {
  test.skip(process.env.DOCS_SHOTS !== '1', 'docs screenshots run only with DOCS_SHOTS=1');
  const out = process.env.DOCS_IMG ?? '../docs/img/studio';
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.emulateMedia({ colorScheme: 'light' });
  await stubApi(page, { ai: ON });
  // A slow report in a query tab, with its result (fictitious numbers).
  await page.route('**/v1/databases/shop/sql:query', (r) =>
    r.fulfill({
      status: 200,
      headers: { 'content-type': 'application/x-ndjson' },
      body:
        [
          { header: { index: 0, columns: [{ name: 'status', type: 'VARCHAR', mysql_type: 253 }, { name: 'n', type: 'BIGINT', mysql_type: 8 }, { name: 'revenue', type: 'DECIMAL', mysql_type: 246 }] } },
          { rows: { rows: [['paid', '18402', '1204330.50'], ['shipped', '4977', '301118.00']] } },
          { end: { index: 0, affected_rows: '0' } },
        ]
          .map((x) => JSON.stringify(x))
          .join('\n') + '\n',
    }),
  );
  await open(page, '/studio/t/acme/q/new');
  await page.getByTestId('db-select').selectOption('shop');
  await page.getByTestId('sql-editor').click();
  await page.keyboard.insertText(
    "SELECT status, COUNT(*) AS n, SUM(total) AS revenue\nFROM orders\nWHERE status IN ('paid', 'shipped') AND created_at >= NOW() - INTERVAL 30 DAY\nGROUP BY status;",
  );
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByText('1204330.50')).toBeVisible();
  await page.keyboard.press('ControlOrMeta+i');
  const dock = page.getByTestId('assistant-dock');
  await dock.getByRole('button', { name: 'DBA', exact: true }).click();
  await dock.getByRole('textbox', { name: 'Ask the assistant' }).fill('Why is this report slow?');
  await page.keyboard.press('Enter');
  const card = dock.getByTestId('ai-proposal');
  await expect(card.locator('.tk-keyword').first()).toBeVisible();
  await card.scrollIntoViewIfNeeded();
  await dock.getByRole('textbox', { name: 'Ask the assistant' }).blur();
  await page.screenshot({ path: `${out}/assistant-dock.png` });
  await card.screenshot({ path: `${out}/assistant-proposal.png` });
  const tui = process.env.SPRIG_TUI_HTML;
  if (tui) {
    await page.setViewportSize({ width: 1000, height: 640 });
    await page.goto(`file://${tui}`);
    await page.locator('pre').screenshot({ path: process.env.SPRIG_TUI_PNG ?? '../docs/img/sprig/tui-assistant.png' });
  }
});
