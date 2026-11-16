// Dashboards end to end, against a gateway in catalog mode (tenant acme,
// user acme.app, HTTP listener on): make a dashboard from a query result,
// change its chart on the shelves, share it, check that a viewer runs its
// tiles with their own rights, move and remove tiles, and accept a tile the
// data assistant proposes.
//
// The assistant runs against a stub of an OpenAI-compatible provider that
// this spec serves on 127.0.0.1:$STUDIO_AI_STUB_PORT (nothing leaves the
// machine). The gateway needs a provider record pointing at it and
// ai.model.data naming it:
//
//   sprigctl ai provider add stub --kind openai --base-url http://127.0.0.1:$STUDIO_AI_STUB_PORT/v1 --no-key
//   sprigctl settings set ai.model.data stub/stub-model --scope global
//
// Without STUDIO_AI_STUB_PORT the assistant test is skipped.
//
// The spec makes and drops the database aidash_e2e, the user aidash_viewer
// and the dashboards it creates, by exact name or id. With DOCS_SHOTS=1 it
// also writes the documentation's screenshots (docs/img/studio/dashboards-*.png).
import { createServer, type Server } from 'node:http';
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { apiToken, signIn, sql, typeSql, openPage } from './fixtures';

const T = 'acme';
const DB = 'aidash_e2e';
const VIEWER = 'aidash_viewer';
const VIEWER_PW = 'aidash-Viewer-2026';
const STUB_PORT = process.env.STUDIO_AI_STUB_PORT;
const SHOTS = process.env.DOCS_SHOTS === '1';
const IMG = process.env.DOCS_IMG ?? '../docs/img/studio';

test.describe.configure({ mode: 'serial' });
test.use({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });

let token = '';
const made: string[] = [];

async function axe(page: Page, what: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  expect(r.violations.map((v) => `${what}: ${v.id} ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

async function shot(page: Page, name: string) {
  if (SHOTS) await page.screenshot({ path: `${IMG}/${name}.png` });
}

async function api(request: APIRequestContext, bearer: string, method: 'GET' | 'POST', path: string, data?: unknown) {
  const r = await request.fetch(path, { method, headers: { authorization: `Bearer ${bearer}` }, ...(data === undefined ? {} : { data }) });
  return { status: r.status(), body: (await r.json()) as Record<string, unknown> };
}

// ---- the stub provider ----

let stub: Server | null = null;
const STUB_TILE = {
  title: 'Average revenue by region',
  db: DB,
  sql: 'SELECT region, AVG(revenue) AS avg_revenue FROM sales GROUP BY region',
  chart: JSON.stringify({ mark: 'bar', columns: [{ field: 'region' }], rows: [{ field: 'avg_revenue', agg: 'sum' }], sort: { by: 'avg_revenue', dir: 'desc' } }),
};

function sse(chunks: unknown[]): string {
  return chunks.map((c) => `data: ${JSON.stringify(c)}\n\n`).join('') + 'data: [DONE]\n\n';
}

function chunk(delta: unknown, finish: string | null) {
  return { id: 'stub', object: 'chat.completion.chunk', model: 'stub-model', choices: [{ index: 0, delta, finish_reason: finish }] };
}

async function startStub(): Promise<void> {
  if (!STUB_PORT) return;
  stub = createServer((req, res) => {
    let body = '';
    req.on('data', (c: Buffer) => (body += c.toString()));
    req.on('end', () => {
      const b = JSON.parse(body || '{}') as { messages?: { role: string }[] };
      const answered = (b.messages ?? []).some((m) => m.role === 'tool');
      const usage = { id: 'stub', object: 'chat.completion.chunk', model: 'stub-model', choices: [], usage: { prompt_tokens: 50, completion_tokens: 20, total_tokens: 70 } };
      const out = answered
        ? [chunk({ role: 'assistant', content: "I proposed a bar chart of average revenue by region (one dimension and one measure). It hasn't been added." }, null), chunk({}, 'stop'), usage]
        : [
            chunk({ role: 'assistant', tool_calls: [{ index: 0, id: 'call_1', type: 'function', function: { name: 'propose_tile', arguments: '' } }] }, null),
            chunk({ tool_calls: [{ index: 0, function: { arguments: JSON.stringify(STUB_TILE) } }] }, null),
            chunk({}, 'tool_calls'),
            usage,
          ];
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.end(sse(out));
    });
  });
  await new Promise<void>((ok) => stub!.listen(Number(STUB_PORT), '127.0.0.1', ok));
}

// ---- setup ----

test.beforeAll(async ({ request }) => {
  token = await apiToken(request);
  await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB}`);
  await sql(request, token, '-', `CREATE DATABASE ${DB}`);
  await sql(request, token, DB, 'CREATE TABLE sales (id INT PRIMARY KEY, week DATE NOT NULL, region VARCHAR(20) NOT NULL, status VARCHAR(20) NOT NULL, revenue DECIMAL(10,2) NOT NULL, orders INT NOT NULL)');
  const regions = ['North', 'South', 'West'];
  const rows: string[] = [];
  let id = 1;
  for (let w = 0; w < 8; w++) {
    const day = new Date(Date.UTC(2026, 8, 7 + 7 * w)).toISOString().slice(0, 10);
    regions.forEach((r, i) => {
      const revenue = 1000 + w * 120 + i * 350 + ((w * 37 + i * 11) % 90);
      rows.push(`(${id++}, '${day}', '${r}', '${(w + i) % 5 === 0 ? 'refunded' : 'paid'}', ${revenue}, ${10 + w + i * 3})`);
    });
  }
  await sql(request, token, DB, `INSERT INTO sales VALUES ${rows.join(', ')}`);
  await sql(request, token, '-', `DROP USER IF EXISTS ${VIEWER}`);
  await sql(request, token, '-', `CREATE USER ${VIEWER} IDENTIFIED BY '${VIEWER_PW}'`);
  await sql(request, token, '-', `ALTER TENANT SPRIG SET (ai.enabled = TRUE)`);
  await startStub();
});

test.afterAll(async ({ request }) => {
  for (const id of made) await api(request, token, 'POST', `/v1/dashboards/${id}:delete`, {});
  await sql(request, token, '-', `DROP USER IF EXISTS ${VIEWER}`);
  await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB}`);
  await new Promise<void>((ok) => (stub ? stub.close(() => ok()) : ok()));
});

function dashboardId(page: Page): string {
  const m = /\/d\/(\d+)/.exec(page.url());
  expect(m).not.toBeNull();
  return m![1]!;
}

// ---- the flows ----

test('a query result becomes a tile; its chart changes on the shelves', async ({ page }) => {
  await signIn(page);
  await openPage(page, 'Query');
  await page.getByTestId('db-select').selectOption(DB);
  await typeSql(page, 'SELECT week, region, status, revenue, orders FROM sales');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('result-0')).toContainText('24 rows');
  await page.getByTestId('add-to-dashboard').click();
  const dlg = page.getByTestId('add-to-dashboard-dialog');
  await dlg.getByLabel('Dashboard', { exact: true }).selectOption('new');
  await dlg.getByLabel('New dashboard’s name').fill('Weekly sales');
  await dlg.getByLabel('Tile title').fill('Revenue by region, weekly');
  await dlg.getByRole('button', { name: 'Add and edit chart' }).click();

  // The chart editor: Show Me picked a line for a date and measures.
  const ed = page.getByTestId('chart-editor');
  await expect(ed).toBeVisible();
  await expect(page.getByTestId('mark-select')).toHaveValue('line');
  made.push(dashboardId(page));
  // Region on Colour, orders off the Rows shelf, then a stacked bar from Show Me.
  await page.getByTestId('add-region').selectOption('color');
  await expect(page.getByTestId('shelf-color')).toContainText('region');
  await page.getByTestId('shelf-rows').getByRole('button', { name: 'Remove SUM(orders)' }).click();
  await expect(page.getByTestId('showme-stacked')).toBeEnabled();
  await expect(page.getByTestId('showme-point')).toBeDisabled();
  await page.getByTestId('showme-stacked').click();
  await expect(page.getByTestId('mark-select')).toHaveValue('bar');
  await page.getByLabel('Date part of week').selectOption('week');
  await expect(ed.locator('.recharts-bar-rectangle').first()).toBeVisible();
  await axe(page, 'chart editor');
  await shot(page, 'dashboards-chart-editor');
  await page.getByTestId('save-tile').click();

  // Back on the dashboard: the stacked bars, one series per region.
  const tile = page.getByTestId('tile-Revenue by region, weekly');
  await expect(tile).toBeVisible();
  await expect(tile.locator('.recharts-bar-rectangle').first()).toBeVisible();
  await expect(tile.locator('.recharts-legend-item')).toHaveCount(3);
  await expect(page.getByTestId('share-badge')).toHaveText('Only me');
});

test('more tiles, a filter, edit mode, sharing', async ({ page, request }) => {
  await signIn(page);
  const id = made[0]!;
  await page.goto(`/studio/t/${T}/d/${id}`);
  // A KPI tile and a pivot, through the API the editor uses.
  for (const t of [
    { title: 'Revenue', sql: 'SELECT week, revenue FROM sales', chart: { mark: 'kpi', columns: [{ field: 'week', datePart: 'week' }], rows: [{ field: 'revenue', agg: 'sum' }] }, pos: { x: 0, y: 4, w: 3, h: 3 } },
    {
      title: 'Orders by region and status',
      sql: 'SELECT region, status, orders FROM sales',
      chart: { mark: 'pivot', rows: [{ field: 'region' }, { field: 'status' }], columns: [{ field: 'orders', agg: 'sum' }] },
      pos: { x: 3, y: 4, w: 5, h: 5 },
    },
  ]) {
    const r = await api(request, token, 'POST', `/v1/dashboards/${id}/tiles`, { ...t, database: DB });
    expect(r.status, JSON.stringify(r.body)).toBe(200);
  }
  await page.reload();
  await expect(page.getByTestId('tile-Revenue').getByTestId('kpi-value')).toBeVisible();
  await expect(page.getByTestId('tile-Orders by region and status').getByRole('rowheader', { name: 'Subtotal' }).first()).toBeVisible();

  // Edit mode: a date filter the tiles may use, then move a tile with the keyboard.
  await page.getByTestId('edit-dashboard').click();
  await page.getByTestId('add-filter').click();
  await page.getByLabel('Filter name in queries').fill('from');
  await page.getByLabel('Filter label').fill('From');
  await page.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(page.getByRole('group', { name: 'Dashboard filters' }).getByLabel('From', { exact: true })).toBeVisible();
  const kpi = page.getByTestId('tile-Revenue');
  await kpi.getByTestId('tile-grip').focus();
  await page.keyboard.press('ArrowRight');
  await page.keyboard.press('Shift+ArrowRight');
  await expect.poll(async () => (await api(request, token, 'GET', `/v1/dashboards/${id}`)).body.tiles).toContainEqual(expect.objectContaining({ title: 'Revenue', pos: { x: 1, y: 4, w: 4, h: 3 } }));
  // The pivot it now overlaps moved down below it.
  const after = (await api(request, token, 'GET', `/v1/dashboards/${id}`)).body.tiles as { title: string; pos: { y: number } }[];
  expect(after.find((t) => t.title === 'Orders by region and status')!.pos.y).toBe(7);
  // Drag the KPI back with the pointer.
  const grip = kpi.getByTestId('tile-grip');
  const box = (await grip.boundingBox())!;
  await page.mouse.move(box.x + 5, box.y + 5);
  await page.mouse.down();
  const cell = (await page.getByTestId('dash-grid').boundingBox())!.width / 12;
  await page.mouse.move(box.x + 5 - cell, box.y + 5, { steps: 5 });
  await page.mouse.up();
  await expect.poll(async () => ((await api(request, token, 'GET', `/v1/dashboards/${id}`)).body.tiles as { title: string; pos: { x: number } }[]).find((t) => t.title === 'Revenue')!.pos.x).toBe(0);
  await axe(page, 'dashboard, edit mode');
  await page.getByTestId('edit-dashboard').click();

  // A tidy layout for the rest (and the screenshots).
  const tiles = (await api(request, token, 'GET', `/v1/dashboards/${id}`)).body.tiles as { id: string; title: string }[];
  const at: Record<string, { x: number; y: number; w: number; h: number }> = {
    'Revenue by region, weekly': { x: 0, y: 0, w: 7, h: 5 },
    Revenue: { x: 7, y: 0, w: 5, h: 3 },
    'Orders by region and status': { x: 7, y: 3, w: 5, h: 6 },
  };
  const laid = await api(request, token, 'POST', `/v1/dashboards/${id}`, { positions: tiles.map((t) => ({ tile: t.id, pos: at[t.title] })) });
  expect(laid.status, JSON.stringify(laid.body)).toBe(200);
  await page.reload();
  await expect(page.getByTestId('tile-Revenue').getByTestId('kpi-value')).toBeVisible();

  // Shared with the tenant.
  await page.getByTestId('share-select').selectOption('tenant');
  await expect(page.getByTestId('share-badge')).toHaveText('Tenant');
  await shot(page, 'dashboards-grid');
});

test('a viewer of a shared dashboard runs its tiles with their own rights', async ({ request }) => {
  const id = made[0]!;
  const r = await request.post('/v1/auth/login', { data: { login: `${T}.${VIEWER}`, password: VIEWER_PW, refresh_transport: 'body', client: 'api' } });
  expect(r.status()).toBe(200);
  const viewer = ((await r.json()) as { access_token: string }).access_token;
  const list = await api(request, viewer, 'GET', '/v1/dashboards');
  const shared = list.body.shared as { id: string; can_edit: boolean }[];
  expect(shared.find((d) => d.id === id)).toMatchObject({ can_edit: false });
  const d = await api(request, viewer, 'GET', `/v1/dashboards/${id}`);
  const tile = (d.body.tiles as { id: string; title: string }[]).find((t) => t.title === 'Revenue')!;
  // No grant on the database: the owner's rights don't carry over.
  const denied = await api(request, viewer, 'POST', `/v1/dashboards/${id}/tiles/${tile.id}:run`, { params: {} });
  expect(denied.status).toBeGreaterThanOrEqual(400);
  expect(JSON.stringify(denied.body)).toMatch(/denied|1142|1044/i);
  // A viewer can't change it either.
  const change = await api(request, viewer, 'POST', `/v1/dashboards/${id}`, { name: 'Taken' });
  expect(change.status).toBeGreaterThanOrEqual(400);
  // With a grant of their own, the same tile runs.
  await sql(request, token, '-', `GRANT SELECT ON ${DB}.sales TO ${VIEWER}`);
  const ok = await api(request, viewer, 'POST', `/v1/dashboards/${id}/tiles/${tile.id}:run`, { params: {} });
  expect(ok.status, JSON.stringify(ok.body)).toBe(200);
  expect(ok.body.row_count).toBe(24);
  // Filter values are bound as literals: a quote can't end the string.
  const bad = await api(request, token, 'POST', '/v1/dashboards:run', {
    database: DB,
    sql: 'SELECT COUNT(*) AS n FROM sales WHERE region = {{region}}',
    filters: [{ name: 'region', kind: 'text', value: "North' OR '1'='1" }],
  });
  expect(bad.status, JSON.stringify(bad.body)).toBe(200);
  expect(String((bad.body.rows as unknown[][])[0]![0])).toBe('0');
  const write = await api(request, token, 'POST', '/v1/dashboards:run', { database: DB, sql: 'DELETE FROM sales' });
  expect(write.status).toBeGreaterThanOrEqual(400);
});

test('the ask bar previews the assistant’s tile, and Accept adds it', async ({ page }) => {
  test.skip(!STUB_PORT, 'needs STUDIO_AI_STUB_PORT and a provider record pointing at it');
  await signIn(page);
  const id = made[0]!;
  await page.goto(`/studio/t/${T}/d/${id}`);
  await page.getByTestId('ask-tile').fill('average revenue by region');
  await page.getByRole('button', { name: 'Ask', exact: true }).click();
  const preview = page.getByTestId('tile-preview');
  await expect(preview).toContainText('Average revenue by region', { timeout: 30_000 });
  await expect(preview.locator('.recharts-bar-rectangle').first()).toBeVisible();
  await preview.scrollIntoViewIfNeeded();
  await shot(page, 'dashboards-ask');
  await axe(page, 'dashboard with a preview');
  await page.getByTestId('preview-accept').click();
  await expect(preview).toHaveCount(0);
  await expect(page.getByTestId('tile-Average revenue by region')).toBeVisible();
  await expect(page.getByTestId('tile-Average revenue by region').locator('.recharts-bar-rectangle')).toHaveCount(3);
});

test('dark theme, tile menu, and removing a tile', async ({ page }) => {
  await signIn(page);
  const id = made[0]!;
  await page.emulateMedia({ colorScheme: 'dark' });
  await page.goto(`/studio/t/${T}/d/${id}`);
  await expect(page.getByTestId('tile-Revenue').getByTestId('kpi-value')).toBeVisible();
  await axe(page, 'dashboard, dark');
  const pivot = page.getByTestId('tile-Orders by region and status');
  await pivot.getByTestId('tile-menu').click();
  await expect(page.getByRole('menuitem', { name: 'Open in a query tab' })).toBeVisible();
  page.once('dialog', (d) => void d.accept());
  await page.getByRole('menuitem', { name: 'Remove' }).click();
  await expect(pivot).toHaveCount(0);
});
