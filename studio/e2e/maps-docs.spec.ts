// Point and filled maps on dashboards and the Docs panel's own pages, against a
// stubbed API: every /v1 call is answered here, so no gateway is needed.
// Run it against Vite's dev server:
//
//   cd studio
//   pnpm exec vite --port 26473 --strictPort &
//   STUDIO_URL=http://127.0.0.1:26473 pnpm exec playwright test e2e/maps-docs.spec.ts --project chromium
//
// The map checks that, without studio.map_tile_url, nothing is fetched from
// anywhere but Studio's own origin (the country shapes included, and only
// for a filled map); with it, tiles come from that server.
import { expect, test, type Page, type Route } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

// MapLibre needs WebGL; headless Chrome draws it in software with this flag.
test.use({ launchOptions: { args: ['--enable-unsafe-swiftshader', '--use-angle=swiftshader'] } });

const TILES = 'https://tiles.example.test/{z}/{x}/{y}.png';
// A 1x1 PNG.
const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');

const ME = (tiles: string) => ({
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
    map_tile_url: tiles,
    view_as: 'off',
    operator_mode: 'off',
  },
  gateway: { id: 'gw-1', dc: 'dc-a', studio_build: 'dev' },
});

const POS = { x: 0, y: 0, w: 12, h: 8 };
const DASH = {
  id: 'd1',
  name: 'Stores',
  owner: 'app',
  shared: false,
  mine: true,
  can_edit: true,
  updated_ms: Date.now(),
  filters: [],
  tiles: [
    {
      id: 't1',
      title: 'Sales by store',
      database: 'shop',
      sql: 'SELECT city, lat, lng, region, revenue FROM stores',
      chart: { mark: 'map-point', columns: ['lng'], rows: ['lat'], size: { field: 'revenue', agg: 'sum' }, color: 'region', tooltip: ['city'] },
      refresh_seconds: 0,
      pos: POS,
    },
    {
      id: 't2',
      title: 'Sales by country',
      database: 'shop',
      sql: 'SELECT country, revenue FROM stores',
      chart: { mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] },
      refresh_seconds: 0,
      pos: { ...POS, y: 8, h: 4 },
    },
  ],
};

const RUN = {
  columns: [
    { name: 'city', type: 'VARCHAR' },
    { name: 'lat', type: 'DECIMAL' },
    { name: 'lng', type: 'DECIMAL' },
    { name: 'region', type: 'VARCHAR' },
    { name: 'revenue', type: 'DECIMAL' },
  ],
  rows: [
    ['Pune', '18.52', '73.85', 'West', '150'],
    ['Delhi', '28.61', '77.21', 'North', '300'],
    ['Chennai', '13.08', '80.27', 'South', '20'],
  ],
  row_count: 3,
  truncated: false,
  ms: 3,
};

// Countries as names and codes (Singapore and Malta are small ones); two
// rows match nothing.
const RUN2 = {
  columns: [
    { name: 'country', type: 'VARCHAR' },
    { name: 'revenue', type: 'DECIMAL' },
  ],
  rows: [
    ['IN', '150'],
    ['India', '300'],
    ['USA', '220'],
    ['DE', '90'],
    ['076', '40'],
    ['SG', '12'],
    ['Malta', '12'],
    ['Atlantis', '5'],
    [null, '1'],
  ],
  row_count: 9,
  truncated: false,
  ms: 3,
};

const PAGE = {
  version: '0.9.0',
  page: 'tools/studio.md',
  title: 'Studio',
  audience: ['user', 'dba'],
  ordered: true,
  markdown:
    '# Studio\n\nStudio is the web client.\n\n## The Docs panel\n\nSearch, then read a page here. See [dashboards](dashboards.md#maps) or [errors](../reference/errors.md), and [the panel](#the-docs-panel).\n\n| Key | Default |\n|---|---|\n| `studio.map_tile_url` | empty |\n\n```sql test\nSELECT 1;\n```\n',
  chunks: [],
};

const DASHBOARDS_PAGE = {
  ...PAGE,
  page: 'tools/dashboards.md',
  title: 'Dashboards',
  ordered: false,
  markdown: '# Dashboards\n\nTiles.\n\n## Maps\n\nPoint maps draw places.\n',
};

/** Answers every /v1 call; returns the URLs the page asked for. */
async function stub(page: Page, tiles = '', dash = DASH): Promise<{ urls: string[]; tileRequests: string[] }> {
  const urls: string[] = [];
  const tileRequests: string[] = [];
  page.on('request', (r) => urls.push(r.url()));
  await page.route('**/studio/version.json', (r) => r.fulfill({ json: { build_version: 'dev' } }));
  await page.route('**/studio/_/errors', (r) => r.fulfill({ status: 204 }));
  await page.route('https://tiles.example.test/**', (r) => {
    tileRequests.push(r.request().url());
    return r.fulfill({ status: 200, contentType: 'image/png', body: PNG });
  });
  await page.route('**/v1/**', async (route: Route) => {
    const path = new URL(route.request().url()).pathname;
    const ok = (json: unknown) => route.fulfill({ json });
    if (path === '/v1/auth/refresh') return ok({ access_token: 'tok', token_type: 'Bearer', expires_in: 3600, session_id: 's1' });
    if (path === '/v1/auth/me') return ok(ME(tiles));
    if (path === '/v1/catalog') return ok({ databases: ['shop'] });
    if (path === '/v1/dashboards') return ok({ mine: [{ ...dash, tiles: dash.tiles.length }], shared: [] });
    if (path === '/v1/dashboards/d1') return ok(dash);
    if (path === '/v1/dashboards/d1/tiles/t1:run') return ok(RUN);
    if (path === '/v1/dashboards/d1/tiles/t2:run') return ok(RUN2);
    if (path === '/v1/docs:search') {
      return ok({
        version: '0.9.0',
        mode: 'hybrid',
        hits: [
          {
            chunk_id: 'c1',
            page: 'tools/studio.md',
            title: 'Studio',
            heading_path: 'Studio > The Docs panel',
            audience: ['user'],
            text: 'Search, then read a page here.',
            score: 0.8,
          },
        ],
      });
    }
    if (path === '/v1/docs/pages/tools/studio.md') return ok(PAGE);
    if (path === '/v1/docs/pages/tools/dashboards.md') return ok(DASHBOARDS_PAGE);
    if (path.startsWith('/v1/docs/pages/')) return route.fulfill({ status: 400, json: { code: 39792, name: 'ER_SPRIG_ADMIN_REQUEST_INVALID', message: 'no such page' } });
    return ok({});
  });
  return { urls, tileRequests };
}

async function axe(page: Page, what: string, include: string): Promise<void> {
  const r = await new AxeBuilder({ page }).include(include).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  expect(r.violations.map((v) => `${what}: ${v.id} ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

const SHAPES = /shapes|countries-50m/;

test('point and filled maps draw without any outside request', async ({ page, baseURL }) => {
  const { urls } = await stub(page);
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.goto('/studio/t/acme/d/d1');
    const map = page.getByTestId('point-map');
    await expect(map).toBeVisible();
    await expect(map).toHaveAttribute('data-points', '3');
    await expect(map).toHaveAttribute('data-tiles', 'off');
    await expect(map.locator('canvas.maplibregl-canvas')).toBeVisible();
    await expect(page.getByRole('figure', { name: /point map of 3 places, sized by SUM\(revenue\), coloured by region/ })).toBeVisible();
    await expect(map.getByTestId('map-legend')).toContainText('West');
    await expect(map.getByTestId('map-legend')).toContainText('Size: SUM(revenue)');
    // The filled map: countries by name and code, aggregated, two rows unmatched.
    const filled = page.getByTestId('filled-map');
    await expect(filled).toBeVisible();
    await expect(filled).toHaveAttribute('data-shapes', 'on');
    await expect(filled).toHaveAttribute('data-regions', '6');
    await expect(filled.locator('canvas.maplibregl-canvas')).toBeVisible();
    await expect(page.getByRole('figure', { name: /Sales by country: map of 6 countries coloured by SUM\(revenue\), 2 rows not matched to a country/ })).toBeVisible();
    // Five distinct values (Singapore and Malta tie): a step each, with its value.
    const legend = filled.getByTestId('map-legend');
    await expect(legend).toContainText('SUM(revenue)');
    await expect(legend.getByRole('list', { name: 'Colour: SUM(revenue), in 5 steps' })).toBeVisible();
    await expect(legend.getByRole('listitem')).toHaveText(['12', '40', '90', '220', '450', 'No data']);
    await expect(legend.getByRole('listitem').first()).toHaveAttribute('title', '2 countries');
    await expect(filled.getByTestId('map-note')).toHaveText(/2 rows not matched to a country \(Atlantis, NULL\)/);
    await axe(page, `dashboard, ${scheme}`, 'main');
  }
  expect(urls.some((u) => SHAPES.test(u))).toBe(true);
  // Nothing but Studio's own origin (and blobs and data) was asked for.
  const origin = new URL(baseURL!).origin;
  const outside = urls.filter((u) => !u.startsWith(origin) && !u.startsWith('blob:') && !u.startsWith('data:'));
  expect(outside).toEqual([]);
});

test('the country shapes load only for a filled map', async ({ page }) => {
  const { urls } = await stub(page, '', { ...DASH, tiles: [DASH.tiles[0]!] });
  await page.goto('/studio/t/acme/d/d1');
  await expect(page.getByTestId('point-map')).toHaveAttribute('data-points', '3');
  await expect(page.getByTestId('point-map').locator('canvas.maplibregl-canvas')).toBeVisible();
  expect(urls.filter((u) => SHAPES.test(u))).toEqual([]);
});

test('with studio.map_tile_url the map draws that server’s tiles', async ({ page }) => {
  const { tileRequests } = await stub(page, TILES);
  await page.goto('/studio/t/acme/d/d1');
  const map = page.getByTestId('point-map');
  await expect(map).toHaveAttribute('data-tiles', 'on');
  await expect.poll(() => tileRequests.length).toBeGreaterThan(0);
  expect(tileRequests.every((u) => /^https:\/\/tiles\.example\.test\/\d+\/\d+\/\d+\.png$/.test(u))).toBe(true);
  await expect(map.getByText('Map tiles: tiles.example.test')).toBeAttached();
});

test('the Docs panel opens pages in Studio, follows their links, and keeps the website a click away', async ({ page }) => {
  await stub(page);
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.goto('/studio/t/acme/docs?q=docs%20panel');
    const hit = page.getByTestId('docs-hit');
    await expect(hit.getByRole('link', { name: 'Studio › The Docs panel', exact: true })).toHaveAttribute('href', '/studio/t/acme/docs/p/tools/studio.md#the-docs-panel');
    await expect(hit.getByRole('link', { name: /on the website/ })).toHaveAttribute('href', 'https://modlix-india.github.io/sprigdb/docs/tools/studio/#the-docs-panel');
    await hit.getByRole('link', { name: 'Studio › The Docs panel', exact: true }).click();
    const doc = page.getByTestId('docs-page');
    await expect(doc.getByRole('heading', { name: 'Studio', level: 2 })).toBeVisible();
    await expect(doc.getByRole('heading', { name: 'The Docs panel', level: 3 })).toHaveAttribute('id', 'the-docs-panel');
    await expect(doc.getByRole('cell', { name: 'studio.map_tile_url' })).toBeVisible();
    await expect(doc.getByTestId('docs-website')).toHaveAttribute('href', 'https://modlix-india.github.io/sprigdb/docs/tools/studio/');
    await expect(doc.getByTestId('docs-unordered')).toHaveCount(0);
    await expect(doc.getByRole('link', { name: 'errors' })).toHaveAttribute('href', '/studio/t/acme/docs/p/reference/errors.md');
    await expect(doc.getByRole('link', { name: 'the panel' })).toHaveAttribute('href', '/studio/t/acme/docs/p/tools/studio.md#the-docs-panel');
    await axe(page, `docs page, ${scheme}`, 'main');
    // A relative link opens the other page in Studio; its pack had no order.
    await doc.getByRole('link', { name: 'dashboards' }).click();
    await expect(page).toHaveURL(/\/studio\/t\/acme\/docs\/p\/tools\/dashboards\.md#maps$/);
    await expect(doc.getByRole('heading', { name: 'Dashboards', level: 2 })).toBeVisible();
    await expect(doc.getByTestId('docs-unordered')).toBeVisible();
    await expect(doc.getByRole('heading', { name: 'Maps', level: 3 })).toBeInViewport();
  }
});
