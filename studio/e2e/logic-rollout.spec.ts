// The Logic page's Rollout tab for a trigger, against a stubbed API: every
// /v1 call is answered here, so no gateway is needed. Run it against Vite's
// dev server:
//
//   cd studio
//   pnpm exec vite --host 127.0.0.1 --port 26473 --strictPort &
//   STUDIO_URL=http://127.0.0.1:26473 pnpm exec playwright test e2e/logic-rollout.spec.ts --project chromium
//
// A trigger's rollout shows its stage (picked by record key) and each
// version's runs; moving, promoting and aborting are ALTER TRIGGER
// statements, shown before they run and sent as SQL.
import { expect, test, type Page, type Route } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const ME = {
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
};

const MODULE = {
  module_id: 41,
  db_id: 3,
  name: 'audit_order_events',
  kind: 'trigger',
  runtime: 'javascript',
  security: 'definer',
  tier_pref: 'auto',
  active_version: 1,
  state: 'active',
  created_by: 1,
};

const version = (v: number) => ({ module_id: 41, version: v, signature: null, tier: 'engine', placement: null, state: v === 1 ? 'active' : 'candidate', compiler_version: '1' });

const STATS = [1, 2].map((v) => ({
  version: v,
  calls: v === 1 ? 120 : 30,
  errors: v === 1 ? 1 : 0,
  errorRate: v === 1 ? 1 / 120 : 0,
  meanMs: v === 1 ? 2.5 : 2.1,
  p95Ms: 5,
  histogram: [0, 3, 20, 5, 2, 0, 0, 0, 0, 0, 0, 0],
  since: '2026-11-17 08:00:00',
}));

/** Answers every /v1 call; the rollout's stage follows the statements sent. */
async function stub(page: Page): Promise<string[]> {
  const sent: string[] = [];
  let stage: { pct: number } | null = { pct: 20 };
  await page.route('**/studio/version.json', (r) => r.fulfill({ json: { build_version: 'dev' } }));
  await page.route('**/studio/_/errors', (r) => r.fulfill({ status: 204 }));
  await page.route('**/v1/**', async (route: Route) => {
    const path = decodeURIComponent(new URL(route.request().url()).pathname);
    const ok = (json: unknown) => route.fulfill({ json });
    if (path === '/v1/auth/refresh') return ok({ access_token: 'tok', token_type: 'Bearer', expires_in: 3600, session_id: 's1' });
    if (path === '/v1/auth/me') return ok(ME);
    if (path === '/v1/catalog') return ok({ databases: ['shop'] });
    if (path === '/v1/databases/shop/logic') return ok({ modules: [MODULE], routines: true });
    if (path === '/v1/databases/shop/logic/trigger/audit_order_events') {
      return ok({
        module: MODULE,
        versions: [version(1), version(2)],
        binding: { binding_id: 9, timing: 'on stream', events: 'shop.orders_events', state: 'active' },
        schedule: null,
        routines: true,
        source: 'export default function (r) { return r; }',
      });
    }
    if (path === '/v1/databases/shop/logic/trigger/audit_order_events/rollout') {
      return ok({
        module: 41,
        active_version: 1,
        rollout: stage
          ? { module_id: 41, active_version: 1, candidate_version: 2, pct: stage.pct, by: { key: null }, started_at: '2026-11-17 08:00:00.000000' }
          : null,
        stats: STATS,
        verdict: null,
        min_calls: 20,
        buckets_ms: [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000],
      });
    }
    if (path.endsWith('/logs')) return ok({ logs: [] });
    if (path === '/v1/databases/shop/sql:execute') {
      const sql = String((route.request().postDataJSON() as { sql: string }).sql);
      sent.push(sql);
      const to = /ROLLOUT TO (\d+)/.exec(sql);
      if (to) stage = { pct: Number(to[1]) };
      if (/ROLLOUT (PROMOTE|ABORT)/.test(sql)) stage = null;
      return ok({ results: [{ columns: [], rows: [], affected_rows: '0', warnings: 0 }] });
    }
    return ok({});
  });
  return sent;
}

async function axe(page: Page, what: string): Promise<void> {
  const r = await new AxeBuilder({ page }).include('main').withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  expect(r.violations.map((v) => `${what}: ${v.id} ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`)).toEqual([]);
}

test('a trigger’s rollout is shown, moved and promoted from the Rollout tab', async ({ page }) => {
  const sent = await stub(page);
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.goto('/studio/t/acme/logic/shop/trigger/audit_order_events');
    await page.getByTestId('logic-tab-rollout').click();
    await expect(page.getByTestId('logic-rollout-stage')).toContainText('v2');
    await expect(page.getByTestId('logic-rollout-pct')).toHaveText('20% of keys');
    await expect(page.getByTestId('logic-rollout-by')).toContainText('record key');
    await expect(page.getByRole('heading', { name: 'Runs per version' })).toBeVisible();
    await expect(page.getByTestId('logic-rollout-stats')).toContainText('v2 (candidate)');
    // A trigger picks by key alone: no "Pick by".
    await expect(page.getByRole('combobox', { name: 'Pick by' })).toHaveCount(0);
    await axe(page, `trigger rollout, ${scheme}`);
  }
  // Move: the statement is shown first, then sent.
  await page.getByTestId('logic-rollout-to').fill('60');
  await page.getByTestId('logic-rollout-move').click();
  await expect(page.getByTestId('logic-confirm-sql')).toHaveText('ALTER TRIGGER `shop`.`audit_order_events` ROLLOUT TO 60');
  await page.getByTestId('logic-confirm').click();
  await expect(page.getByTestId('logic-rollout-pct')).toHaveText('60% of keys');
  // Promote, with its note.
  await page.getByTestId('logic-rollout-promote').click();
  await expect(page.getByTestId('logic-confirm-note')).toContainText('every record and row');
  await expect(page.getByTestId('logic-confirm-sql')).toHaveText('ALTER TRIGGER `shop`.`audit_order_events` ROLLOUT PROMOTE');
  await page.getByTestId('logic-confirm').click();
  await expect(page.getByTestId('logic-rollout-none')).toContainText('every record and row runs v1');
  expect(sent).toEqual(['ALTER TRIGGER `shop`.`audit_order_events` ROLLOUT TO 60', 'ALTER TRIGGER `shop`.`audit_order_events` ROLLOUT PROMOTE']);
});
