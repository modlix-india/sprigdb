// Workspaces: a user who qualifies for each single workspace lands in it,
// with its activity bar and home page; the analyst's tabs start read-only
// and the server still decides. The users are created for the run, named
// ws_test_*, and dropped at the end (also when a test fails).
import { randomUUID } from 'node:crypto';
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import { apiToken, sql, signIn, typeSql } from './fixtures';

const DB = process.env.STUDIO_WS_DB ?? 'shop';
const PASSWORD = `Ws-${randomUUID()}-9a`;

/** Each test user: their grants, and what Studio should show them. */
const USERS: Record<string, { grants: string[]; workspace: string; options: string[]; bar: string[] }> = {
  ws_test_analyst: {
    grants: [`GRANT SELECT ON \`${DB}\`.* TO ws_test_analyst`],
    workspace: 'Analyst',
    options: ['Analyst'],
    bar: ['Explorer', 'Query', 'Saved queries', 'History', 'Dashboards', 'Docs'],
  },
  ws_test_dev: {
    grants: [`GRANT ALTER, CREATE ROUTINE ON \`${DB}\`.* TO ws_test_dev`],
    workspace: 'Developer',
    options: ['Developer'],
    bar: ['Explorer', 'Query', 'Saved queries', 'History', 'Dashboards', 'Messaging', 'Logic', 'Inheritance', 'Docs'],
  },
  ws_test_dba: {
    grants: ['GRANT INDEX ON *.* TO ws_test_dba'],
    workspace: 'Database administrator',
    options: ['Database administrator'],
    bar: ['Explorer', 'Query', 'Saved queries', 'History', 'Performance', 'Docs', 'Settings'],
  },
  ws_test_sec: {
    grants: ['GRANT CREATE USER ON *.* TO ws_test_sec'],
    workspace: 'Security administrator',
    options: ['Security administrator'],
    bar: ['Access control', 'Docs', 'Settings'],
  },
  ws_test_none: {
    grants: [],
    workspace: 'Analyst',
    options: ['Analyst'],
    bar: ['Explorer', 'Query', 'Saved queries', 'History', 'Dashboards', 'Docs'],
  },
};

async function asAdmin(request: APIRequestContext, statements: string[], tolerate = false): Promise<void> {
  const token = await apiToken(request);
  for (const s of statements) {
    try {
      await sql(request, token, '-', s);
    } catch (e) {
      if (!tolerate) throw e;
    }
  }
}

async function dropAll(request: APIRequestContext): Promise<void> {
  await asAdmin(
    request,
    Object.keys(USERS).map((u) => `DROP USER IF EXISTS ${u}`),
    true,
  );
}

test.beforeAll(async ({ request }) => {
  await dropAll(request);
  for (const [u, spec] of Object.entries(USERS)) {
    await asAdmin(request, [`CREATE USER ${u} IDENTIFIED BY '${PASSWORD}'`, ...spec.grants]);
  }
});

test.afterAll(async ({ request }) => {
  await dropAll(request);
  const token = await apiToken(request);
  const r = await request.get('/v1/users', { headers: { authorization: `Bearer ${token}` } });
  const users = ((await r.json()) as { users: { user: string }[] }).users.map((x) => x.user);
  expect(users.filter((u) => u.startsWith('ws_test_'))).toEqual([]);
});

async function activityBar(page: Page): Promise<string[]> {
  return page
    .getByRole('navigation', { name: 'Sections' })
    .locator('a, button')
    .evaluateAll((els) => els.map((e) => e.getAttribute('aria-label') ?? ''));
}

async function workspaceOptions(page: Page): Promise<string[]> {
  const control = page.getByTestId('workspace');
  const tag = await control.evaluate((e) => e.tagName);
  if (tag === 'SELECT') return control.locator('option').allTextContents();
  return [((await control.textContent()) ?? '').trim()];
}

for (const [user, spec] of Object.entries(USERS)) {
  test(`${user} lands in the ${spec.workspace} workspace`, async ({ page }) => {
    await signIn(page, `acme.${user}`, PASSWORD);
    await expect(page.getByTestId('whoami')).toHaveText(`acme.${user}`);
    expect(await workspaceOptions(page)).toEqual(spec.options);
    expect(await activityBar(page)).toEqual(spec.bar);
    await expect(page.getByTestId(`home-${idOf(spec.workspace)}`)).toBeVisible();
  });
}

function idOf(label: string): string {
  return { Analyst: 'analyst', Developer: 'developer', 'Database administrator': 'db_admin', 'Security administrator': 'security_admin' }[label] ?? label;
}

test("the analyst's tab starts read-only, and the server still decides", async ({ page }) => {
  await signIn(page, 'acme.ws_test_analyst', PASSWORD);
  await page.getByRole('navigation', { name: 'Sections' }).getByRole('link', { name: 'Query', exact: true }).click();
  await expect(page.getByTestId('read-only')).toBeChecked();
  await expect(page.getByRole('button', { name: 'Explain' })).toHaveCount(0);
  await page.getByTestId('db-select').selectOption(DB);
  await typeSql(page, 'CREATE TABLE ws_test_t (a INT);');
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(page.getByTestId('query-error')).toContainText("didn't send");
  // Off: the statement goes to the gateway, which refuses it (no privilege).
  await page.getByTestId('read-only').uncheck();
  await page.getByRole('button', { name: 'Run', exact: true }).click();
  // studio.confirm_writes asks first for DDL, as in every workspace.
  await page.getByRole('button', { name: 'Run it' }).click();
  await expect(page.getByTestId('query-error')).toContainText(/denied/i);
  // A section the analyst's workspace doesn't offer: a note, not its forms.
  await page.goto('/studio/t/acme/logic');
  await expect(page.getByTestId('workspace-guard')).toBeVisible();
  await expect(page.getByTestId('workspace-guard-switch')).toHaveCount(0);
});

test('a tenant administrator gets every workspace, widest first', async ({ page }) => {
  await signIn(page);
  const options = await workspaceOptions(page);
  expect(options.slice(0, 5)).toEqual(['Tenant administrator', 'Database administrator', 'Security administrator', 'Developer', 'Analyst']);
});
