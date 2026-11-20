import { describe, expect, it } from 'vitest';
import type { Capabilities } from '../api/types';
import { SECTIONS, hasPages } from './sections';
import {
  WORKSPACES,
  barSections,
  capabilitiesOf,
  chooseWorkspace,
  openWorkspaces,
  paletteSections,
  workspaceById,
  type Capability,
  type WorkspaceId,
} from './workspaces';

const NONE: Capabilities = {
  read_data: false,
  change_schemas: false,
  manage_logic: false,
  manage_users: false,
  approve_changes: false,
  administer_tenant: false,
  operator: false,
};
const caps = (...on: Capability[]): Capabilities => ({ ...NONE, ...Object.fromEntries(on.map((c) => [c, true])) });
const ids = (ws: { id: string }[]) => ws.map((w) => w.id);
const ws = (id: WorkspaceId) => workspaceById(id)!;

describe('which workspaces a capability set opens', () => {
  it('opens each workspace by its own capability, widest first', () => {
    expect(ids(openWorkspaces(caps('read_data')))).toEqual(['analyst']);
    expect(ids(openWorkspaces(caps('read_data', 'change_schemas')))).toEqual(['developer', 'analyst']);
    expect(ids(openWorkspaces(caps('manage_logic')))).toEqual(['developer']);
    expect(ids(openWorkspaces(caps('approve_changes')))).toEqual(['db_admin']);
    expect(ids(openWorkspaces(caps('manage_users')))).toEqual(['security_admin']);
    expect(ids(openWorkspaces(caps('administer_tenant')))).toEqual(['tenant_admin']);
  });

  it('gives a solo developer on a small tenant all five', () => {
    const all = caps('read_data', 'change_schemas', 'manage_logic', 'manage_users', 'approve_changes', 'administer_tenant');
    expect(ids(openWorkspaces(all))).toEqual(['tenant_admin', 'db_admin', 'security_admin', 'developer', 'analyst']);
  });

  it("adds the operator's workspace last, and a tenant workspace to everyone", () => {
    expect(ids(openWorkspaces(caps('read_data', 'operator')))).toEqual(['analyst', 'operator']);
    // Neither read access nor anything else: still a tenant workspace to land in.
    expect(ids(openWorkspaces(NONE))).toEqual(['analyst']);
    expect(ids(openWorkspaces(caps('operator')))).toEqual(['analyst', 'operator']);
  });

  it('guesses capabilities from the privileges of an older gateway', () => {
    const me = {
      privileges: { tenant_admin: false, settings_admin: false, create_user: true, grant_option: false, index_all: false, index_dbs: ['shop'] },
      platform_roles: ['operator'],
    };
    expect(capabilitiesOf(me)).toEqual({
      read_data: true,
      change_schemas: true,
      manage_logic: true,
      manage_users: true,
      approve_changes: false,
      administer_tenant: false,
      operator: true,
    });
    const given = caps('manage_users');
    expect(capabilitiesOf({ ...me, capabilities: given })).toBe(given);
  });
});

describe('the default workspace', () => {
  const dev = caps('read_data', 'change_schemas', 'approve_changes');

  it("takes the user's own last choice first", () => {
    expect(chooseWorkspace(dev, 'analyst', 'developer').id).toBe('analyst');
  });

  it("then the tenant administrator's default for the user or a role", () => {
    expect(chooseWorkspace(dev, null, 'developer').id).toBe('developer');
    // A choice the user no longer qualifies for is skipped.
    expect(chooseWorkspace(dev, 'tenant_admin', 'developer').id).toBe('developer');
  });

  it('then the widest workspace the user qualifies for', () => {
    expect(chooseWorkspace(dev, null, null).id).toBe('db_admin');
    expect(chooseWorkspace(dev, null, 'security_admin').id).toBe('db_admin');
    expect(chooseWorkspace(caps('read_data'), undefined, undefined).id).toBe('analyst');
    expect(chooseWorkspace(NONE, null, null).id).toBe('analyst');
  });

  it("never opens on the operator's workspace", () => {
    expect(chooseWorkspace(caps('read_data', 'operator'), 'operator', 'operator').id).toBe('analyst');
  });
});

describe('the activity bar per workspace', () => {
  const bar = (id: WorkspaceId, operator = false) => ids(barSections(ws(id), operator));

  it('shows only the sections each workspace uses', () => {
    expect(bar('analyst')).toEqual(['explorer', 'query', 'saved', 'history', 'dashboards', 'docs']);
    expect(bar('developer')).toEqual(['explorer', 'query', 'saved', 'history', 'dashboards', 'messaging', 'logic', 'inheritance', 'docs']);
    expect(bar('db_admin')).toEqual(['explorer', 'query', 'saved', 'history', 'performance', 'docs', 'settings']);
    expect(bar('security_admin')).toEqual(['access', 'docs', 'settings']);
    expect(bar('tenant_admin')).toEqual([
      'explorer',
      'query',
      'saved',
      'history',
      'dashboards',
      'messaging',
      'logic',
      'performance',
      'inheritance',
      'access',
      'docs',
      'settings',
    ]);
  });

  it('keeps operator mode at the bottom for operators in every workspace', () => {
    for (const w of WORKSPACES.filter((x) => !x.entry)) {
      expect(bar(w.id, true)).toContain('operator');
      expect(bar(w.id, false)).not.toContain('operator');
    }
  });

  it('hides Operations and Security until they have a page', () => {
    const ops = SECTIONS.find((s) => s.id === 'operations')!;
    const sec = SECTIONS.find((s) => s.id === 'security')!;
    expect(hasPages(ops)).toBe(false);
    expect(hasPages(sec)).toBe(false);
    expect(ws('tenant_admin').sections.operations).toBe('bar');
    expect(ws('security_admin').sections.security).toBe('bar');
    // Once a page is added, the section shows where the workspace puts it.
    const withPage = { ...ops, links: [{ label: 'nav.operations' as const, to: 'backups' }] };
    expect(hasPages(withPage)).toBe(true);
  });

  it('offers the workspace sections first in the palette, then the palette-only ones, never the others', () => {
    const pal = ids(paletteSections(ws('analyst'), false));
    expect(pal).toEqual(['explorer', 'query', 'dashboards', 'docs']);
    const dev = ids(paletteSections(ws('developer'), false));
    expect(dev.slice(0, 7)).toEqual(['explorer', 'query', 'dashboards', 'messaging', 'logic', 'inheritance', 'docs']);
    expect(dev.slice(7)).toEqual(['performance', 'access', 'settings']);
    const sec = ids(paletteSections(ws('security_admin'), false));
    expect(sec).toEqual(['access', 'docs', 'settings', 'explorer', 'query', 'dashboards', 'inheritance']);
    expect(sec).not.toContain('logic');
  });

  it('places only sections that exist', () => {
    const known = new Set(SECTIONS.map((s) => s.id));
    for (const w of WORKSPACES) {
      for (const id of Object.keys(w.sections)) expect(known.has(id as never)).toBe(true);
    }
  });
});
