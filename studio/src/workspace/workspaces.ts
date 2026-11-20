// Workspaces: Studio arranged for one kind of user (an analyst, a developer,
// a database or security administrator, a tenant administrator, an
// operator). Each is data: which capabilities open it, and which sections it
// puts in the activity bar or offers only in the command palette. A
// workspace changes what Studio shows, never what a user may do: the gateway
// decides every statement, whatever the workspace.

import type { Capabilities, WhoAmI } from '../api/types';
import type { MessageId } from '../i18n/en';
import { SECTIONS, hasPages, sectionOf, type Section, type SectionId } from './sections';

export type Capability = keyof Capabilities;

export type WorkspaceId = 'analyst' | 'developer' | 'db_admin' | 'security_admin' | 'tenant_admin' | 'operator';

/** `bar`: in the activity bar (and the palette); `palette`: only in the
 * command palette. A section a workspace doesn't list isn't offered. */
export type Placement = 'bar' | 'palette';

export interface WorkspaceDef {
  id: WorkspaceId;
  label: MessageId;
  /** One line on who it is for. */
  blurb: MessageId;
  /** Any one of these opens the workspace. */
  needs: Capability[];
  sections: Partial<Record<SectionId, Placement>>;
  /** Entered at this path instead of the tenant's home (operator mode): never
   * a default, never kept as the user's choice. */
  entry?: string;
  /** How Studio simplifies itself in this workspace (the analyst's). */
  simple?: Simple;
}

/** The analyst's simplifications. Each changes only what Studio shows or
 * asks first; the gateway still enforces the user's real privileges. */
export interface Simple {
  /** The tree and database pages list tables, views and collections only;
   * object pages show data, columns and relations, not indexes, triggers,
   * policies, shards or definitions. */
  tree: boolean;
  /** Query tabs start read-only: Studio runs reads and stops anything else
   * before sending it, until the tab's switch is turned off. */
  readOnlyTabs: boolean;
  /** Pages of sections the workspace doesn't offer (where the DDL forms
   * are) show a note instead, and settings links are left out. */
  hideDdl: boolean;
  /** No engine terms: no Explain (plans speak of shards and units), no
   * Shards tab, no gateway or data-centre names on the home page. */
  plainWords: boolean;
}

/** Widest first: the default for a user who hasn't chosen is the first one
 * they qualify for. */
export const WORKSPACES: readonly WorkspaceDef[] = [
  {
    id: 'tenant_admin',
    label: 'ws.tenant_admin',
    blurb: 'ws.tenant_admin.blurb',
    needs: ['administer_tenant'],
    sections: {
      explorer: 'bar',
      query: 'bar',
      saved: 'bar',
      history: 'bar',
      dashboards: 'bar',
      messaging: 'bar',
      logic: 'bar',
      performance: 'bar',
      inheritance: 'bar',
      access: 'bar',
      operations: 'bar',
      security: 'bar',
      docs: 'bar',
      settings: 'bar',
    },
  },
  {
    id: 'db_admin',
    label: 'ws.db_admin',
    blurb: 'ws.db_admin.blurb',
    needs: ['approve_changes'],
    sections: {
      explorer: 'bar',
      query: 'bar',
      saved: 'bar',
      history: 'bar',
      dashboards: 'palette',
      messaging: 'palette',
      logic: 'palette',
      performance: 'bar',
      inheritance: 'palette',
      access: 'palette',
      operations: 'bar',
      docs: 'bar',
      settings: 'bar',
    },
  },
  {
    id: 'security_admin',
    label: 'ws.security_admin',
    blurb: 'ws.security_admin.blurb',
    needs: ['manage_users'],
    sections: {
      explorer: 'palette',
      query: 'palette',
      dashboards: 'palette',
      inheritance: 'palette',
      access: 'bar',
      operations: 'palette',
      security: 'bar',
      docs: 'bar',
      settings: 'bar',
    },
  },
  {
    id: 'developer',
    label: 'ws.developer',
    blurb: 'ws.developer.blurb',
    needs: ['change_schemas', 'manage_logic'],
    sections: {
      explorer: 'bar',
      query: 'bar',
      saved: 'bar',
      history: 'bar',
      dashboards: 'bar',
      messaging: 'bar',
      logic: 'bar',
      performance: 'palette',
      inheritance: 'bar',
      access: 'palette',
      docs: 'bar',
      settings: 'palette',
    },
  },
  {
    id: 'analyst',
    label: 'ws.analyst',
    blurb: 'ws.analyst.blurb',
    needs: ['read_data'],
    simple: { tree: true, readOnlyTabs: true, hideDdl: true, plainWords: true },
    sections: {
      explorer: 'bar',
      query: 'bar',
      saved: 'bar',
      history: 'bar',
      dashboards: 'bar',
      docs: 'bar',
    },
  },
  {
    id: 'operator',
    label: 'ws.operator',
    blurb: 'ws.operator.blurb',
    needs: ['operator'],
    sections: {},
    entry: '/ops/clusters',
  },
];

/** The workspace a user who qualifies for none of the tenant workspaces gets. */
export const FALLBACK: WorkspaceId = 'analyst';

export function workspaceById(id: string | null | undefined): WorkspaceDef | undefined {
  return WORKSPACES.find((w) => w.id === id);
}

/** The capability summary from WhoAmI; for an older gateway without one, a
 * guess from the privileges it does return. */
export function capabilitiesOf(me: Pick<WhoAmI, 'capabilities' | 'privileges' | 'platform_roles'>): Capabilities {
  if (me.capabilities) return me.capabilities;
  const p = me.privileges;
  const schemas = p.tenant_admin || p.index_all || p.index_dbs.length > 0;
  return {
    read_data: true,
    change_schemas: schemas,
    manage_logic: schemas,
    manage_users: p.create_user,
    approve_changes: p.index_all,
    administer_tenant: p.tenant_admin,
    operator: me.platform_roles.includes('operator'),
  };
}

/** The workspaces a capability set opens, widest first (the operator's
 * last). */
export function openWorkspaces(caps: Capabilities): WorkspaceDef[] {
  const open = WORKSPACES.filter((w) => w.needs.some((n) => caps[n]));
  // Everyone signed in to a tenant gets at least one tenant workspace.
  if (!open.some((w) => !w.entry)) open.unshift(workspaceById(FALLBACK)!);
  return open;
}

/** The workspace Studio opens on: the user's own last choice; else the
 * default a tenant administrator set for the user or one of their roles
 * (`studio.default_workspace`); else the widest one the user qualifies for.
 * A choice or default the user doesn't qualify for (any more) is skipped. */
export function chooseWorkspace(
  caps: Capabilities,
  chosen: string | null | undefined,
  dflt: string | null | undefined,
): WorkspaceDef {
  const open = openWorkspaces(caps).filter((w) => !w.entry);
  return open.find((w) => w.id === chosen) ?? open.find((w) => w.id === dflt) ?? open[0]!;
}

function placed(ws: WorkspaceDef, s: Section, isOperator: boolean): Placement | undefined {
  // Operator mode stays at the bottom of every workspace's bar for operators.
  if (s.operatorOnly) return isOperator ? 'bar' : undefined;
  return ws.sections[s.id];
}

/** The activity bar of a workspace: its `bar` sections that have pages, in
 * the bar's order. */
export function barSections(ws: WorkspaceDef, isOperator: boolean): Section[] {
  return SECTIONS.filter((s) => placed(ws, s, isOperator) === 'bar' && hasPages(s));
}

/** The sections the command palette offers in a workspace: the bar's first,
 * then the palette-only ones. Panels (saved queries, history) have no page
 * to go to and aren't listed. */
export function paletteSections(ws: WorkspaceDef, isOperator: boolean): Section[] {
  const offered = SECTIONS.filter((s) => !s.panel && hasPages(s));
  return [
    ...offered.filter((s) => placed(ws, s, isOperator) === 'bar'),
    ...offered.filter((s) => placed(ws, s, isOperator) === 'palette'),
  ];
}

// ---- the analyst's simplifications ----

type ObjectKind = 'table' | 'view' | 'procedure' | 'function' | 'graph';

/** The objects a workspace's tree and database pages list. */
export function treeObjects<T extends { kind: ObjectKind }>(ws: WorkspaceDef, objects: readonly T[]): T[] {
  return ws.simple?.tree ? objects.filter((o) => o.kind === 'table' || o.kind === 'view') : [...objects];
}

/** The tabs of an object's page in a workspace, from the full list. */
export function objectTabs<T extends string>(ws: WorkspaceDef, kind: string, all: readonly T[]): T[] {
  if (!ws.simple?.tree) return [...all];
  const keep: string[] = kind === 'table' ? ['data', 'columns', 'relations'] : kind === 'view' ? ['data', 'columns'] : [];
  return all.filter((t) => keep.includes(t));
}

/** Whether a query tab starts read-only in a workspace. */
export function readOnlyByDefault(ws: WorkspaceDef): boolean {
  return !!ws.simple?.readOnlyTabs;
}

/** Whether a read-only tab stops a statement of class `cls` (lib/sql's
 * classify, or the MongoDB and Cypher equivalents) before sending it. */
export function stopsInReadOnly(cls: string): boolean {
  return cls !== 'read' && cls !== 'session';
}

/** Pages every workspace keeps whatever its sections: your own sign-in
 * security, and the home page. */
const ALWAYS: readonly string[] = ['', 'security'];

/** Whether a workspace shows the page at tenant-relative `rel`. Only a
 * workspace with `hideDdl` shows a note instead: on pages of sections it
 * doesn't offer at all. */
export function showsPage(ws: WorkspaceDef, rel: string): boolean {
  if (!ws.simple?.hideDdl || ALWAYS.includes(rel)) return true;
  const s = sectionOf(rel);
  return s.id in ws.sections;
}

/** The widest workspace in `open` that offers the section of `rel`, to
 * suggest when the current one doesn't. */
export function workspaceFor(open: readonly WorkspaceDef[], rel: string): WorkspaceDef | undefined {
  const s = sectionOf(rel);
  return open.find((w) => !w.entry && s.id in w.sections && showsPage(w, rel));
}
