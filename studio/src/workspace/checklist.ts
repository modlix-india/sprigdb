// The first-run checklist of each workspace: a few first steps, shown on
// the home page until they're done or hidden. Steps Studio can see done
// (a query run or saved, a table or database opened) tick themselves; the
// others are ticked by hand. Ticks and hiding are kept in this browser.

import { getPref, setPref } from '../lib/prefs';
import type { WorkspaceId } from './workspaces';

/** What Studio can see done. */
export type Signal = 'ranQuery' | 'savedQuery' | 'openedTable' | 'openedDatabase';

export interface CheckItem {
  id: string;
  label: string;
  /** Where the step is done: a tenant-relative path, or `new-query`. */
  to: string;
  signal?: Signal;
}

export const CHECKLISTS: Record<Exclude<WorkspaceId, 'operator'>, readonly CheckItem[]> = {
  analyst: [
    { id: 'find-table', label: 'Find a table and open it', to: '', signal: 'openedTable' },
    { id: 'run-query', label: 'Run a query', to: 'new-query', signal: 'ranQuery' },
    { id: 'save-query', label: 'Save a query with Ctrl+S', to: 'new-query', signal: 'savedQuery' },
    { id: 'dashboard', label: 'Put a result on a dashboard', to: 'd' },
  ],
  developer: [
    { id: 'open-db', label: 'Open a database', to: '', signal: 'openedDatabase' },
    { id: 'run-query', label: 'Run a query', to: 'new-query', signal: 'ranQuery' },
    { id: 'logic', label: 'Write a function or a trigger', to: 'logic' },
    { id: 'connect', label: 'Connect your application', to: 'docs?q=connect' },
  ],
  db_admin: [
    { id: 'approvals', label: 'Review what waits for approval', to: 'inbox' },
    { id: 'slow', label: 'Look at the slowest statements', to: 'indexing?view=fingerprints' },
    { id: 'policy', label: 'Set how changes are approved', to: 'inbox/policy' },
    { id: 'settings', label: "Check the tenant's settings", to: 'settings' },
  ],
  security_admin: [
    { id: 'users', label: 'Review the users and their last sign-in', to: 'access/users' },
    { id: 'factors', label: 'Turn on two-factor sign-in for yourself', to: 'security' },
    { id: 'roles', label: 'Review roles and row-level policies', to: 'policies' },
    { id: 'keys', label: 'Check the API keys', to: 'access/keys' },
  ],
  tenant_admin: [
    { id: 'users', label: 'Add the people who use this tenant', to: 'access/users' },
    { id: 'workspaces', label: "Set each team's default workspace (studio.default_workspace)", to: 'settings' },
    { id: 'policy', label: 'Decide how changes are approved', to: 'inbox/policy' },
    { id: 'docs', label: 'Read how SprigDB fits together', to: 'docs' },
  ],
};

export type Progress = Record<Signal, boolean>;

export interface Kept {
  ticked: string[];
  hidden: boolean;
}

export interface Step extends CheckItem {
  done: boolean;
}

/** Each step with whether it's done: by its signal or ticked by hand. */
export function steps(items: readonly CheckItem[], progress: Progress, ticked: readonly string[]): Step[] {
  return items.map((it) => ({ ...it, done: (it.signal ? progress[it.signal] : false) || ticked.includes(it.id) }));
}

/** Whether the checklist still shows: not hidden, and a step left. */
export function shows(all: readonly Step[], kept: Kept): boolean {
  return !kept.hidden && all.some((s) => !s.done);
}

const key = (tenant: string, user: string, ws: string) => `checklist.${tenant}/${user}.${ws}`;

export function loadKept(tenant: string, user: string, ws: string): Kept {
  const k = getPref<Partial<Kept> | null>(key(tenant, user, ws), null);
  return { ticked: Array.isArray(k?.ticked) ? k.ticked : [], hidden: k?.hidden === true };
}

export function saveKept(tenant: string, user: string, ws: string, kept: Kept): void {
  setPref(key(tenant, user, ws), kept);
}
