// The workspace in use. The user's choice is kept in the tenant's catalog
// beside their saved queries (POST /v1/library/prefs; WhoAmI returns it), so
// it follows them between browsers. When the catalog can't keep it (an
// accounts-file login, an older gateway) it is kept in this browser.

import { useMemo } from 'react';
import { create } from 'zustand';
import { json } from '../api/client';
import { useSession } from '../auth/session';
import { getPref, setPref } from '../lib/prefs';
import { capabilitiesOf, chooseWorkspace, openWorkspaces, type WorkspaceDef, type WorkspaceId } from './workspaces';

interface ChoiceState {
  /** Picked in this tab since sign-in; wins over what WhoAmI said. */
  picked: WorkspaceId | null;
}

export const useWorkspaceChoice = create<ChoiceState>(() => ({ picked: null }));

const prefKey = (tenant: string | null, user: string) => `workspace.${tenant ?? '-'}/${user}`;

/** The workspaces open to the signed-in user, and the one in use. */
export function useWorkspaces(): { open: WorkspaceDef[]; current: WorkspaceDef } {
  const me = useSession((s) => s.me);
  const picked = useWorkspaceChoice((s) => s.picked);
  return useMemo(() => {
    if (!me) {
      const open = openWorkspaces({
        read_data: true,
        change_schemas: false,
        manage_logic: false,
        manage_users: false,
        approve_changes: false,
        administer_tenant: false,
        operator: false,
      });
      return { open, current: open[0]! };
    }
    const caps = capabilitiesOf(me);
    const local = getPref<string | null>(prefKey(me.principal.tenant, me.principal.user), null);
    const current = chooseWorkspace(caps, picked ?? me.workspace?.chosen ?? local, me.workspace?.default);
    return { open: openWorkspaces(caps), current };
  }, [me, picked]);
}

/** Switches workspace and keeps the choice. Open tabs stay open. */
export async function pickWorkspace(id: WorkspaceId): Promise<void> {
  useWorkspaceChoice.setState({ picked: id });
  const me = useSession.getState().me;
  if (!me) return;
  const key = prefKey(me.principal.tenant, me.principal.user);
  try {
    await json('/v1/library/prefs', { body: { workspace: id } });
    // The catalog keeps it now; a browser copy would only go stale.
    if (getPref<string | null>(key, null) !== null) setPref(key, null);
  } catch {
    setPref(key, id);
  }
}

if (typeof window !== 'undefined') {
  const reset = () => useWorkspaceChoice.setState({ picked: null });
  window.addEventListener('sprig-signed-out', reset);
  window.addEventListener('sprig-identity-changed', reset);
}
