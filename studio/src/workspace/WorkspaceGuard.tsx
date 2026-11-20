import type { ReactNode } from 'react';
import { useIntl } from 'react-intl';
import { useRel } from './WorkspaceTabs';
import { pickWorkspace, useWorkspaces } from './current';
import { showsPage, workspaceFor } from './workspaces';

/** A page outside the workspace: a workspace that hides DDL forms (the
 * analyst's) shows this note instead of the pages of sections it doesn't
 * offer, with a way to switch when the user has a workspace that does. It
 * changes only what Studio shows; the gateway decides what the user may do. */
export function WorkspaceGuard({ children }: { children: ReactNode }) {
  const intl = useIntl();
  const { rel } = useRel();
  const { open, current } = useWorkspaces();
  if (showsPage(current, rel)) return <>{children}</>;
  const other = workspaceFor(open, rel);
  return (
    <section className="card stack" data-testid="workspace-guard" style={{ maxInlineSize: 560 }}>
      <h1 style={{ margin: 0, fontSize: 18 }}>
        {intl.formatMessage({ id: 'ws.guard.title' }, { name: intl.formatMessage({ id: current.label }) })}
      </h1>
      <p className="muted">{intl.formatMessage({ id: 'ws.guard.body' })}</p>
      {other && (
        <div className="row">
          <button
            className="primary"
            // The page shows as soon as the workspace changes.
            onClick={() => void pickWorkspace(other.id)}
            data-testid="workspace-guard-switch"
          >
            {intl.formatMessage({ id: 'ws.guard.switch' }, { name: intl.formatMessage({ id: other.label }) })}
          </button>
        </div>
      )}
    </section>
  );
}
