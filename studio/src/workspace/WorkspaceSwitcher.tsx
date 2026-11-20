import { useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { pickWorkspace, useWorkspaces } from './current';
import { workspaceById } from './workspaces';

/** The workspace control in the title bar, beside the tenant switcher: the
 * workspaces the user's capabilities open. Switching opens the workspace's
 * home and keeps every tab open; the operator's workspace is operator mode. */
export function WorkspaceSwitcher() {
  const intl = useIntl();
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  const { open, current } = useWorkspaces();
  const label = intl.formatMessage({ id: 'ws.label' });
  if (open.length <= 1) {
    return (
      <span className="badge" title={intl.formatMessage({ id: current.blurb })} data-testid="workspace">
        {intl.formatMessage({ id: current.label })}
      </span>
    );
  }
  return (
    <label className="row">
      <span className="visually-hidden">{label}</span>
      <select
        aria-label={label}
        value={current.id}
        data-testid="workspace"
        title={intl.formatMessage({ id: current.blurb })}
        onChange={(e) => {
          const ws = workspaceById(e.target.value);
          if (!ws) return;
          if (ws.entry) {
            void navigate(ws.entry);
            return;
          }
          void pickWorkspace(ws.id);
          void navigate(`/t/${encodeURIComponent(tenant)}`);
        }}
      >
        {open.map((w) => (
          <option key={w.id} value={w.id}>
            {intl.formatMessage({ id: w.label })}
          </option>
        ))}
      </select>
    </label>
  );
}
