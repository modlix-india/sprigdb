// The Dashboards section's side panel: the caller's own dashboards, the
// tenant's shared ones, and the digest. Kept apart from the pages so the
// charting library loads only when a dashboard opens.

import { useState } from 'react';
import { NavLink, useNavigate } from 'react-router';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import { useRel } from '../workspace/WorkspaceTabs';
import { createDashboard, useDashboards, useStoreDashboard } from './api';

/** The Dashboards section's side panel: the caller's own, the tenant's
 * shared ones, and the digest (tenant administrators). */
export function DashboardsPanel() {
  const { base } = useRel();
  const navigate = useNavigate();
  const me = useSession((s) => s.me);
  const list = useDashboards();
  const store = useStoreDashboard();
  const [error, setError] = useState<string | null>(null);
  const make = async () => {
    const name = window.prompt('Name of the new dashboard', 'New dashboard');
    if (!name?.trim()) return;
    try {
      const d = await createDashboard(name.trim());
      store(d);
      void navigate(`${base}/d/${d.id}`);
    } catch (e) {
      setError(errorText(e));
    }
  };
  const item = (d: { id: string; name: string; owner: string; mine: boolean }) => (
    <li key={d.id}>
      <NavLink to={`${base}/d/${d.id}`} data-testid={`dash-link-${d.name}`}>
        <span aria-hidden="true">▦</span>
        {d.name}
        {!d.mine && <span className="owner">{d.owner}</span>}
      </NavLink>
    </li>
  );
  return (
    <nav aria-label="Dashboards">
      <ul className="side-dash">
        <li className="group">Mine</li>
        {list.data?.mine.map(item)}
        {list.data && !list.data.mine.length && <li className="muted small" style={{ padding: '2px 8px' }}>None yet</li>}
        <li className="group">Shared with the tenant</li>
        {list.data?.shared.map(item)}
        {list.data && !list.data.shared.length && <li className="muted small" style={{ padding: '2px 8px' }}>None shared</li>}
        {me?.privileges.tenant_admin && (
          <>
            <li className="group">Digest</li>
            <li data-slot="digest">
              <NavLink to={`${base}/digests`}>
                <span aria-hidden="true">✦</span>Tenant digests
              </NavLink>
            </li>
          </>
        )}
      </ul>
      <div style={{ padding: '8px 12px' }}>
        <button onClick={() => void make()} data-testid="new-dashboard">
          ＋ New dashboard
        </button>
      </div>
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {error && <p className="error">{error}</p>}
    </nav>
  );
}

