import type { ComponentType } from 'react';
import { useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { Mascot } from './Mascot';
import { useSession } from '../auth/session';
import { useWorkspaces } from '../workspace/current';
import type { WorkspaceId } from '../workspace/workspaces';
import { Checklist } from '../home/Checklist';
import { AnalystHome, DbAdminHome, DeveloperHome, SecurityAdminHome, TenantAdminHome } from '../home/Homes';

const HOMES: Record<Exclude<WorkspaceId, 'operator'>, ComponentType> = {
  analyst: AnalystHome,
  developer: DeveloperHome,
  db_admin: DbAdminHome,
  security_admin: SecurityAdminHome,
  tenant_admin: TenantAdminHome,
};

/** The tenant's home: the home page of the workspace in use. */
export function TenantHome() {
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const { current } = useWorkspaces();
  const intl = useIntl();
  if (!me) return null;
  const home = me.principal.tenant === tenant;
  const Home = HOMES[current.id as keyof typeof HOMES] ?? AnalystHome;
  return (
    <div className="stack" data-testid={`home-${current.id}`}>
      <div className="welcome-hero">
        <Mascot pose="waving" size={96} />
        <div>
          <h1>{intl.formatMessage({ id: 'welcome.title' }, { user: me.principal.login })}</h1>
          <p className="muted" style={{ margin: 0 }}>
            {intl.formatMessage({ id: current.label })}: {intl.formatMessage({ id: current.blurb })}
          </p>
          <p className="muted small" style={{ margin: '4px 0 0' }}>
            {tenant}
            {me.gateway && !current.simple?.plainWords ? ` · ${me.gateway.id} in ${me.gateway.dc}` : ''}
          </p>
        </div>
      </div>
      {!home && (
        <p className="banner">
          You administer this tenant from {me.principal.tenant}. Its settings and accounts are open to you; data views use your home
          tenant.
        </p>
      )}
      <Checklist ws={current} />
      <Home />
    </div>
  );
}
