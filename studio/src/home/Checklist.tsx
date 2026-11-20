import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { useSession } from '../auth/session';
import { addTab } from '../editor/tabs';
import { useLibrary } from '../workspace/library';
import { useWorkspace } from '../workspace/store';
import { CHECKLISTS, loadKept, saveKept, shows, steps, type Kept } from '../workspace/checklist';
import type { WorkspaceDef } from '../workspace/workspaces';

/** The workspace's first-run checklist, on its home page. */
export function Checklist({ ws }: { ws: WorkspaceDef }) {
  const intl = useIntl();
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const history = useLibrary((s) => s.history.length);
  const saved = useLibrary((s) => s.saved.length);
  const docs = useWorkspace((s) => s.docs);
  const user = me?.principal.user ?? '';
  const [kept, setKept] = useState<Kept>(() => loadKept(tenant, user, ws.id));
  const [keptFor, setKeptFor] = useState(ws.id);
  if (keptFor !== ws.id) {
    setKeptFor(ws.id);
    setKept(loadKept(tenant, user, ws.id));
  }
  if (ws.id === 'operator') return null;
  const all = steps(
    CHECKLISTS[ws.id],
    {
      ranQuery: history > 0,
      savedQuery: saved > 0,
      openedTable: docs.some((d) => d.kind === 'table' || d.kind === 'view'),
      openedDatabase: docs.some((d) => d.kind === 'database'),
    },
    kept.ticked,
  );
  if (!shows(all, kept)) return null;
  const keep = (k: Kept) => {
    setKept(k);
    saveKept(tenant, user, ws.id, k);
  };
  const base = `/t/${encodeURIComponent(tenant)}`;
  return (
    <section className="card stack" aria-labelledby="checklist-h" data-testid="checklist">
      <div className="row">
        <h2 id="checklist-h" style={{ flex: 1, margin: 0, fontSize: 15 }}>
          {intl.formatMessage({ id: 'checklist.title' })}
        </h2>
        <button className="link small" onClick={() => keep({ ...kept, hidden: true })} data-testid="checklist-hide">
          {intl.formatMessage({ id: 'checklist.dismiss' })}
        </button>
      </div>
      <ul className="checklist">
        {all.map((s) => (
          <li key={s.id}>
            <input
              type="checkbox"
              checked={s.done}
              disabled={!!s.signal && s.done && !kept.ticked.includes(s.id)}
              aria-label={s.label}
              onChange={(e) =>
                keep({
                  ...kept,
                  ticked: e.target.checked ? [...kept.ticked, s.id] : kept.ticked.filter((x) => x !== s.id),
                })
              }
            />
            {s.to === 'new-query' ? (
              <button
                className="link"
                onClick={() => {
                  const t = addTab();
                  void navigate(`${base}/q/${t.id}`);
                }}
              >
                {s.label}
              </button>
            ) : (
              <Link to={s.to ? `${base}/${s.to}` : base}>{s.label}</Link>
            )}
            {s.done && <span className="muted small"> · {intl.formatMessage({ id: 'checklist.done' })}</span>}
          </li>
        ))}
      </ul>
    </section>
  );
}
