// "Add to dashboard" on a query result: the statement becomes a tile on a
// dashboard the user picks (or a new one), drawn as Show Me suggests for
// its columns; the user goes on to the chart editor to change it.

import { useEffect, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import { errorText } from '../api/errors';
import { addTile, createDashboard, useDashboards, useStoreDashboard } from './api';
import { suggest, toStored, type ColumnInfo } from './spec';

export function AddToDashboard({ db, sql, columns }: { db: string | null; sql: string; columns: ColumnInfo[] }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)} data-testid="add-to-dashboard">
        ▦ Add to dashboard
      </button>
      {open && <AddDialog db={db} sql={sql} columns={columns} onClose={() => setOpen(false)} />}
    </>
  );
}

function AddDialog({ db, sql, columns, onClose }: { db: string | null; sql: string; columns: ColumnInfo[]; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  const { tenant = '' } = useParams();
  const navigate = useNavigate();
  const list = useDashboards();
  const store = useStoreDashboard();
  const editable = [...(list.data?.mine ?? []), ...(list.data?.shared ?? []).filter((d) => d.can_edit)];
  const [picked, setTarget] = useState<string>('');
  // The first dashboard the caller may edit, until they pick.
  const target = picked || editable[0]?.id || 'new';
  const [newName, setNewName] = useState('My dashboard');
  const [title, setTitle] = useState(() => sql.replace(/\s+/g, ' ').trim().slice(0, 60));
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    ref.current?.showModal?.();
  }, []);
  const add = async (edit: boolean) => {
    setBusy(true);
    setError(null);
    try {
      let id = target;
      if (target === 'new' || !target) {
        const d = await createDashboard(newName.trim() || 'My dashboard');
        store(d);
        id = d.id;
      }
      const chart = toStored(suggest(columns));
      const r = await addTile(id, { title: title.trim() || 'Tile', database: db, sql: sql.trim().replace(/;\s*$/, ''), chart });
      store(r.dashboard);
      onClose();
      const base = `/t/${encodeURIComponent(tenant)}`;
      void navigate(edit ? `${base}/d/${id}/tile/${r.tile}` : `${base}/d/${id}`);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <dialog ref={ref} onClose={onClose} aria-labelledby="add-dash-title" data-testid="add-to-dashboard-dialog">
      <form
        method="dialog"
        className="stack"
        onSubmit={(e) => {
          e.preventDefault();
          void add(false);
        }}
      >
        <h2 id="add-dash-title" style={{ margin: 0, fontSize: 16 }}>
          Add to dashboard
        </h2>
        <label className="stack">
          <span>Dashboard</span>
          <select value={target} onChange={(e) => setTarget(e.target.value)} aria-label="Dashboard">
            {editable.map((d) => (
              <option key={d.id} value={d.id}>
                {d.name}
                {d.mine ? '' : ` (shared by ${d.owner})`}
              </option>
            ))}
            <option value="new">New dashboard…</option>
          </select>
        </label>
        {target === 'new' && (
          <label className="stack">
            <span>New dashboard’s name</span>
            <input value={newName} onChange={(e) => setNewName(e.target.value)} aria-label="New dashboard’s name" />
          </label>
        )}
        <label className="stack">
          <span>Tile title</span>
          <input value={title} onChange={(e) => setTitle(e.target.value)} aria-label="Tile title" />
        </label>
        <p className="muted small" style={{ margin: 0 }}>
          The tile runs this statement as whoever views the dashboard, with their own rights. Its chart starts as Show Me suggests for the columns.
        </p>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        <div className="row">
          <button type="submit" className="primary" disabled={busy}>
            Add
          </button>
          <button type="button" onClick={() => void add(true)} disabled={busy}>
            Add and edit chart
          </button>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
        </div>
      </form>
    </dialog>
  );
}
