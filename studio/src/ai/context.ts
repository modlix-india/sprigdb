// The chips the dock offers from where the user is: the database, the
// table they look at, the query tab and its statement. The user removes
// any; an entry point adds its own (an error, a statement, a recommendation).

import { useTabs, type Tab } from '../editor/tabs';
import { useRel } from '../workspace/WorkspaceTabs';
import { useAssistant, type Chip } from './store';

/** The chips a page offers, before the user removed any. */
export function chipsFor(rel: string, tabs: Tab[]): Chip[] {
  const parts = rel.split('/').map(decodeURIComponent);
  const out: Chip[] = [];
  if (parts[0] === 'db' && parts[1]) {
    out.push({ id: `db:${parts[1]}`, kind: 'database', label: parts[1], ref: parts[1] });
    if (parts[2] === 'obj' && parts[4] && (parts[3] === 'table' || parts[3] === 'view')) {
      const ref = `${parts[1]}.${parts[4]}`;
      out.push({ id: `table:${ref}`, kind: 'table', label: parts[4], ref });
    }
  } else if (parts[0] === 'q' && parts[1]) {
    const t = tabs.find((x) => x.id === parts[1]);
    if (t) {
      if (t.db) out.push({ id: `db:${t.db}`, kind: 'database', label: t.db, ref: t.db });
      if (t.sql.trim() && (t.language ?? 'sql') === 'sql') {
        out.push({ id: `query:${t.id}`, kind: 'query', label: t.title, ref: t.sql });
      }
    }
  }
  return out;
}

/** The chips in force: the page's (less those removed) and the entry
 * point's; and the database a turn runs in. */
export function useChips(): { auto: Chip[]; extra: Chip[]; all: Chip[]; database: string | null } {
  const { rel } = useRel();
  const tabs = useTabs((s) => s.tabs);
  const hidden = useAssistant((s) => s.hidden);
  const extra = useAssistant((s) => s.extra);
  const auto = chipsFor(rel, tabs).filter((c) => !hidden.includes(c.id) && !extra.some((e) => e.id === c.id));
  const all = [...auto, ...extra];
  const database = all.find((c) => c.kind === 'database')?.ref ?? null;
  return { auto, extra, all, database };
}
