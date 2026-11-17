// The dashboards pages, loaded on first use (they bring the charting library).

import { lazy, Suspense } from 'react';

const DashboardsPage = lazy(() => import('./DashboardsPage').then((m) => ({ default: m.DashboardsPage })));
const TileEditor = lazy(() => import('./TileEditor').then((m) => ({ default: m.TileEditor })));

const loading = <p className="muted">Loading…</p>;

export function DashboardsRoute() {
  return (
    <Suspense fallback={loading}>
      <DashboardsPage />
    </Suspense>
  );
}

export function TileEditorRoute() {
  return (
    <Suspense fallback={loading}>
      <TileEditor />
    </Suspense>
  );
}
