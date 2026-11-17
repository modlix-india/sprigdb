// A point map drawn with MapLibre GL. Loaded on first use (ChartView
// imports it lazily), so the main bundle doesn't carry MapLibre.
//
// - Base map: `studio.map_tile_url` when it is an XYZ raster template
//   (the gateway adds its origin to the CSP). Without one the points sit on
//   a plain background with a 30° graticule and nothing is fetched from
//   anywhere: no tiles, glyphs or sprites.
// - The worker is MapLibre's own module, served by Studio (worker-src 'self').
// - Colours follow the theme: the chart palette for categories, the
//   sequential ramp for a colour measure; a dark theme dims raster tiles.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Map as MlMap, NavigationControl, setWorkerUrl, type StyleSpecification } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?url';
import { useSession } from '../auth/session';
import { isDark } from '../app/theme';
import type { Palette } from './ChartView';
import { bounds, radius, tileTemplate, type PointMap } from './map';

setWorkerUrl(workerUrl);

/** The GeoJSON this view makes. */
interface Feature {
  type: 'Feature';
  id?: number;
  properties: Record<string, unknown>;
  geometry: { type: 'Point'; coordinates: number[] } | { type: 'LineString'; coordinates: number[][] };
}
interface Collection {
  type: 'FeatureCollection';
  features: Feature[];
}

/** Whether the browser can make a WebGL context (MapLibre needs one). */
let webgl: boolean | null = null;
function hasWebgl(): boolean {
  if (webgl === null) {
    try {
      const c = document.createElement('canvas');
      webgl = !!(c.getContext('webgl2') ?? c.getContext('webgl'));
    } catch {
      webgl = false;
    }
  }
  return webgl;
}

/** A point's colour on `pal`. */
function colourOf(m: PointMap, c: number | null, pal: Palette): string {
  if (!m.colour || c === null) return m.colour ? pal.muted : pal.series[0]!;
  if (m.colour.kind === 'category') return c < 8 ? pal.series[c]! : pal.muted;
  const { min, max } = m.colour;
  const t = max > min ? (c - min) / (max - min) : 1;
  return pal.seq[Math.min(pal.seq.length - 1, Math.max(1, Math.round(t * (pal.seq.length - 1))))]!;
}

/** Lines every 30 degrees, for a map without tiles. */
function graticule(): Collection {
  const features: Feature[] = [];
  for (let lon = -180; lon <= 180; lon += 30) {
    features.push({ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: [[lon, -85], [lon, 85]] } });
  }
  for (let lat = -60; lat <= 60; lat += 30) {
    const coords: [number, number][] = [];
    for (let lon = -180; lon <= 180; lon += 10) coords.push([lon, lat]);
    features.push({ type: 'Feature', properties: { equator: lat === 0 }, geometry: { type: 'LineString', coordinates: coords } });
  }
  return { type: 'FeatureCollection', features };
}

function styleOf(m: PointMap, pal: Palette, tiles: string | null): StyleSpecification {
  const dark = isDark();
  const points: Collection = {
    type: 'FeatureCollection',
    features: m.points.map((p, i) => ({
      type: 'Feature',
      id: i,
      properties: { i, r: radius(m, p), c: colourOf(m, p.colour, pal) },
      geometry: { type: 'Point', coordinates: [p.lon, p.lat] },
    })),
  };
  const style: StyleSpecification = {
    version: 8,
    sources: {
      points: { type: 'geojson', data: points },
    },
    layers: [{ id: 'background', type: 'background', paint: { 'background-color': pal.surface } }],
  };
  if (tiles) {
    style.sources.base = { type: 'raster', tiles: [tiles], tileSize: 256, maxzoom: 19 };
    style.layers.push({
      id: 'base',
      type: 'raster',
      source: 'base',
      paint: dark ? { 'raster-brightness-max': 0.7, 'raster-saturation': -0.4 } : {},
    });
  } else {
    style.sources.graticule = { type: 'geojson', data: graticule() };
    style.layers.push({
      id: 'graticule',
      type: 'line',
      source: 'graticule',
      paint: { 'line-color': pal.grid, 'line-width': ['case', ['get', 'equator'], 1.5, 0.75] },
    });
  }
  style.layers.push({
    id: 'points',
    type: 'circle',
    source: 'points',
    paint: {
      'circle-radius': ['get', 'r'],
      'circle-color': ['get', 'c'],
      'circle-opacity': 0.8,
      'circle-stroke-color': pal.surface,
      'circle-stroke-width': 1,
    },
  });
  return style;
}

function Legend({ m, pal, format }: { m: PointMap; pal: Palette; format: (v: unknown) => string }) {
  if (!m.colour && !m.size) return null;
  return (
    <div className="map-legend small" data-testid="map-legend">
      {m.colour?.kind === 'category' && (
        <ul aria-label={`Colour: ${m.colour.label}`}>
          {m.colour.values.slice(0, 8).map((v, i) => (
            <li key={v}>
              <i aria-hidden="true" style={{ background: pal.series[i] }} />
              {v}
            </li>
          ))}
          {m.colour.values.length > 8 && (
            <li>
              <i aria-hidden="true" style={{ background: pal.muted }} />
              {m.colour.values.length - 8} more
            </li>
          )}
        </ul>
      )}
      {m.colour?.kind === 'measure' && (
        <div>
          {m.colour.label}: {format(m.colour.min)}{' '}
          <span className="map-ramp" aria-hidden="true" style={{ background: `linear-gradient(to right, ${pal.seq.slice(1).join(', ')})` }} /> {format(m.colour.max)}
        </div>
      )}
      {m.size && (
        <div>
          Size: {m.size.label}, {format(m.size.min)} to {format(m.size.max)}
        </div>
      )}
    </div>
  );
}

export default function MapView({ m, pal, format }: { m: PointMap; pal: Palette; format: (v: unknown) => string }) {
  const box = useRef<HTMLDivElement>(null);
  const map = useRef<MlMap | null>(null);
  const setting = useSession((s) => s.me?.studio.map_tile_url ?? '');
  const tiles = tileTemplate(setting);
  const [failed, setFailed] = useState<string | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; i: number } | null>(null);
  const style = useMemo(() => styleOf(m, pal, tiles), [m, pal, tiles]);
  const area = useMemo(() => bounds(m), [m]);

  // The map: made once, its style swapped (diffed) when the data or theme changes.
  useEffect(() => {
    if (!box.current) return;
    let made: MlMap;
    try {
      made = new MlMap({
        container: box.current,
        style,
        attributionControl: tiles ? { compact: true, customAttribution: `Map tiles: ${new URL(tiles.replace(/[{}]/g, '')).host}` } : false,
        renderWorldCopies: false,
        maxZoom: 18,
        dragRotate: false,
        pitchWithRotate: false,
        touchPitch: false,
      });
    } catch (e) {
      const why = e instanceof Error ? e.message : String(e);
      const t = window.setTimeout(() => setFailed(why));
      return () => window.clearTimeout(t);
    }
    made.addControl(new NavigationControl({ showCompass: false }), 'top-right');
    made.on('mousemove', 'points', (e) => {
      const f = e.features?.[0];
      const i = typeof f?.properties?.i === 'number' ? f.properties.i : null;
      made.getCanvas().style.cursor = i === null ? '' : 'pointer';
      setHover(i === null ? null : { x: e.point.x, y: e.point.y, i });
    });
    made.on('mouseleave', 'points', () => {
      made.getCanvas().style.cursor = '';
      setHover(null);
    });
    map.current = made;
    // A tile resized (edit mode, the window): the map follows its box.
    const ro = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => made.resize());
    ro?.observe(box.current);
    return () => {
      ro?.disconnect();
      map.current = null;
      made.remove();
    };
    // Made once per mount; style changes are applied below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    map.current?.setStyle(style);
  }, [style]);

  useEffect(() => {
    const mm = map.current;
    if (!mm || !area) return;
    const [[w, s], [e, n]] = area;
    if (w === e && s === n) mm.jumpTo({ center: [w, s], zoom: 6 });
    else mm.fitBounds(area, { padding: 32, maxZoom: 12, animate: false });
  }, [area]);

  if (!hasWebgl()) return <p className="muted chart-note">This browser can’t draw maps: it has no WebGL.</p>;
  if (failed) return <p className="muted chart-note">This browser can’t draw maps here ({failed}).</p>;
  const tip = hover ? m.points[hover.i] : null;
  return (
    <div className={isDark() ? 'map-wrap map-dark' : 'map-wrap'} data-testid="point-map" data-points={m.points.length} data-tiles={tiles ? 'on' : 'off'}>
      <div ref={box} className="map-box" />
      <Legend m={m} pal={pal} format={format} />
      {(m.cut > 0 || m.skipped > 0 || (setting.trim() && !tiles)) && (
        <p className="muted chart-cut map-note">
          {m.cut > 0 && `${m.cut} more places not shown. `}
          {m.skipped > 0 && `${m.skipped} rows without a valid latitude and longitude. `}
          {setting.trim() && !tiles && 'studio.map_tile_url isn’t a {z}/{x}/{y} tile template, so there is no base map.'}
        </p>
      )}
      {tip && hover && (
        <div className="chart-tip map-tip" style={{ left: hover.x + 12, top: hover.y + 12 }} role="status">
          <div className="muted">
            {tip.lat.toFixed(4)}, {tip.lon.toFixed(4)}
          </div>
          {tip.tip.map((t) => (
            <div key={t.label}>
              {t.label}: <b>{t.measure ? format(t.value) : String(t.value ?? 'NULL')}</b>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
