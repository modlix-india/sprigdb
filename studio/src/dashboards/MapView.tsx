// Point and filled maps drawn with MapLibre GL. Loaded on first use
// (ChartView imports it lazily), so the main bundle doesn't carry MapLibre.
//
// - A filled map's country shapes (shapes.ts, about 100 KB) load only when
//   one is drawn, from Studio's own origin.
// - Base map: `studio.map_tile_url` when it is an XYZ raster template
//   (the gateway adds its origin to the CSP). Without one the points sit on
//   a plain background with a 30° graticule and nothing is fetched from
//   anywhere: no tiles, glyphs or sprites.
// - The worker is MapLibre's own module, served by Studio (worker-src 'self').
// - Colours follow the theme: the chart palette for categories, the
//   sequential ramp for a colour measure and a filled map's steps; a dark
//   theme dims raster tiles.

import { useEffect, useMemo, useRef, useState } from 'react';
import { Map as MlMap, NavigationControl, setWorkerUrl, type StyleSpecification } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?url';
import { useSession } from '../auth/session';
import { isDark } from '../app/theme';
import type { Palette } from './ChartView';
import { bounds, radius, step, tileTemplate, STEPS, type FilledMap, type PointMap } from './map';
import type { Shape } from './shapes';

setWorkerUrl(workerUrl);

/** The GeoJSON this view makes. */
interface Feature {
  type: 'Feature';
  id?: number;
  properties: Record<string, unknown>;
  geometry:
    | { type: 'Point'; coordinates: number[] }
    | { type: 'LineString'; coordinates: number[][] }
    | { type: 'MultiPolygon'; coordinates: number[][][][] };
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

/** The background, and the tiles when there are any (dimmed in a dark
 * theme). */
function base(pal: Palette, tiles: string | null): StyleSpecification {
  const style: StyleSpecification = {
    version: 8,
    sources: {},
    layers: [{ id: 'background', type: 'background', paint: { 'background-color': pal.surface } }],
  };
  if (tiles) {
    style.sources.base = { type: 'raster', tiles: [tiles], tileSize: 256, maxzoom: 19 };
    style.layers.push({
      id: 'base',
      type: 'raster',
      source: 'base',
      paint: isDark() ? { 'raster-brightness-max': 0.7, 'raster-saturation': -0.4 } : {},
    });
  }
  return style;
}

function pointStyle(m: PointMap, pal: Palette, tiles: string | null): StyleSpecification {
  const points: Collection = {
    type: 'FeatureCollection',
    features: m.points.map((p, i) => ({
      type: 'Feature',
      id: i,
      properties: { i, r: radius(m, p), c: colourOf(m, p.colour, pal) },
      geometry: { type: 'Point', coordinates: [p.lon, p.lat] },
    })),
  };
  const style = base(pal, tiles);
  style.sources.points = { type: 'geojson', data: points };
  if (!tiles) {
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

/** A step's colour: the sequential ramp past its lightest (light theme) or
 * darkest (dark theme) shade, which is too close to the background. */
const stepColour = (pal: Palette, k: number) => pal.seq[Math.min(pal.seq.length - 1, k + 1)]!;

/** Countries without data, and Antarctica unless it has some. */
function filledStyle(m: FilledMap, shapes: Shape[] | null, pal: Palette, tiles: string | null): StyleSpecification {
  const style = base(pal, tiles);
  if (!shapes) return style;
  const at = new Map(m.regions.map((r, i) => [r.key, i]));
  const features: Feature[] = [];
  shapes.forEach((sh, n) => {
    const i = sh.key === null ? undefined : at.get(sh.key);
    if (i === undefined && sh.key === '010') return;
    const k = i === undefined ? null : step(m, m.regions[i]!.value);
    features.push({
      type: 'Feature',
      id: n,
      properties: { i: i ?? -1, s: n, d: k !== null, c: k === null ? pal.grid : stepColour(pal, k) },
      geometry: { type: 'MultiPolygon', coordinates: sh.polygons },
    });
  });
  style.sources.regions = { type: 'geojson', data: { type: 'FeatureCollection', features } as Collection };
  style.layers.push({
    id: 'regions',
    type: 'fill',
    source: 'regions',
    // Over tiles, countries without data stay clear.
    paint: { 'fill-color': ['get', 'c'], 'fill-opacity': tiles ? ['case', ['get', 'd'], 0.75, 0] : 1 },
  });
  style.layers.push({
    id: 'borders',
    type: 'line',
    source: 'regions',
    paint: { 'line-color': tiles ? pal.muted : pal.surface, 'line-width': 0.6 },
  });
  return style;
}

/** The bounds of the countries with data, or the world when they span
 * most of it (or there are none). */
function filledBounds(m: FilledMap, shapes: Shape[] | null): [[number, number], [number, number]] | null {
  if (!shapes) return null;
  const keys = new Set(m.regions.map((r) => r.key));
  let w = 180;
  let s = 90;
  let e = -180;
  let n = -90;
  for (const sh of shapes) {
    if (sh.key === null || !keys.has(sh.key)) continue;
    w = Math.min(w, sh.bounds[0][0]);
    s = Math.min(s, sh.bounds[0][1]);
    e = Math.max(e, sh.bounds[1][0]);
    n = Math.max(n, sh.bounds[1][1]);
  }
  if (w > e || e - w > 250) return [
    [-180, -56],
    [180, 80],
  ];
  return [
    [w, s],
    [e, n],
  ];
}

function FilledLegend({ m, pal, format }: { m: FilledMap; pal: Palette; format: (v: unknown) => string }) {
  const { min, max, label } = m.measure;
  const width = (max - min) / STEPS;
  return (
    <div className="map-legend small" data-testid="map-legend">
      <div>{label}</div>
      <ul aria-label={`Colour: ${label}, ${format(min)} to ${format(max)}`} className="map-steps">
        <li>{format(min)}</li>
        {Array.from({ length: STEPS }, (_, k) => (
          <li key={k} title={max > min ? `${format(min + k * width)} to ${format(min + (k + 1) * width)}` : format(max)}>
            <i aria-hidden="true" style={{ background: stepColour(pal, k) }} />
          </li>
        ))}
        <li>{format(max)}</li>
        <li>
          <i aria-hidden="true" style={{ background: pal.grid }} />
          No data
        </li>
      </ul>
    </div>
  );
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

export default function MapView({ m, pal, format }: { m: PointMap | FilledMap; pal: Palette; format: (v: unknown) => string }) {
  const box = useRef<HTMLDivElement>(null);
  const map = useRef<MlMap | null>(null);
  const setting = useSession((s) => s.me?.studio.map_tile_url ?? '');
  const tiles = tileTemplate(setting);
  const [failed, setFailed] = useState<string | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; i: number; shape?: number } | null>(null);
  const [shapes, setShapes] = useState<Shape[] | null>(null);
  const filled = m.kind === 'filled';
  // A filled map's shapes: loaded on first use.
  useEffect(() => {
    if (!filled || shapes) return;
    let live = true;
    import('./shapes')
      .then((x) => {
        if (live) setShapes(x.countries());
      })
      .catch((e: unknown) => {
        if (live) setFailed(`the country shapes didn’t load: ${e instanceof Error ? e.message : String(e)}`);
      });
    return () => {
      live = false;
    };
  }, [filled, shapes]);
  const style = useMemo(() => (m.kind === 'filled' ? filledStyle(m, shapes, pal, tiles) : pointStyle(m, pal, tiles)), [m, pal, tiles, shapes]);
  const area = useMemo(() => (m.kind === 'filled' ? filledBounds(m, shapes) : bounds(m)), [m, shapes]);

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
    // Hover: the point or country under the pointer, from whichever of
    // the two layers the current style has (a tile may switch between them).
    made.on('mousemove', (e) => {
      const layers = ['points', 'regions'].filter((l) => made.getLayer(l));
      const p = layers.length ? made.queryRenderedFeatures(e.point, { layers })[0]?.properties : undefined;
      const i = typeof p?.i === 'number' ? p.i : null;
      made.getCanvas().style.cursor = i === null || i < 0 ? '' : 'pointer';
      setHover(i === null ? null : { x: e.point.x, y: e.point.y, i, shape: typeof p?.s === 'number' ? p.s : undefined });
    });
    made.on('mouseout', () => {
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
    else mm.fitBounds(area, { padding: 32, maxZoom: filled ? 5 : 12, animate: false });
  }, [area, filled]);

  if (!hasWebgl()) return <p className="muted chart-note">This browser can’t draw maps: it has no WebGL.</p>;
  if (failed) return <p className="muted chart-note">This browser can’t draw maps here ({failed}).</p>;
  const badTiles = !!setting.trim() && !tiles;
  const tileNote = badTiles && 'studio.map_tile_url isn’t a {z}/{x}/{y} tile template, so there is no base map.';
  const dark = isDark() ? 'map-wrap map-dark' : 'map-wrap';
  if (m.kind === 'filled') {
    const region = hover && hover.i >= 0 ? m.regions[hover.i] : null;
    const shape = hover?.shape !== undefined ? shapes?.[hover.shape] : undefined;
    return (
      <div className={dark} data-testid="filled-map" data-regions={m.regions.length} data-shapes={shapes ? 'on' : 'off'} data-tiles={tiles ? 'on' : 'off'}>
        <div ref={box} className="map-box" />
        <FilledLegend m={m} pal={pal} format={format} />
        {(m.unmatched > 0 || badTiles || !shapes) && (
          <p className="muted chart-cut map-note" data-testid="map-note">
            {!shapes && 'Loading the country shapes… '}
            {m.unmatched > 0 &&
              `${m.unmatched} ${m.unmatched === 1 ? 'row' : 'rows'} not matched to a country (${m.examples.join(', ')}${m.unmatched > m.examples.length ? ', …' : ''}). `}
            {tileNote}
          </p>
        )}
        {hover && shape && (
          <div className="chart-tip map-tip" style={{ left: hover.x + 12, top: hover.y + 12 }} role="status">
            <div>
              <b>{shape.name}</b>
            </div>
            {region ? (
              <>
                <div>
                  {m.measure.label}: <b>{format(region.value)}</b>
                </div>
                {region.tip.map((t) => (
                  <div key={t.label}>
                    {t.label}: <b>{format(t.value)}</b>
                  </div>
                ))}
              </>
            ) : (
              <div className="muted">No data</div>
            )}
          </div>
        )}
      </div>
    );
  }
  const tip = hover ? m.points[hover.i] : null;
  return (
    <div className={dark} data-testid="point-map" data-points={m.points.length} data-tiles={tiles ? 'on' : 'off'}>
      <div ref={box} className="map-box" />
      <Legend m={m} pal={pal} format={format} />
      {(m.cut > 0 || m.skipped > 0 || badTiles) && (
        <p className="muted chart-cut map-note">
          {m.cut > 0 && `${m.cut} more places not shown. `}
          {m.skipped > 0 && `${m.skipped} rows without a valid latitude and longitude. `}
          {tileNote}
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
