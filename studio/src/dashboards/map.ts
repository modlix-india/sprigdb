// Point maps: a tile's rows as points at their latitude and longitude,
// sized by the Size shelf and coloured by the Colour shelf. Worked out
// without React or MapLibre so it can be tested; MapView draws it.

import { fieldsOf, grouped, labelOf, num, type Cell } from './data';
import { isLat, isLon, isMeasure, type ColumnInfo, type Field, type Spec } from './spec';

const isGeo = (f: Field) => isLat(f.field) || isLon(f.field);

/** The most points one map draws. */
export const MAX_POINTS = 5000;

export interface MapPoint {
  lon: number;
  lat: number;
  /** The Size shelf's value. */
  size: number | null;
  /** The Colour shelf's value: a category's index in `colour.values`, or the measure. */
  colour: number | null;
  /** The fields shown on hover, in shelf order. */
  tip: { label: string; value: Cell; measure: boolean }[];
}

export interface PointMap {
  kind: 'map';
  points: MapPoint[];
  size: { label: string; min: number; max: number } | null;
  colour: { kind: 'category'; label: string; values: string[] } | { kind: 'measure'; label: string; min: number; max: number } | null;
  /** Points past MAX_POINTS. */
  cut: number;
  /** Rows without a usable latitude and longitude. */
  skipped: number;
}

/** The point map of a spec over a result, or null when no shelf holds both
 * a latitude and a longitude field. Locations are never aggregated: rows
 * at one place are one point, their measures aggregated. */
export function pointMap(s: Spec, columns: ColumnInfo[], rows: unknown[][]): PointMap | null {
  const all = [...s.columns, ...s.rows, ...s.detail, ...s.tooltip, ...(s.label ? [s.label] : []), ...(s.color ? [s.color] : [])];
  const lat = all.find((f) => isLat(f.field));
  const lon = all.find((f) => isLon(f.field));
  if (!lat || !lon) return null;
  const others = [...s.columns, ...s.rows].filter((f) => !isGeo(f));
  const colour = s.color && !isGeo(s.color) ? s.color : null;
  const size = s.size && !isGeo(s.size) ? { ...s.size, agg: s.size.agg ?? 'sum' } : null;
  const label = s.label && !isGeo(s.label) ? s.label : null;
  const tips = s.tooltip.filter((f) => !isGeo(f));
  const inner: Spec = {
    ...s,
    columns: [{ field: lat.field }, { field: lon.field }, ...others],
    rows: [],
    color: colour,
    size,
    label,
    detail: [...s.detail.filter((f) => !isGeo(f)), ...tips.filter((f) => !isMeasure(f)), ...(label && !isMeasure(label) ? [label] : [])],
    tooltip: tips.filter(isMeasure),
    facet: null,
  };
  const recs = grouped(inner, columns, rows);
  const { dims, measures } = fieldsOf(inner);
  const shown = [...dims, ...measures].filter((f) => !isGeo(f));
  const sizeKey = size ? labelOf(size) : null;
  const colourKey = colour ? labelOf(colour) : null;
  const colourIsMeasure = !!colour && isMeasure(colour);
  const categories: string[] = [];
  const points: MapPoint[] = [];
  let skipped = 0;
  for (const r of recs) {
    const la = num(r[lat.field]);
    const lo = num(r[lon.field]);
    if (la === null || lo === null || Math.abs(la) > 90 || Math.abs(lo) > 180) {
      skipped++;
      continue;
    }
    let c: number | null = null;
    if (colourKey) {
      if (colourIsMeasure) c = num(r[colourKey]);
      else {
        const v = String(r[colourKey] ?? 'NULL');
        let i = categories.indexOf(v);
        if (i < 0) {
          categories.push(v);
          i = categories.length - 1;
        }
        c = i;
      }
    }
    points.push({
      lon: lo,
      lat: la,
      size: sizeKey ? num(r[sizeKey]) : null,
      colour: c,
      tip: shown.map((f) => ({ label: labelOf(f), value: r[labelOf(f)] ?? null, measure: isMeasure(f) })),
    });
  }
  const cut = Math.max(0, points.length - MAX_POINTS);
  const kept = points.slice(0, MAX_POINTS);
  const range = (xs: (number | null)[]) => {
    const ns = xs.filter((x): x is number => x !== null);
    return ns.length ? { min: Math.min(...ns), max: Math.max(...ns) } : { min: 0, max: 0 };
  };
  return {
    kind: 'map',
    points: kept,
    size: sizeKey ? { label: sizeKey, ...range(kept.map((p) => p.size)) } : null,
    colour: !colourKey
      ? null
      : colourIsMeasure
        ? { kind: 'measure', label: colourKey, ...range(kept.map((p) => p.colour)) }
        : { kind: 'category', label: colourKey, values: categories },
    cut,
    skipped,
  };
}

/** A point's radius in pixels: by area, 4 to 22 (6 without a Size field). */
export function radius(m: PointMap, p: MapPoint): number {
  if (!m.size || p.size === null) return 6;
  const { min, max } = m.size;
  const t = max > min ? (p.size - min) / (max - min) : 1;
  return 4 + 18 * Math.sqrt(Math.max(0, Math.min(1, t)));
}

/** The bounds of the points: [[west, south], [east, north]], or null. */
export function bounds(m: PointMap): [[number, number], [number, number]] | null {
  if (!m.points.length) return null;
  let w = 180;
  let s = 90;
  let e = -180;
  let n = -90;
  for (const p of m.points) {
    w = Math.min(w, p.lon);
    e = Math.max(e, p.lon);
    s = Math.min(s, p.lat);
    n = Math.max(n, p.lat);
  }
  return [
    [w, s],
    [e, n],
  ];
}

/** Whether a `studio.map_tile_url` is an XYZ raster template Studio can use:
 * http(s), with `{z}`, `{x}` and `{y}`. */
export function tileTemplate(url: string): string | null {
  const u = url.trim();
  if (!/^https?:\/\//i.test(u)) return null;
  if (!u.includes('{z}') || !u.includes('{x}') || !u.includes('{y}')) return null;
  return u;
}
