// Maps, worked out without React or MapLibre so they can be tested;
// MapView draws them.
// - Point maps: a tile's rows as points at their latitude and longitude,
//   sized by the Size shelf and coloured by the Colour shelf.
// - Filled maps: countries coloured by a measure, the rows matched to
//   countries by name or ISO code (countries.ts) and aggregated per
//   country with the measure's own aggregation.

import { countryKey } from './countries';
import { aggregate, fieldsOf, filtered, grouped, labelOf, num, type Cell } from './data';
import { isCountry, isLat, isLon, isMeasure, type ColumnInfo, type Field, type Spec } from './spec';

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

// ---- filled maps ----

/** One country of a filled map. */
export interface Region {
  /** The shape key (countries.ts): the numeric code, or `XK`. */
  key: string;
  /** The measure, aggregated over the country's rows. */
  value: number | null;
  /** The tooltip's measures, aggregated likewise. */
  tip: { label: string; value: number | null }[];
  /** Rows matched to it. */
  rows: number;
}

export interface FilledMap {
  kind: 'filled';
  /** The country field. */
  country: string;
  /** The measure's label and range over the regions. */
  measure: { label: string; min: number; max: number };
  regions: Region[];
  /** Rows whose country matched no country shape (or was empty). */
  unmatched: number;
  /** A few of those rows' values, as written. */
  examples: string[];
}

/** The steps of a filled map's colour scale. */
export const STEPS = 6;

/** The filled map of a spec over a result: the first country field on a
 * shelf, and the measure on Colour (else the first on Columns or Rows,
 * else Size). Null without a country field; `measure` null without a
 * measure. */
export function filledMap(s: Spec, columns: ColumnInfo[], rows: unknown[][]): FilledMap | { kind: 'filled'; measure: null } | null {
  const all = [...s.columns, ...s.rows, ...(s.color ? [s.color] : []), ...s.detail, ...s.tooltip, ...(s.label ? [s.label] : [])];
  const country = all.find((f) => isCountry(f.field));
  if (!country) return null;
  const measure: Field | undefined =
    (s.color && isMeasure(s.color) ? s.color : undefined) ??
    [...s.columns, ...s.rows].find((f) => isMeasure(f) && !isCountry(f.field)) ??
    (s.size ? { ...s.size, agg: s.size.agg ?? 'sum' } : undefined);
  if (!measure) return { kind: 'filled', measure: null };
  const tips = s.tooltip.filter((f) => isMeasure(f) && labelOf(f) !== labelOf(measure));
  const { recs, get } = filtered(s, columns, rows);
  const byKey = new Map<string, { vals: unknown[][]; rows: number }>();
  let unmatched = 0;
  const examples: string[] = [];
  for (const r of recs) {
    const raw = get(r, country.field);
    const key = countryKey(raw);
    if (!key) {
      unmatched++;
      const shown = raw === null ? 'NULL' : String(raw);
      if (examples.length < 3 && !examples.includes(shown)) examples.push(shown);
      continue;
    }
    let g = byKey.get(key);
    if (!g) {
      g = { vals: [measure, ...tips].map(() => []), rows: 0 };
      byKey.set(key, g);
    }
    g.rows++;
    [measure, ...tips].forEach((f, i) => g.vals[i]!.push(get(r, f.field)));
  }
  const regions: Region[] = [...byKey.entries()].map(([key, g]) => ({
    key,
    value: aggregate(g.vals[0]!, measure.agg),
    tip: tips.map((f, i) => ({ label: labelOf(f), value: aggregate(g.vals[i + 1]!, f.agg) })),
    rows: g.rows,
  }));
  const ns = regions.map((x) => x.value).filter((v): v is number => v !== null);
  return {
    kind: 'filled',
    country: country.field,
    measure: { label: labelOf(measure), min: ns.length ? Math.min(...ns) : 0, max: ns.length ? Math.max(...ns) : 0 },
    regions,
    unmatched,
    examples,
  };
}

/** A value's step on the colour scale, 0 to STEPS - 1 (equal intervals
 * from the smallest value to the largest), or null without a value. */
export function step(m: FilledMap, v: number | null): number | null {
  if (v === null) return null;
  const { min, max } = m.measure;
  if (max <= min) return STEPS - 1;
  return Math.min(STEPS - 1, Math.max(0, Math.floor(((v - min) / (max - min)) * STEPS)));
}
