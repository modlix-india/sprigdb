// The country shapes of filled maps: Natural Earth's 1:110m countries
// from world-atlas, a TopoJSON file bundled with Studio (served from
// Studio's own origin, never a CDN). MapView imports this module only
// when it draws a filled map, so the file loads then and only then.
//
// TopoJSON keeps each border once, as an "arc" of quantized, delta-coded
// points; a polygon's rings list the arcs they follow (~i is arc i
// reversed). `decode` turns that into GeoJSON rings, as topojson-client's
// `feature` does for polygons.

import topology from 'world-atlas/countries-110m.json';
import { shapeKey } from './countries';

type Ring = number[][];

interface Topology {
  transform: { scale: [number, number]; translate: [number, number] };
  arcs: [number, number][][];
  objects: {
    countries: {
      geometries: { type: string; id?: string; arcs: number[][] | number[][][]; properties: { name: string } }[];
    };
  };
}

/** A country's shape, in GeoJSON form, with its key and bounds. */
export interface Shape {
  /** countries.ts's key, or null (a shape without a code). */
  key: string | null;
  name: string;
  /** Polygons, each its rings (outer first). */
  polygons: Ring[][];
  /** [[west, south], [east, north]]. */
  bounds: [[number, number], [number, number]];
}

/** The arcs as absolute longitudes and latitudes. */
function arcsOf(t: Topology): Ring[] {
  const [sx, sy] = t.transform.scale;
  const [tx, ty] = t.transform.translate;
  return t.arcs.map((arc) => {
    let x = 0;
    let y = 0;
    return arc.map(([dx, dy]) => {
      x += dx;
      y += dy;
      return [x * sx + tx, y * sy + ty];
    });
  });
}

/** A ring from its arcs, each arc's first point dropped after the first
 * (it repeats the previous arc's last). */
function ring(arcs: Ring[], ids: number[]): Ring {
  const out: Ring = [];
  for (const i of ids) {
    const a = i < 0 ? [...arcs[~i]!].reverse() : arcs[i]!;
    out.push(...(out.length ? a.slice(1) : a));
  }
  return out;
}

/** Decodes a topology's countries. */
export function decode(t: Topology): Shape[] {
  const arcs = arcsOf(t);
  return t.objects.countries.geometries
    .filter((g) => g.type === 'Polygon' || g.type === 'MultiPolygon')
    .map((g) => {
      const polys = (g.type === 'Polygon' ? [g.arcs as number[][]] : (g.arcs as number[][][])).map((p) => p.map((r) => ring(arcs, r)));
      let w = 180;
      let s = 90;
      let e = -180;
      let n = -90;
      for (const p of polys) {
        for (const [x, y] of p[0] ?? []) {
          w = Math.min(w, x!);
          e = Math.max(e, x!);
          s = Math.min(s, y!);
          n = Math.max(n, y!);
        }
      }
      return {
        key: shapeKey(g.id, g.properties.name),
        name: g.properties.name,
        polygons: polys,
        bounds: [
          [w, s],
          [e, n],
        ],
      };
    });
}

let shapes: Shape[] | null = null;

/** Every country's shape, decoded once. */
export function countries(): Shape[] {
  shapes ??= decode(topology as unknown as Topology);
  return shapes;
}
