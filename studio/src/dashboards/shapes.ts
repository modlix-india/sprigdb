// The country shapes of filled maps: Natural Earth's 1:50m countries
// from world-atlas (241 shapes, small island countries and city states
// included), a TopoJSON file bundled with Studio (served from Studio's own
// origin, never a CDN). MapView imports this module only when it draws a
// filled map, so the file loads then and only then.
//
// TopoJSON keeps each border once, as an "arc" of quantized, delta-coded
// points; a polygon's rings list the arcs they follow (~i is arc i
// reversed). `decode` turns that into GeoJSON rings, as topojson-client's
// `feature` does for polygons.

import topology from 'world-atlas/countries-50m.json';
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
  /** [[west, south], [east, north]], of the parts on the same side of
   * the antimeridian as the largest (so the United States' Aleutians past
   * 180° and Russia's Chukotka don't stretch them round the world). */
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

/**
 * A ring that crosses the antimeridian (Russia's Chukotka, a Fijian
 * island) made continuous: its longitudes run on past 180° (or -180°)
 * instead of jumping to the other edge, which a flat map would fill as a
 * band across the world; the map's tiler wraps the part past the edge
 * back round. A ring that jumps an odd number of times goes round the
 * pole (Antarctica's) and is left as it is.
 */
export function unwrap(r: Ring): Ring {
  let jumps = 0;
  for (let i = 1; i < r.length; i++) if (Math.abs(r[i]![0]! - r[i - 1]![0]!) > 180) jumps++;
  if (!jumps || jumps % 2) return r;
  let shift = 0;
  const out = r.map(([x, y], i) => {
    if (i > 0) {
      const d = x! - r[i - 1]![0]!;
      if (d > 180) shift -= 360;
      else if (d < -180) shift += 360;
    }
    return [x! + shift, y!];
  });
  // Mostly on the map's own longitudes, whichever side it started on.
  const mean = out.reduce((a, [x]) => a + x!, 0) / out.length;
  const back = mean > 180 ? -360 : mean < -180 ? 360 : 0;
  return back ? out.map(([x, y]) => [x! + back, y!]) : out;
}

/** Decodes a topology's countries. */
export function decode(t: Topology): Shape[] {
  const arcs = arcsOf(t);
  return t.objects.countries.geometries
    .filter((g) => g.type === 'Polygon' || g.type === 'MultiPolygon')
    .map((g) => {
      const polys = (g.type === 'Polygon' ? [g.arcs as number[][]] : (g.arcs as number[][][])).map((p) => p.map((r) => unwrap(ring(arcs, r))));
      // Each part's outer ring: its extent and (planar) size.
      const parts = polys.map((p) => {
        let w = 180;
        let s = 90;
        let e = -180;
        let n = -90;
        let area = 0;
        const outer = p[0] ?? [];
        // A point on the antimeridian counts on the part's own side.
        const east = outer.reduce((a, [x]) => a + x!, 0) > 0;
        outer.forEach(([x0, y], i) => {
          const x = Math.abs(x0!) === 180 ? (east ? 180 : -180) : x0!;
          w = Math.min(w, x);
          e = Math.max(e, x);
          s = Math.min(s, y!);
          n = Math.max(n, y!);
          const [x2, y2] = outer[(i + 1) % outer.length]!;
          area += x0! * y2! - x2! * y!;
        });
        return { w, s, e, n, area: Math.abs(area), mid: (w + e) / 2 };
      });
      const main = parts.reduce((a, b) => (b.area > a.area ? b : a), parts[0]!);
      let w = 180;
      let s = 90;
      let e = -180;
      let n = -90;
      for (const p of parts) {
        if (Math.abs(p.mid - main.mid) > 180) continue;
        w = Math.min(w, p.w);
        e = Math.max(e, p.e);
        s = Math.min(s, p.s);
        n = Math.max(n, p.n);
      }
      return {
        key: shapeKey(g.id, g.properties.name),
        name: g.properties.name,
        polygons: polys,
        bounds: [
          [Math.max(-180, w), s],
          [Math.min(180, e), n],
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
