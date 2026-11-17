import { describe, expect, it } from 'vitest';
import { countryKey, norm } from './countries';
import { filledMap, step, STEPS, type FilledMap } from './map';
import { describePlan, plan } from './plan';
import { countries } from './shapes';
import { normalize, type ChartSpec } from './spec';

describe('matching a country column', () => {
  it('reads names, ISO alpha-2, alpha-3 and numeric codes, in any case', () => {
    for (const v of ['US', 'us', 'USA', 'usa', 840, '840', 'United States of America', 'United States', 'U.S.A.', 'u.s.', ' america ']) {
      expect(countryKey(v), String(v)).toBe('840');
    }
    for (const v of ['GB', 'GBR', 826, 'United Kingdom', 'UK', 'Great Britain']) expect(countryKey(v), String(v)).toBe('826');
    expect(countryKey('DE')).toBe('276');
    expect(countryKey('deu')).toBe('276');
    expect(countryKey('Germany')).toBe('276');
    // Codes padded or not; accents, apostrophes and "&" don't matter.
    expect(countryKey(76)).toBe('076');
    expect(countryKey('076')).toBe('076');
    expect(countryKey("Côte d'Ivoire")).toBe('384');
    expect(countryKey('Cote d’Ivoire')).toBe('384');
    expect(countryKey('Ivory Coast')).toBe('384');
    expect(countryKey('Bosnia & Herzegovina')).toBe('070');
    expect(countryKey('Korea, Republic of')).toBe('410');
    expect(countryKey('Democratic Republic of the Congo')).toBe('180');
    expect(countryKey('Congo')).toBe('178');
    expect(countryKey('Russian Federation')).toBe('643');
    expect(countryKey('UAE')).toBe('784');
    expect(countryKey('Türkiye')).toBe('792');
    // Kosovo has no numeric code.
    expect(countryKey('XK')).toBe('XK');
    expect(countryKey('Kosovo')).toBe('XK');
  });

  it('matches nothing for other places, countries without a shape and junk', () => {
    for (const v of ['Atlantis', 'SG', 'Singapore', '702', 'ZZ', 'XYZ', '', '  ', null, undefined, 0, 1000, 8.5, true, 'England', 'California']) {
      expect(countryKey(v), String(v)).toBeNull();
    }
  });

  it('normalises names for comparing', () => {
    expect(norm("  The Côte d'Ivoire ")).toBe('cote divoire');
    expect(norm('Trinidad & Tobago')).toBe('trinidad and tobago');
    expect(norm('Myanmar (Burma)')).toBe('myanmar burma');
  });
});

describe('the country shapes', () => {
  const shapes = countries();

  it('decode to every country of the file, each with its key and bounds', () => {
    expect(shapes.length).toBe(177);
    const keyed = shapes.filter((s) => s.key !== null);
    expect(keyed.length).toBe(175);
    expect(new Set(keyed.map((s) => s.key)).size).toBe(175);
    // Every shape's own name, and every key, matches its shape.
    for (const s of keyed) {
      expect(countryKey(s.name), s.name).toBe(s.key);
      expect(countryKey(s.key), s.name).toBe(s.key);
    }
    const us = shapes.find((s) => s.key === '840')!;
    expect(us.name).toBe('United States of America');
    const [[w, south], [e, n]] = us.bounds;
    expect(w).toBeGreaterThan(-180);
    expect(e).toBeLessThan(-60);
    expect(south).toBeGreaterThan(15);
    expect(n).toBeLessThan(75);
    // Rings are closed.
    for (const s of shapes) {
      for (const poly of s.polygons) {
        for (const r of poly) expect(r[0], s.name).toEqual(r[r.length - 1]);
      }
    }
  });
});

describe('filled maps', () => {
  const COLS = [
    { name: 'country', type: 'VARCHAR' },
    { name: 'city', type: 'VARCHAR' },
    { name: 'revenue', type: 'DECIMAL' },
    { name: 'orders', type: 'BIGINT' },
  ];
  // The same country written three ways; rows that match nothing.
  const ROWS: unknown[][] = [
    ['US', 'Austin', '100', 2],
    ['United States', 'Boston', 300, 4],
    ['usa', 'Denver', '200', 3],
    ['DE', 'Berlin', 50, 1],
    ['Germany', 'Munich', 70, 1],
    ['IN', 'Pune', 10, 5],
    ['Atlantis', 'Poseidonia', 999, 9],
    [null, 'Nowhere', 5, 1],
    ['', 'Blank', 5, 1],
  ];
  const fm = (s: ChartSpec, rows = ROWS): FilledMap => {
    const p = plan(normalize(s, COLS), COLS, rows);
    if (p.kind !== 'filled') throw new Error(`not a filled map: ${p.kind}`);
    return p;
  };
  const value = (m: FilledMap, key: string) => m.regions.find((r) => r.key === key)?.value;

  it('aggregates each country’s rows with the measure’s own aggregation, after matching', () => {
    const sum = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(sum.regions.map((r) => r.key).sort()).toEqual(['276', '356', '840']);
    expect(value(sum, '840')).toBe(600);
    expect(value(sum, '276')).toBe(120);
    expect(sum.measure).toEqual({ label: 'SUM(revenue)', min: 10, max: 600 });
    expect(sum.regions.find((r) => r.key === '840')!.rows).toBe(3);
    // Rows that match nothing are left out and counted, with a few values.
    expect(sum.unmatched).toBe(3);
    expect(sum.examples).toEqual(['Atlantis', 'NULL', '']);
    // Other aggregations work over the matched rows, not per spelling.
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'avg' }] }), '840')).toBe(200);
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'median' }] }), '840')).toBe(200);
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'max' }] }), '276')).toBe(70);
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'city', agg: 'countd' }] }), '840')).toBe(3);
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'orders', agg: 'count' }] }), '276')).toBe(2);
  });

  it('takes the measure from Colour first, adds tooltip measures, and applies filters', () => {
    const m = fm({
      mark: 'map-filled',
      columns: ['country'],
      rows: [{ field: 'revenue', agg: 'sum' }],
      color: { field: 'orders', agg: 'sum' },
      tooltip: [{ field: 'revenue', agg: 'avg' }],
      filters: [{ field: 'city', in: ['Austin', 'Boston', 'Berlin'] }],
    });
    expect(m.measure.label).toBe('SUM(orders)');
    expect(value(m, '840')).toBe(6);
    expect(m.regions.find((r) => r.key === '840')!.tip).toEqual([{ label: 'AVG(revenue)', value: 200 }]);
    expect(m.unmatched).toBe(0);
    // A grouped query's bare measure gets SUM, as elsewhere.
    expect(value(fm({ mark: 'map-filled', columns: ['country'], rows: ['revenue'] }), '356')).toBe(10);
  });

  it('steps values on an equal-interval scale', () => {
    const m = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(step(m, 10)).toBe(0);
    expect(step(m, 600)).toBe(STEPS - 1);
    expect(step(m, 120)).toBe(1);
    expect(step(m, null)).toBeNull();
    const one = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] }, [['FR', 'Paris', 5, 1]]);
    expect(step(one, 5)).toBe(STEPS - 1);
  });

  it('says what is missing, and describes itself', () => {
    const none = plan(normalize({ mark: 'map-filled', columns: ['country'] }, [COLS[0]!]), [COLS[0]!], [['US']]);
    expect(none.kind === 'empty' && none.why).toMatch(/needs a measure/);
    const city = plan(normalize({ mark: 'map-filled', columns: ['city'], rows: [{ field: 'revenue', agg: 'sum' }] }, COLS), COLS, ROWS);
    expect(city.kind === 'empty' && city.why).toMatch(/needs a country field/);
    const m = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] });
    expect(describePlan(m, 'Sales')).toBe('Sales: map of 3 countries coloured by SUM(revenue), 3 rows not matched to a country');
    expect(filledMap(normalize({ mark: 'map-filled', columns: ['city'] }, COLS), COLS, ROWS)).toBeNull();
  });
});
