import { describe, expect, it } from 'vitest';
import { countryKey, norm } from './countries';
import { filledMap, MAX_STEPS, quantiles, shade, step, type FilledMap } from './map';
import { describePlan, plan } from './plan';
import { countries, unwrap } from './shapes';
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

  it('matches small countries, islands and city states', () => {
    const cases: [string, unknown[]][] = [
      ['344', ['HK', 'hkg', 344, 'Hong Kong', 'Hong Kong SAR China', 'Hong Kong, China']],
      ['446', ['MO', 'MAC', 446, 'Macao', 'Macau', 'Macao SAR China']],
      ['702', ['SG', 'SGP', 702, '702', 'Singapore']],
      ['470', ['MT', 'MLT', 'Malta']],
      ['048', ['BH', 'BHR', 48, 'Bahrain']],
      ['480', ['MU', 'MUS', 'Mauritius']],
      ['462', ['MV', 'MDV', 'Maldives']],
      ['492', ['MC', 'Monaco']],
      ['336', ['VA', 'VAT', 'Vatican', 'Vatican City', 'Holy See']],
      ['662', ['LC', 'Saint Lucia', 'St. Lucia', 'St Lucia']],
      ['670', ['VC', 'St. Vincent & Grenadines', 'Saint Vincent and the Grenadines']],
      ['678', ['ST', 'São Tomé & Príncipe', 'Sao Tome and Principe']],
      ['531', ['CW', 'CUW', 'Curaçao', 'Curacao']],
      ['132', ['CV', 'Cabo Verde', 'Cape Verde']],
      ['234', ['FO', 'Faroe Islands', 'Faeroe Islands']],
      ['850', ['VI', 'VIR', 'U.S. Virgin Islands', 'Virgin Islands, U.S.']],
      ['092', ['VG', 'British Virgin Islands']],
      ['248', ['AX', 'ALA', 'Åland', 'Aland Islands']],
      ['583', ['FM', 'Micronesia', 'Federated States of Micronesia']],
      ['654', ['SH', 'St. Helena', 'Saint Helena, Ascension and Tristan da Cunha']],
    ];
    for (const [key, vs] of cases) for (const v of vs) expect(countryKey(v), String(v)).toBe(key);
  });

  it('matches nothing for other places, countries without a shape and junk', () => {
    for (const v of ['Atlantis', 'GI', 'Gibraltar', '292', 'Tokelau', 'CX', 'Christmas Island', 'Virgin Islands', 'ZZ', 'XYZ', '', '  ', null, undefined, 0, 1000, 8.5, true, 'England', 'California']) {
      expect(countryKey(v), String(v)).toBeNull();
    }
  });

  it('normalises names for comparing', () => {
    expect(norm("  The Côte d'Ivoire ")).toBe('cote divoire');
    expect(norm('Trinidad & Tobago')).toBe('trinidad and tobago');
    expect(norm('Myanmar (Burma)')).toBe('myanmar burma');
    expect(norm('Saint Kitts & Nevis')).toBe('st kitts and nevis');
  });
});

describe('the country shapes', () => {
  const shapes = countries();

  it('decode to every country of the file, each with its key and bounds', () => {
    expect(shapes.length).toBe(241);
    // Shapes without an ISO code stay grey.
    expect(
      shapes
        .filter((s) => s.key === null)
        .map((s) => s.name)
        .sort(),
    ).toEqual(['Indian Ocean Ter.', 'N. Cyprus', 'Siachen Glacier', 'Somaliland']);
    const keyed = shapes.filter((s) => s.key !== null);
    expect(keyed.length).toBe(237);
    // Ashmore and Cartier Islands share Australia's code; every other key is one shape's.
    expect(new Set(keyed.map((s) => s.key)).size).toBe(236);
    expect(keyed.filter((s) => s.key === '036').map((s) => s.name)).toEqual(['Australia', 'Ashmore and Cartier Is.']);
    // Every shape's own name, and every key, matches its shape.
    for (const s of keyed) {
      if (s.name !== 'Ashmore and Cartier Is.') expect(countryKey(s.name), s.name).toBe(s.key);
      expect(countryKey(s.key), s.name).toBe(s.key);
    }
    for (const name of ['Singapore', 'Hong Kong', 'Macao', 'Malta', 'Bahrain', 'Mauritius', 'Maldives']) {
      expect(shapes.some((s) => s.name === name && s.key === countryKey(name)), name).toBe(true);
    }
    const us = shapes.find((s) => s.key === '840')!;
    expect(us.name).toBe('United States of America');
    const [[w, south], [e, n]] = us.bounds;
    expect(w).toBeGreaterThan(-180);
    expect(e).toBeLessThan(-60);
    expect(south).toBeGreaterThan(15);
    expect(n).toBeLessThan(75);
    // Rings are closed, and none jumps across the map at the antimeridian
    // (but Antarctica's, which goes round the pole along its edge).
    for (const s of shapes) {
      for (const poly of s.polygons) {
        for (const r of poly) {
          expect(r[0], s.name).toEqual(r[r.length - 1]);
          if (s.key === '010') continue;
          for (let i = 1; i < r.length; i++) expect(Math.abs(r[i]![0]! - r[i - 1]![0]!), s.name).toBeLessThan(180);
        }
      }
    }
    // Countries across the antimeridian have bounds on their own side.
    const bounds = (key: string) => shapes.find((s) => s.key === key)!.bounds.flat().map(Math.round);
    expect(bounds('643')).toEqual([20, 41, 180, 82]); // Russia
    expect(bounds('242')).toEqual([175, -22, 180, -12]); // Fiji
    expect(bounds('554')).toEqual([166, -53, 179, -34]); // New Zealand
  });

  it('unwraps a ring across the antimeridian', () => {
    const r = [
      [170, 60],
      [180, 61],
      [-180, 61],
      [-170, 62],
      [-180, 65],
      [180, 65],
      [170, 60],
    ];
    expect(unwrap(r).map(([x]) => x)).toEqual([170, 180, 180, 190, 180, 180, 170]);
    // Round the pole (one jump): left as it is.
    const pole = [
      [-180, -90],
      [0, -70],
      [180, -90],
      [-180, -90],
    ];
    expect(unwrap(pole)).toBe(pole);
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

  it('steps values by quantiles, one step per value when there are few', () => {
    const m = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] });
    // Three countries, three values: a step each.
    expect(m.steps).toEqual([
      { from: 10, to: 10, count: 1 },
      { from: 120, to: 120, count: 1 },
      { from: 600, to: 600, count: 1 },
    ]);
    expect(step(m, 10)).toBe(0);
    expect(step(m, 120)).toBe(1);
    expect(step(m, 600)).toBe(2);
    expect(step(m, null)).toBeNull();
    // One value is one colour.
    const one = fm({ mark: 'map-filled', columns: ['country'], rows: [{ field: 'revenue', agg: 'sum' }] }, [
      ['FR', 'Paris', 5, 1],
      ['ES', 'Madrid', 5, 1],
    ]);
    expect(one.steps).toEqual([{ from: 5, to: 5, count: 2 }]);
    expect(step(one, 5)).toBe(0);
    expect(quantiles([])).toEqual([]);
  });

  it('steps the docs example one value each', () => {
    const rows = [
      ['DE', 'x', '120.00', 1],
      ['Germany', 'x', '80.00', 1],
      ['USA', 'x', '300.00', 1],
      ['United States', 'x', '50.00', 1],
      ['IN', 'x', '75.50', 1],
      ['Atlantis', 'x', '9.99', 1],
      ['SG', 'x', '42.00', 1],
    ];
    const m = fm({ mark: 'map-filled', columns: ['country'], rows: ['revenue'] }, rows);
    expect(m.steps.map((x) => x.from)).toEqual([42, 75.5, 200, 350]);
    expect(m.unmatched).toBe(1);
  });

  it('keeps a huge value from washing out the rest', () => {
    // Twelve countries, one far above the others: equal intervals would put
    // eleven of them in the first step.
    const vs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1_000_000];
    const q = quantiles(vs);
    expect(q.length).toBe(MAX_STEPS);
    expect(q.map((x) => x.count)).toEqual([2, 2, 2, 2, 2, 2]);
    expect(q[0]).toEqual({ from: 1, to: 2, count: 2 });
    expect(q[5]).toEqual({ from: 11, to: 1_000_000, count: 2 });
    // About as many countries in each step for a long tail too.
    const tail = Array.from({ length: 60 }, (_, i) => Math.round(1.2 ** i));
    expect(quantiles(tail).map((x) => x.count)).toEqual([10, 10, 10, 10, 10, 10]);
  });

  it('never splits equal values across steps', () => {
    // Twenty countries at 0 and ten others: the zeros are one step.
    const vs = [...Array<number>(20).fill(0), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const q = quantiles(vs);
    expect(q[0]).toEqual({ from: 0, to: 0, count: 20 });
    expect(q.reduce((a, x) => a + x.count, 0)).toBe(30);
    for (let i = 1; i < q.length; i++) expect(q[i]!.from).toBeGreaterThan(q[i - 1]!.to);
    expect(q.length).toBeLessThanOrEqual(MAX_STEPS);
    // Ties at a quantile move the break to the nearest change of value.
    const t = quantiles([1, 2, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7]);
    expect(t.filter((y) => y.from <= 3 && 3 <= y.to).length).toBe(1);
    expect(t.find((x) => x.from <= 3 && 3 <= x.to)!.count).toBeGreaterThanOrEqual(6);
    // Few distinct values: one step each, however many countries share them.
    expect(quantiles([7, 7, 7, 9, 9, 1]).map((x) => [x.from, x.count])).toEqual([
      [1, 1],
      [7, 3],
      [9, 2],
    ]);
  });

  it('spreads the steps over the ramp past its first shade', () => {
    expect([0, 1, 2, 3, 4, 5].map((k) => shade(k, 6, 7))).toEqual([1, 2, 3, 4, 5, 6]);
    expect([0, 1].map((k) => shade(k, 2, 7))).toEqual([1, 6]);
    expect([0, 1, 2].map((k) => shade(k, 3, 7))).toEqual([1, 4, 6]);
    expect(shade(0, 1, 7)).toBe(6);
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
