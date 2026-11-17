// Matching a filled map's country column to the country shapes: by name,
// or by ISO 3166-1 alpha-2, alpha-3 or numeric code. The shapes (Natural
// Earth's 1:110m countries, from world-atlas) carry the numeric code and a
// name; the table below adds each country's alpha-2 and alpha-3 codes, and
// ALIASES the other names a column commonly holds. Only countries with a
// shape are listed: a code of anything smaller matches nothing.

/** Numeric code, alpha-2, alpha-3 and the shapes' name, one per line. */
const TABLE = `
004 AF AFG Afghanistan
008 AL ALB Albania
010 AQ ATA Antarctica
012 DZ DZA Algeria
024 AO AGO Angola
031 AZ AZE Azerbaijan
032 AR ARG Argentina
036 AU AUS Australia
040 AT AUT Austria
044 BS BHS Bahamas
050 BD BGD Bangladesh
051 AM ARM Armenia
056 BE BEL Belgium
064 BT BTN Bhutan
068 BO BOL Bolivia
070 BA BIH Bosnia and Herz.
072 BW BWA Botswana
076 BR BRA Brazil
084 BZ BLZ Belize
090 SB SLB Solomon Is.
096 BN BRN Brunei
100 BG BGR Bulgaria
104 MM MMR Myanmar
108 BI BDI Burundi
112 BY BLR Belarus
116 KH KHM Cambodia
120 CM CMR Cameroon
124 CA CAN Canada
140 CF CAF Central African Rep.
144 LK LKA Sri Lanka
148 TD TCD Chad
152 CL CHL Chile
156 CN CHN China
158 TW TWN Taiwan
170 CO COL Colombia
178 CG COG Congo
180 CD COD Dem. Rep. Congo
188 CR CRI Costa Rica
191 HR HRV Croatia
192 CU CUB Cuba
196 CY CYP Cyprus
203 CZ CZE Czechia
204 BJ BEN Benin
208 DK DNK Denmark
214 DO DOM Dominican Rep.
218 EC ECU Ecuador
222 SV SLV El Salvador
226 GQ GNQ Eq. Guinea
231 ET ETH Ethiopia
232 ER ERI Eritrea
233 EE EST Estonia
238 FK FLK Falkland Is.
242 FJ FJI Fiji
246 FI FIN Finland
250 FR FRA France
260 TF ATF Fr. S. Antarctic Lands
262 DJ DJI Djibouti
266 GA GAB Gabon
268 GE GEO Georgia
270 GM GMB Gambia
275 PS PSE Palestine
276 DE DEU Germany
288 GH GHA Ghana
300 GR GRC Greece
304 GL GRL Greenland
320 GT GTM Guatemala
324 GN GIN Guinea
328 GY GUY Guyana
332 HT HTI Haiti
340 HN HND Honduras
348 HU HUN Hungary
352 IS ISL Iceland
356 IN IND India
360 ID IDN Indonesia
364 IR IRN Iran
368 IQ IRQ Iraq
372 IE IRL Ireland
376 IL ISR Israel
380 IT ITA Italy
384 CI CIV Côte d'Ivoire
388 JM JAM Jamaica
392 JP JPN Japan
398 KZ KAZ Kazakhstan
400 JO JOR Jordan
404 KE KEN Kenya
408 KP PRK North Korea
410 KR KOR South Korea
414 KW KWT Kuwait
417 KG KGZ Kyrgyzstan
418 LA LAO Laos
422 LB LBN Lebanon
426 LS LSO Lesotho
428 LV LVA Latvia
430 LR LBR Liberia
434 LY LBY Libya
440 LT LTU Lithuania
442 LU LUX Luxembourg
450 MG MDG Madagascar
454 MW MWI Malawi
458 MY MYS Malaysia
466 ML MLI Mali
478 MR MRT Mauritania
484 MX MEX Mexico
496 MN MNG Mongolia
498 MD MDA Moldova
499 ME MNE Montenegro
504 MA MAR Morocco
508 MZ MOZ Mozambique
512 OM OMN Oman
516 NA NAM Namibia
524 NP NPL Nepal
528 NL NLD Netherlands
540 NC NCL New Caledonia
548 VU VUT Vanuatu
554 NZ NZL New Zealand
558 NI NIC Nicaragua
562 NE NER Niger
566 NG NGA Nigeria
578 NO NOR Norway
586 PK PAK Pakistan
591 PA PAN Panama
598 PG PNG Papua New Guinea
600 PY PRY Paraguay
604 PE PER Peru
608 PH PHL Philippines
616 PL POL Poland
620 PT PRT Portugal
624 GW GNB Guinea-Bissau
626 TL TLS Timor-Leste
630 PR PRI Puerto Rico
634 QA QAT Qatar
642 RO ROU Romania
643 RU RUS Russia
646 RW RWA Rwanda
682 SA SAU Saudi Arabia
686 SN SEN Senegal
688 RS SRB Serbia
694 SL SLE Sierra Leone
703 SK SVK Slovakia
704 VN VNM Vietnam
705 SI SVN Slovenia
706 SO SOM Somalia
710 ZA ZAF South Africa
716 ZW ZWE Zimbabwe
724 ES ESP Spain
728 SS SSD S. Sudan
729 SD SDN Sudan
732 EH ESH W. Sahara
740 SR SUR Suriname
748 SZ SWZ eSwatini
752 SE SWE Sweden
756 CH CHE Switzerland
760 SY SYR Syria
762 TJ TJK Tajikistan
764 TH THA Thailand
768 TG TGO Togo
780 TT TTO Trinidad and Tobago
784 AE ARE United Arab Emirates
788 TN TUN Tunisia
792 TR TUR Turkey
795 TM TKM Turkmenistan
800 UG UGA Uganda
804 UA UKR Ukraine
807 MK MKD Macedonia
818 EG EGY Egypt
826 GB GBR United Kingdom
834 TZ TZA Tanzania
840 US USA United States of America
854 BF BFA Burkina Faso
858 UY URY Uruguay
860 UZ UZB Uzbekistan
862 VE VEN Venezuela
887 YE YEM Yemen
894 ZM ZMB Zambia
`;

/** Other names, by alpha-2 (matched after normalising, see `norm`). */
const ALIASES: Record<string, string[]> = {
  AE: ['UAE'],
  BA: ['Bosnia and Herzegovina'],
  BN: ['Brunei Darussalam'],
  BO: ['Bolivia, Plurinational State of'],
  CD: ['Democratic Republic of the Congo', 'Congo, Democratic Republic of the', 'DR Congo', 'DRC', 'Congo - Kinshasa', 'Congo (Kinshasa)'],
  CF: ['Central African Republic'],
  CG: ['Republic of the Congo', 'Congo, Republic of the', 'Congo - Brazzaville', 'Congo (Brazzaville)'],
  CI: ['Ivory Coast'],
  CZ: ['Czech Republic'],
  DO: ['Dominican Republic'],
  EH: ['Western Sahara'],
  FK: ['Falkland Islands', 'Falkland Islands (Malvinas)'],
  GB: ['UK', 'U.K.', 'Great Britain', 'Britain'],
  GQ: ['Equatorial Guinea'],
  IR: ['Iran, Islamic Republic of', 'Islamic Republic of Iran'],
  KP: ["Korea, Democratic People's Republic of", "Democratic People's Republic of Korea", 'DPRK'],
  KR: ['Korea, Republic of', 'Republic of Korea'],
  LA: ["Lao People's Democratic Republic", 'Lao PDR'],
  MD: ['Moldova, Republic of', 'Republic of Moldova'],
  MK: ['North Macedonia'],
  MM: ['Burma', 'Myanmar (Burma)'],
  PS: ['Palestine, State of', 'State of Palestine', 'Palestinian Territories'],
  RU: ['Russian Federation'],
  SB: ['Solomon Islands'],
  SS: ['South Sudan'],
  SY: ['Syrian Arab Republic'],
  SZ: ['Eswatini', 'Swaziland'],
  TF: ['French Southern Territories', 'French Southern and Antarctic Lands'],
  TL: ['East Timor'],
  TR: ['Türkiye'],
  TW: ['Taiwan, Province of China'],
  TZ: ['United Republic of Tanzania', 'Tanzania, United Republic of'],
  US: ['United States', 'U.S.', 'U.S.A.', 'America'],
  VE: ['Venezuela, Bolivarian Republic of'],
  VN: ['Viet Nam'],
};

/** Kosovo has a shape but no numeric code: its key is its user-assigned code. */
export const KOSOVO = 'XK';

/** A name as it is compared: lower case, without accents, `&` as "and",
 * without apostrophes and full stops, and other punctuation as spaces. */
export function norm(s: string): string {
  return s
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replaceAll('&', ' and ')
    .replace(/['’.]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/^the /, '');
}

interface Index {
  a2: Map<string, string>;
  a3: Map<string, string>;
  names: Map<string, string>;
  ids: Set<string>;
}

let index: Index | null = null;

function built(): Index {
  if (index) return index;
  const ix: Index = { a2: new Map(), a3: new Map(), names: new Map(), ids: new Set() };
  const byA2 = new Map<string, string>();
  for (const line of TABLE.trim().split('\n')) {
    const [id, a2, a3, ...name] = line.split(' ');
    ix.ids.add(id!);
    ix.a2.set(a2!, id!);
    ix.a3.set(a3!, id!);
    ix.names.set(norm(name.join(' ')), id!);
    byA2.set(a2!, id!);
  }
  ix.ids.add(KOSOVO);
  ix.a2.set('XK', KOSOVO);
  ix.a3.set('XKX', KOSOVO);
  ix.names.set('kosovo', KOSOVO);
  for (const [a2, names] of Object.entries(ALIASES)) {
    const id = byA2.get(a2);
    if (id) for (const n of names) ix.names.set(norm(n), id);
  }
  index = ix;
  return ix;
}

/**
 * The shape key of a country cell, or null when it names no country with
 * a shape: a numeric code (`840`, `"076"`), an alpha-2 (`US`) or alpha-3
 * (`USA`) code in any case, or a name (`United States of America`,
 * `United States`, `côte d'ivoire`). Keys are the numeric code as three
 * digits, and `XK` for Kosovo.
 */
export function countryKey(v: unknown): string | null {
  if (typeof v === 'number') return Number.isInteger(v) && v > 0 && v < 1000 ? hit(String(v).padStart(3, '0')) : null;
  if (typeof v !== 'string') return null;
  const t = v.trim();
  if (!t) return null;
  const ix = built();
  if (/^\d{1,3}$/.test(t)) return hit(t.padStart(3, '0'));
  const n = norm(t);
  // Two or three letters are a code when they are one (`U.S.` too); `UK`
  // and `UAE` aren't, and are found among the names.
  if (/^[a-z]{2,3}$/.test(n)) {
    const code = n.toUpperCase();
    const id = (n.length === 2 ? ix.a2 : ix.a3).get(code);
    if (id) return id;
  }
  return ix.names.get(n) ?? null;
}

function hit(id: string): string | null {
  return built().ids.has(id) ? id : null;
}

/** The shape key of a shape: its numeric code, `XK` for Kosovo, and
 * null for shapes without a code (Northern Cyprus, Somaliland). */
export function shapeKey(id: string | number | undefined, name: string): string | null {
  if (id !== undefined && id !== null && String(id).trim()) return String(id).padStart(3, '0');
  return norm(name) === 'kosovo' ? KOSOVO : null;
}
