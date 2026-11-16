// Links to the product documentation's published pages: `docs/<page>.md`
// is `<site>/docs/<page>/` on the website (site/scripts/sync-docs.mjs).

export const DOCS_SITE = 'https://modlix-india.github.io/sprigdb/docs/';

/** The heading's anchor as Starlight writes it. */
export function slug(heading: string): string {
  return heading
    .toLowerCase()
    .replace(/[`*]/g, '')
    .replace(/[^\p{L}\p{N}\s_-]/gu, '')
    .trim()
    .replace(/\s+/g, '-');
}

/** The published page of a docs path (`tools/studio.md`), at a heading. */
export function docsUrl(page: string, heading?: string): string {
  let p = page.replace(/^\/+/, '').replace(/\.md$/i, '');
  if (/(^|\/)README$/i.test(p)) p = p.replace(/README$/i, '');
  p = p.toLowerCase();
  const anchor = heading ? `#${slug(heading)}` : '';
  return `${DOCS_SITE}${p}${p && !p.endsWith('/') ? '/' : ''}${anchor}`;
}
