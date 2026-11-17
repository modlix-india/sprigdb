// Links to the product documentation. Studio opens a page itself, rebuilt
// from the release's docs pack (GET /v1/docs/pages/{page}, the Docs
// panel); the published website is the second choice ("open on the
// website"): `docs/<page>.md` is `<site>/docs/<page>/` there
// (site/scripts/sync-docs.mjs).

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

/** A docs page's path as the pack names it: `tools/studio.md` (`.md`
 * added; a directory, or nothing, is its `README.md`). */
export function pagePath(page: string): string {
  let p = page.replace(/^\/+/, '');
  if (!p || p.endsWith('/')) p += 'README.md';
  else if (!/\.md$/i.test(p)) p += '.md';
  return p.replace(/\.md$/i, '.md');
}

/** Studio's own page of a docs path, under the tenant's `base`
 * (`/t/acme`), at an anchor (a heading's slug). */
export function docsRoute(base: string, page: string, anchor?: string): string {
  return `${base}/docs/p/${pagePath(page)}${anchor ? `#${anchor}` : ''}`;
}

/** Where a link found in docs page `from` leads inside the docs: a relative
 * page (`../connect/rest-and-grpc.md#tokens`), an anchor on the same page
 * (`#tokens`), or a page of the published website. Null for anything else:
 * other sites, other schemes, and files that aren't pages. */
export function docsTarget(from: string, href: string): { page: string; anchor: string } | null {
  if (href.startsWith(DOCS_SITE)) {
    const [path = '', anchor = ''] = href.slice(DOCS_SITE.length).split('#');
    const p = path.replace(/\/+$/, '');
    if (!/^[\w./-]*$/.test(p) || p.split('/').includes('..')) return null;
    return { page: p ? `${p}.md` : 'README.md', anchor };
  }
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//')) return null;
  const [path = '', anchor = ''] = href.split('#');
  if (!path) return { page: pagePath(from), anchor };
  if (!/\.md$/i.test(path) && !path.endsWith('/')) return null;
  const parts = path.startsWith('/') ? [] : pagePath(from).split('/').slice(0, -1);
  for (const seg of path.split('/')) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') {
      if (!parts.length) return null;
      parts.pop();
    } else parts.push(seg);
  }
  return { page: pagePath(parts.join('/') + (path.endsWith('/') ? '/' : '')), anchor };
}
