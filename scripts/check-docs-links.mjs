#!/usr/bin/env node
// Link check over the combined docs output (root = stable, next/ = main).
// A link on a page under /next/ must resolve inside /next/, and a link on a
// root page must resolve outside it. Anchors are not checked.
//
//   node scripts/check-docs-links.mjs <dist-dir>
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, posix, relative } from 'node:path';

const dist = process.argv[2];
if (!dist) {
  console.error('usage: check-docs-links.mjs <dist-dir>');
  process.exit(2);
}

const htmlFiles = [];
(function walk(dir) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p);
    else if (name.endsWith('.html')) htmlFiles.push(p);
  }
})(dist);

const exists = (urlPath) => {
  const p = join(dist, decodeURIComponent(urlPath));
  if (existsSync(p) && statSync(p).isFile()) return true;
  if (existsSync(join(p, 'index.html'))) return true;
  return !posix.extname(urlPath) && existsSync(`${p}.html`);
};

const TAG = /<(a|img|source|video|audio|script|link|iframe)\b[^>]*>/gi;
const ATTR = /\b(href|src|poster)\s*=\s*(?:"([^"]*)"|'([^']*)')/gi;
const REFRESH = /<meta[^>]*http-equiv=["']refresh["'][^>]*content=["'][^"']*url=([^"']+)["']/gi;
const SKIP = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i;

const problems = [];
let checked = 0;

for (const file of htmlFiles) {
  const pageUrl = '/' + relative(dist, file).split('\\').join('/');
  const inNext = pageUrl.startsWith('/next/');
  const html = readFileSync(file, 'utf8');
  const links = [];
  for (const [tag] of html.matchAll(TAG)) {
    if (/rel=["']?(?:sitemap|canonical)/i.test(tag)) continue;
    // The /next/ banner deliberately points at the stable root.
    const toRoot = tag.includes('data-docs-root');
    for (const m of tag.matchAll(ATTR)) {
      const href = m[2] ?? m[3];
      if (toRoot) {
        checked++;
        if (!exists(href)) problems.push(`${pageUrl}: ${href} does not resolve`);
      } else links.push(href);
    }
  }
  for (const m of html.matchAll(REFRESH)) links.push(m[1]);

  for (const raw of links) {
    const href = raw.replace(/&amp;/g, '&').trim();
    if (!href || SKIP.test(href)) continue;
    const [pathPart] = href.split(/[?#]/);
    if (!pathPart) continue;
    const target = pathPart.startsWith('/')
      ? posix.normalize(pathPart)
      : posix.join(dirname(pageUrl), pathPart);
    // pages.yml adds the benchmark dashboards to both trees after this build.
    if (/^(?:\/next)?\/benchmarks\//.test(target)) continue;
    checked++;
    const targetInNext = target === '/next' || target.startsWith('/next/');
    if (inNext !== targetInNext) {
      problems.push(`${pageUrl}: ${href} crosses the /next/ boundary`);
    } else if (!exists(target)) {
      problems.push(`${pageUrl}: ${href} does not resolve`);
    }
  }
}

console.log(`checked ${checked} internal links in ${htmlFiles.length} pages`);
if (problems.length) {
  const unique = [...new Set(problems)];
  console.error(`${unique.length} broken links:`);
  for (const p of unique.slice(0, 200)) console.error(`  ${p}`);
  if (unique.length > 200) console.error(`  ... and ${unique.length - 200} more`);
  process.exit(1);
}
