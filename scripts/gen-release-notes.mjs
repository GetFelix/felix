#!/usr/bin/env node
// Renders CHANGELOG.md into the docs site's release notes: one page per
// released version plus an index, written to gitignored paths under
// docs-site/src/content/docs/release-notes/. Run by the docs-site dev and
// build scripts, so the site cannot drift from the changelog.

import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const notesDir = join(root, 'docs-site/src/content/docs/release-notes');
const releasesDir = join(notesDir, 'releases');
const repo = 'https://github.com/GetFelix/felix';
const docsPrefix = 'docs-site/src/content/docs/';

/** Splits the changelog into released sections, newest first as written. */
export function parseChangelog(text) {
  const releases = [];
  let current = null;
  let fence = false;
  for (const line of text.split('\n')) {
    if (/^```/.test(line)) fence = !fence;
    const head = !fence && line.match(/^## \[([^\]]+)\](?: - (\d{4}-\d{2}-\d{2}))?\s*$/);
    if (head) {
      current = head[1] === 'Unreleased' ? null : { version: head[1], date: head[2] ?? '', lines: [] };
      if (current) releases.push(current);
      continue;
    }
    // The reference-style link definitions at the foot of the file.
    if (!fence && /^\[[^\]]+\]: \S+$/.test(line)) {
      current = null;
      continue;
    }
    if (current) current.lines.push(line);
  }
  for (const r of releases) r.body = r.lines.join('\n').trim();
  return releases;
}

/** Site path for a repo-relative docs-site content path, or a GitHub URL. */
function rewriteTarget(target) {
  if (/^([a-z][a-z0-9+.-]*:|#|\/)/i.test(target)) return target;
  const clean = target.replace(/^\.\//, '');
  const [path, anchor = ''] = clean.split('#');
  const hash = anchor ? `#${anchor}` : '';
  if (path.startsWith(docsPrefix)) {
    const slug = path.slice(docsPrefix.length).replace(/\.mdx?$/, '').replace(/\/index$/, '');
    return `/${slug}/${hash}`;
  }
  return `${repo}/blob/main/${path}${hash}`;
}

function rewriteLine(line) {
  // Code spans, links and bare URLs pass through untouched, except link targets.
  const parts = line.split(/(`[^`]*`|\[[^\]]*\]\([^)\s]*\)|https?:\/\/[^\s)>]+)/);
  return parts
    .map((part, i) => {
      if (i % 2 === 0) {
        return part.replace(/(?<![\w&/#])#(\d{2,6})\b/g, `[#$1](${repo}/issues/$1)`);
      }
      const link = part.match(/^(\[[^\]]*\]\()([^)\s]*)\)$/);
      return link ? `${link[1]}${rewriteTarget(link[2])})` : part;
    })
    .join('');
}

/** Rewrites links and issue numbers, and lifts section headings one level. */
export function renderBody(body) {
  let fence = false;
  return body
    .split('\n')
    .map((line) => {
      if (/^\s*```/.test(line)) {
        fence = !fence;
        return line;
      }
      if (fence) return line;
      const heading = line.match(/^(#{3,6}) (.*)$/);
      const text = heading ? `${heading[1].slice(1)} ${heading[2]}` : line;
      return rewriteLine(text);
    })
    .join('\n');
}

export function slugFor(version) {
  return `v${version.replace(/\./g, '-')}`;
}

// Leading sentences of the summary paragraph, first alone, or the first two when the first is short.
function firstSentence(body) {
  const para = body.split(/\n\s*\n/)[0];
  if (!para || /^#/.test(para)) return '';
  const sentences = para.replace(/\s+/g, ' ').match(/.+?(?:\.(?=\s|$)|$)/g) ?? [];
  const first = (sentences[0] ?? '').trim();
  const both = sentences.slice(0, 2).map((s) => s.trim()).join(' ');
  return first.length < 45 && both.length <= 220 ? both : first;
}

function page(frontmatter, body) {
  const fm = Object.entries(frontmatter)
    .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
    .join('\n');
  return `---\n${fm}\n---\n\n${body}\n`;
}

function main() {
  const releases = parseChangelog(readFileSync(join(root, 'CHANGELOG.md'), 'utf8'));
  rmSync(releasesDir, { recursive: true, force: true });
  mkdirSync(releasesDir, { recursive: true });

  releases.forEach((r, i) => {
    const preview = r.version.includes('-');
    const intro = `Released ${r.date}${preview ? ' as a preview' : ''}. The full history is in [CHANGELOG.md](${repo}/blob/main/CHANGELOG.md).`;
    const body = `${intro}\n\n${renderBody(r.body)}`;
    writeFileSync(
      join(releasesDir, `${slugFor(r.version)}.md`),
      page(
        {
          title: `Felix ${r.version}`,
          description: `Release notes for Felix ${r.version}, ${r.date}.`,
          editUrl: `${repo}/edit/main/CHANGELOG.md`,
          lastUpdated: false,
          sidebar: { order: i + 1, label: r.version },
        },
        body,
      ),
    );
  });

  const rows = releases
    .map((r) => {
      const summary = firstSentence(renderBody(r.body));
      return `| [${r.version}](/release-notes/releases/${slugFor(r.version)}/) | ${r.date} | ${summary.replace(/\|/g, '\\|')} |`;
    })
    .join('\n');
  const index = `Every released version, newest first. Each page is generated from [CHANGELOG.md](${repo}/blob/main/CHANGELOG.md) when the site is built, so it matches the changelog exactly.

If you are moving a running cluster to a new release, read [Upgrades and compatibility](/deployment/upgrades/) first. It covers upgrade order, fleet features and rollback. For the release in development, see [Upgrading to 0.7](/release-notes/upgrading-to-0-7/).

| Version | Date | Summary |
|---|---|---|
${rows}
`;
  writeFileSync(
    join(notesDir, 'index.md'),
    page(
      {
        title: 'Release notes',
        description: 'What changed in each Felix release, generated from the changelog.',
        editUrl: `${repo}/edit/main/CHANGELOG.md`,
        lastUpdated: false,
        sidebar: { order: 0, label: 'Overview' },
      },
      index,
    ),
  );
  console.log(`release notes: wrote ${releases.length} pages`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
