// Starlight prefixes `base` onto its own chrome but not onto links written in
// Markdown/MDX or frontmatter. The docs use root-relative links
// (`/getting-started/...`), so a build under a sub-path such as /next/ needs
// them rewritten. Astro 7's default Markdown processor takes no rehype
// plugins without an extra dependency, so this runs over the built HTML.
// External URLs and anchors are left alone, and so is anything already under
// the base.
import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const TAG = /<(?:a|img|source|video|audio|iframe|track)\b[^>]*>/gi;
const ATTR = /(\s(?:href|src|poster)=)(["'])(\/(?!\/)[^"']*)\2/gi;

export function prefixLinks(html, base) {
  const prefix = base.replace(/\/+$/, '');
  return html.replace(TAG, (tag) =>
    tag.replace(ATTR, (all, name, quote, value) =>
      value === prefix || value.startsWith(`${prefix}/`)
        ? all
        : `${name}${quote}${prefix}${value}${quote}`,
    ),
  );
}

async function* htmlFiles(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* htmlFiles(path);
    else if (entry.name.endsWith('.html')) yield path;
  }
}

export function prefixBaseLinks({ base = '/' } = {}) {
  return {
    name: 'prefix-base-links',
    hooks: {
      'astro:build:done': async ({ dir }) => {
        if (base.replace(/\/+$/, '') === '') return;
        for await (const file of htmlFiles(fileURLToPath(dir))) {
          const html = await readFile(file, 'utf8');
          const out = prefixLinks(html, base);
          if (out !== html) await writeFile(file, out);
        }
      },
    },
  };
}
