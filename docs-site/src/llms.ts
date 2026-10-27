// Builds /llms.txt (an index) and /llms-full.txt (every page body) from the
// docs collection, in sidebar order, so the two files cannot drift from the site.
import { getCollection, type CollectionEntry } from 'astro:content';
import config from 'virtual:starlight/user-config';

type Doc = CollectionEntry<'docs'>;
type Section = { label: string; docs: Doc[] };

const pageUrl = (id: string, site: URL) =>
  new URL(id === 'index' ? '/' : `/${id.replace(/\/index$/, '')}/`, site).href;

const siteTitle =
  typeof config.title === 'string' ? config.title : Object.values(config.title)[0];

// MDX bodies carry imports and JSX. Drop them, keeping the text inside
// components, so the file is plain markdown. Code spans and fences are left alone.
function clean(body: string): string {
  return body
    .split(/(^```[\s\S]*?^```|`[^`\n]+`)/m)
    .map((part, i) =>
      i % 2
        ? part
        : part
            .replace(/^import .+ from ['"].+['"];?\n/gm, '')
            .replace(/<(video|script|style)\b[\s\S]*?<\/\1>/g, '')
            .replace(/<[A-Za-z][^>]*\/>/g, '')
            .replace(/<\/?[A-Za-z][^>]*>/g, '')
            .replace(/\n{3,}/g, '\n\n'),
    )
    .join('')
    .trim();
}

const oneLine = (s: string) => s.replace(/\s+/g, ' ').trim();

// Walks the configured sidebar so sections and page order match the site.
// Pages not listed in it fall into a trailing "Other" section.
async function sections(): Promise<Section[]> {
  const docs = await getCollection('docs', ({ data }) => !data.draft);
  const byId = new Map(docs.map((d) => [d.id, d]));
  const used = new Set<string>();
  const out: Section[] = [];

  const collect = (items: any[], into: Doc[]) => {
    for (const item of items) {
      if (item.items) collect(item.items, into);
      else if (item.slug && byId.has(item.slug) && !used.has(item.slug)) {
        used.add(item.slug);
        into.push(byId.get(item.slug)!);
      }
    }
  };

  let top: Doc[] = [];
  for (const entry of (config.sidebar ?? []) as any[]) {
    if (entry.items) {
      const section: Section = { label: entry.label, docs: [] };
      collect(entry.items, section.docs);
      if (section.docs.length) out.push(section);
    } else {
      collect([entry], top);
    }
  }
  if (top.length) out.unshift({ label: 'Start', docs: top });

  const rest = docs.filter((d) => !used.has(d.id)).sort((a, b) => a.id.localeCompare(b.id));
  if (rest.length) out.push({ label: 'Other', docs: rest });
  return out;
}

export async function buildIndex(site: URL): Promise<string> {
  const lines = [
    `# ${siteTitle}`,
    '',
    `> ${config.description}`,
    '',
    `The full text of every page is at ${new URL('/llms-full.txt', site).href}.`,
  ];
  for (const section of await sections()) {
    lines.push('', `## ${section.label}`, '');
    for (const d of section.docs) {
      const desc = d.data.description ? `: ${oneLine(d.data.description)}` : '';
      lines.push(`- [${d.data.title}](${pageUrl(d.id, site)})${desc}`);
    }
  }
  return lines.join('\n') + '\n';
}

export async function buildFull(site: URL): Promise<string> {
  const parts = [`# ${siteTitle}`, '', `> ${config.description}`];
  for (const section of await sections()) {
    for (const d of section.docs) {
      parts.push('', `---`, '', `# ${d.data.title}`, '', `Source: ${pageUrl(d.id, site)}`, '', clean(d.body ?? ''));
    }
  }
  return parts.join('\n') + '\n';
}
