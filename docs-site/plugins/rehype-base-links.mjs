// Starlight prefixes `base` onto its own chrome but not onto links written in
// Markdown/MDX. The docs use root-relative links (`/getting-started/...`), so a
// build under a sub-path such as /next/ needs them rewritten. External URLs,
// anchors and links already under the base are left alone.
const ATTRS = ['href', 'src', 'poster'];

export function rehypeBaseLinks({ base = '/' } = {}) {
  const prefix = base.replace(/\/+$/, '');
  if (!prefix) return () => {};

  const rewrite = (value) => {
    if (typeof value !== 'string') return value;
    if (!value.startsWith('/') || value.startsWith('//')) return value;
    if (value === prefix || value.startsWith(`${prefix}/`)) return value;
    return prefix + value;
  };

  const walk = (node) => {
    if (node.properties) {
      for (const attr of ATTRS) {
        if (attr in node.properties) node.properties[attr] = rewrite(node.properties[attr]);
      }
    }
    // JSX elements in .mdx carry their attributes as a list.
    if (Array.isArray(node.attributes)) {
      for (const a of node.attributes) {
        if (a.type === 'mdxJsxAttribute' && ATTRS.includes(a.name)) a.value = rewrite(a.value);
      }
    }
    if (Array.isArray(node.children)) node.children.forEach(walk);
  };

  return (tree) => walk(tree);
}
