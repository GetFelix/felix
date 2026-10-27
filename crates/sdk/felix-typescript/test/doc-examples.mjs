// Writes every ```ts block of the TypeScript client page and the resilience
// guide to test/doc-examples/ as its own module, so `tsc -p tsconfig.json` checks the page against
// index.d.ts. Names a block uses without defining are declared in
// doc-globals.d.ts. CI runs this before tsc; the output is not checked in.

import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const clients = join(here, "../../../../docs-site/src/content/docs/clients");
// Each page, and the prefix its generated modules are named with.
const PAGES = [
  { file: "typescript.md", prefix: "typescript-md", inject: true },
  { file: "resilient-clients.mdx", prefix: "resilient-clients-mdx", inject: false },
];
const out = join(here, "doc-examples");

// Everything index.d.ts exports, so a block can use any of it without the
// import lines the page leaves out.
const VALUES = [
  "AuthError", "Client", "CommitError", "CommitOp", "ConnectionError", "CursorError",
  "EventCountError", "FelixError", "InvalidArgumentError", "NotFoundError",
  "NotOnOwningShardError", "OutcomeUnknownError", "OverloadedError",
  "ShardUnavailableError", "SubscriptionHandle", "ShardedSubscriptionHandle",
  "CacheWatchHandle",
];
const TYPES = [
  "AckMode", "CacheChange", "CacheWatchItem", "CommitReceipt", "ConnectOptions",
  "Event", "GroupDelivery", "GroupRecord", "IdentityOptions", "ShardEvent",
  "ShardMoved", "ShardPositions", "StartPosition", "StateValue", "TokenProvider",
];

export function extract(markdown, name = "typescript.md") {
  const blocks = [];
  const lines = markdown.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const open = /^(\s*)```(ts|typescript)\s*$/.exec(lines[i]);
    if (!open) continue;
    const start = i + 1;
    const body = [];
    for (i++; i < lines.length && !/^\s*```\s*$/.test(lines[i]); i++) body.push(lines[i]);
    if (i === lines.length) throw new Error(`${name}:${start}: unclosed code block`);
    blocks.push({ line: start, code: body.join("\n") });
  }
  return blocks;
}

// With `inject` false a block gets nothing it does not import itself, so a name
// the page forgot to import fails the type check.
export function toModule({ line, code }, file = "typescript.md", inject = true) {
  const imported = new Set();
  const source = code.replace(
    /import\s*\{([^}]*)\}\s*from\s*"felix-client";?/g,
    (_, names) => {
      for (const name of names.split(",")) {
        const bare = name.trim().replace(/^type\s+/, "");
        if (bare) imported.add(bare);
      }
      return `import {${names}} from "../../index";`;
    },
  );
  const wanted = inject
    ? [
        ...VALUES.filter((n) => !imported.has(n)),
        ...TYPES.filter((n) => !imported.has(n)).map((n) => `type ${n}`),
      ]
    : [];
  return [
    `// Generated from docs-site/src/content/docs/clients/${file}:${line}.`,
    wanted.length > 0 ? `import { ${wanted.join(", ")} } from "../../index";` : "",
    source,
    "export {};",
    "",
  ].join("\n");
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out);
  for (const { file, prefix, inject } of PAGES) {
    const blocks = extract(readFileSync(join(clients, file), "utf8"), file);
    if (blocks.length === 0) throw new Error(`${file} has no ts blocks; has the page moved?`);
    for (const block of blocks) {
      const name = `${prefix}-L${String(block.line).padStart(4, "0")}.ts`;
      writeFileSync(join(out, name), toModule(block, file, inject));
    }
    console.log(`wrote ${blocks.length} examples from ${file} to ${out}`);
  }
}
