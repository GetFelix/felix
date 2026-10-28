// Writes every ```ts block of the TypeScript client page and the resilience
// guide to test/doc-examples/ as its own module, so `tsc -p tsconfig.json` checks the page against
// index.d.ts. Each tutorial page is one module instead, because its blocks are
// steps of one program. Names a block uses without defining are declared in
// doc-globals.d.ts. CI runs this before tsc; the output is not checked in.

import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const page = join(here, "../../../../docs-site/src/content/docs/clients/typescript.md");
const clients = join(here, "../../../../docs-site/src/content/docs/clients");
const resilient = join(clients, "resilient-clients.mdx");
const out = join(here, "doc-examples");
const tutorials = ["first-stream", "work-queue", "cache-leases-watches", "build-an-app"];
const tutorialPage = (slug) =>
  join(here, "../../../../docs-site/src/content/docs/tutorials", `${slug}.mdx`);

// Everything index.d.ts exports, so a block can use any of it without the
// import lines the page leaves out.
const VALUES = [
  "AuthError", "CacheCondition", "Client", "CommitError", "CommitOp", "ConnectionError", "CursorError",
  "EventCountError", "FelixError", "InvalidArgumentError", "NotFoundError",
  "NotOnOwningShardError", "OutcomeUnknownError", "OverloadedError",
  "ShardUnavailableError", "SubscriptionHandle", "ShardedSubscriptionHandle",
  "CacheWatchHandle",
];
const TYPES = [
  "AckMode", "CacheChange", "CacheConditionResult", "CacheWatchItem", "CommitReceipt",
  "ConditionalWrite", "ConnectOptions",
  "Event", "GroupDelivery", "GroupRecord", "IdentityOptions", "ShardEvent",
  "ShardMoved", "ShardPositions", "StartPosition", "StateValue", "TokenProvider", "VersionedValue",
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
export function toModule({ line, code }, file = "clients/typescript.md", inject = true) {
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
    `// Generated from docs-site/src/content/docs/${file}:${line}.`,
    wanted.length > 0 ? `import { ${wanted.join(", ")} } from "../../index";` : "",
    source,
    "export {};",
    "",
  ].join("\n");
}

// The page's blocks as one program, with the `felix-client` imports joined into
// one statement so a name two steps import is declared once.
export function joinBlocks(blocks) {
  const imports = [];
  const pattern = /import\s*\{([^}]*)\}\s*from\s*"felix-client";?\n?/g;
  const body = blocks
    .map(({ code }) =>
      code.replace(pattern, (_, names) => {
        for (const name of names.split(",")) {
          if (name.trim() && !imports.includes(name.trim())) imports.push(name.trim());
        }
        return "";
      }),
    )
    .join("\n");
  const head = imports.length ? `import { ${imports.join(", ")} } from "felix-client";\n` : "";
  return { line: blocks[0].line, code: head + body };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const blocks = extract(readFileSync(page, "utf8"));
  if (blocks.length === 0) throw new Error("typescript.md has no ts blocks; has the page moved?");
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out);
  for (const block of blocks) {
    const name = `typescript-md-L${String(block.line).padStart(4, "0")}.ts`;
    writeFileSync(join(out, name), toModule(block));
  }
  const guide = extract(readFileSync(resilient, "utf8"), "resilient-clients.mdx");
  if (guide.length === 0) throw new Error("resilient-clients.mdx has no ts blocks; has the page moved?");
  for (const block of guide) {
    const name = `resilient-clients-mdx-L${String(block.line).padStart(4, "0")}.ts`;
    writeFileSync(join(out, name), toModule(block, "clients/resilient-clients.mdx", false));
  }
  for (const slug of tutorials) {
    const steps = extract(readFileSync(tutorialPage(slug), "utf8"));
    if (steps.length === 0) throw new Error(`${slug}.mdx has no ts blocks`);
    writeFileSync(join(out, `tutorial-${slug}.ts`), toModule(joinBlocks(steps)));
  }
  console.log(
    `wrote ${blocks.length} examples, ${guide.length} guide examples and ${tutorials.length} tutorials to ${out}`,
  );
}
