//! Keeps the Rust examples on the docs site compiling.
//!
//! This reads `docs-site/src/content/docs/clients/rust.md` and
//! `resilient-clients.mdx`, wraps every ```` ```rust ```` block in the harness
//! listed for its page in [`PAGES`], and checks the result against
//! `docs_examples/rust_md.rs` or `resilient_clients_mdx.rs`, which the
//! `docs_examples` test target compiles. An edit to the page that is not regenerated fails here; a
//! regenerated example that does not build fails there. To regenerate:
//!
//! ```text
//! FELIX_BLESS_DOCS=1 cargo test -p felix-client --test docs_examples_sync
//! ```
//!
//! A page without the docs site next to it (the packaged crate) has nothing
//! to check.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// How one example is made into code that compiles.
enum Wrap {
    /// A whole file: its own imports and items, `main` included. `context`
    /// is added next to it.
    File { context: &'static str },
    /// Statements, run in `async fn example(params) -> anyhow::Result<()>`
    /// with the common imports and `context` in scope. Most examples are
    /// this: they continue from a client set up earlier on the page.
    Body {
        params: &'static str,
        context: &'static str,
    },
}

use Wrap::{Body, File};

/// The `nth` ```` ```rust ```` block under the heading `heading`.
struct Example {
    heading: &'static str,
    nth: usize,
    wrap: Wrap,
}

const fn example(heading: &'static str, nth: usize, wrap: Wrap) -> Example {
    Example { heading, nth, wrap }
}

const CLUSTER: &str = "client: &Arc<ClusterClient>";

/// One entry per block on the page, in page order. A block without an entry,
/// or an entry without a block, fails the test.
const RUST_MD_EXAMPLES: &[Example] = &[
    example("Basic Publish/Subscribe", 1, File { context: "" }),
    example(
        "Basic Cache Operations",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example("Getting and refreshing a token", 1, File { context: "" }),
    example(
        "ClientConfig",
        1,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "Acting for many users",
        1,
        Body {
            params: "addr: SocketAddr, config: ClientConfig, alice_token: String, bob_token: String",
            context: "async fn refresh_alice() -> Result<String> { unimplemented!() }",
        },
    ),
    example(
        "Acting for many users",
        2,
        Body {
            params: "seeds: Vec<SocketAddr>, config: ClientConfig, alice_tokens: Arc<dyn TokenProvider>",
            context: "",
        },
    ),
    example(
        "TLS and ALPN",
        1,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "TLS and ALPN",
        2,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "Configuration Tuning",
        1,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "Configuration Tuning",
        2,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "Single Message Publish",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example(
        "The routing key decides the shard",
        1,
        Body {
            params: "cluster: &ClusterClient, payload: Vec<u8>, customer_id: String",
            context: "",
        },
    ),
    example(
        "Naming the shard",
        1,
        Body {
            params: "cluster: &ClusterClient, tick: Vec<u8>",
            context: "",
        },
    ),
    example(
        "At-least-once may duplicate",
        1,
        Body {
            params: "cluster: &ClusterClient, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Batch Publishing",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example(
        "Batches through a ClusterClient",
        1,
        Body {
            params: "cluster: &ClusterClient, key: Bytes, payloads: Vec<Vec<u8>>, \
                     records: Vec<(Bytes, Vec<u8>)>",
            context: "",
        },
    ),
    example(
        "Publisher API",
        1,
        Body {
            params: "client: &Client",
            context: "",
        },
    ),
    example(
        "Publisher Sharding",
        1,
        Body {
            params: "config: &mut ClientConfig",
            context: "",
        },
    ),
    example(
        "Errors you can act on",
        1,
        Body {
            params: "cluster: &ClusterClient, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Errors you can act on",
        2,
        Body {
            params: "err: anyhow::Error, mut start: StartPosition",
            context: "",
        },
    ),
    example(
        "Idempotent producers",
        1,
        Body {
            params: "cluster: ClusterClient, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Idempotent producers",
        2,
        Body {
            params: "producer: &IdempotentProducer, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Idempotent producers",
        3,
        Body {
            params: "producer: &IdempotentProducer, orders: &[Order]",
            context: "struct Order;\n\
                      impl Order { fn encode(&self) -> Vec<u8> { Vec::new() } }",
        },
    ),
    example(
        "Idempotent producers",
        4,
        Body {
            params: "cluster: &Arc<ClusterClient>, mut producer: IdempotentProducer, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Creating Subscriptions",
        1,
        Body {
            params: CLUSTER,
            context: "async fn process_event(_: Event) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Event Structure",
        1,
        File {
            // Fails to build when the page's `Event` and the real one differ in
            // a field's name or type, or when either has a field the other lacks.
            context: "fn mirrors(real: felix_client::Event) -> Event {\n\
                      let felix_client::Event { tenant_id, namespace, stream, payload, offset, \
                      skipped_before, publisher, timestamp_micros } = real;\n\
                      Event { tenant_id, namespace, stream, payload, offset, skipped_before, \
                      publisher, timestamp_micros }\n}",
        },
    ),
    example(
        "Who published an event",
        1,
        Body {
            params: "seeds: Vec<SocketAddr>",
            context: "fn apply_input(_: &str, _: &Bytes) {}",
        },
    ),
    example(
        "Sizing the broker's queue for a subscription",
        1,
        Body {
            params: "addr: SocketAddr, mut config: ClientConfig",
            context: "",
        },
    ),
    example(
        "When an event was written, and replaying from a time",
        1,
        Body {
            params: "client: &Arc<ClusterClient>, since_micros: u64",
            context: "",
        },
    ),
    example(
        "Reading a range and stopping",
        1,
        Body {
            params: "client: &ClusterClient, start: u64, end: u64",
            context: "fn apply(_: u64, _: &Bytes) {}",
        },
    ),
    example(
        "Offsets are how you notice a drop",
        1,
        Body {
            params: "mut subscription: ClusterSubscription",
            context: "fn handle(_: &Bytes) {}",
        },
    ),
    example(
        "A consumer that survives a restart",
        1,
        Body {
            params: "cluster: &Arc<ClusterClient>, checkpoint: Checkpoint",
            context: "struct Checkpoint;\n\
                      impl Checkpoint {\n\
                      async fn load(&self) -> Result<Option<u64>> { Ok(None) }\n\
                      async fn save(&self, _: u64) -> Result<()> { Ok(()) }\n}\n\
                      async fn handle(_: &Bytes) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Where a subscription joined",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example(
        "When a shard moves",
        1,
        Body {
            params: "mut subscription: Subscription",
            context: "fn handle(_: &Event) {}",
        },
    ),
    example(
        "When a subscription falls behind",
        1,
        Body {
            params: "client: &Client, mut subscription: Subscription",
            context: "fn handle(_: &Event) {}",
        },
    ),
    example(
        "Multiple Subscriptions",
        1,
        Body {
            params: CLUSTER,
            context: "async fn handle_order(_: Event) -> Result<()> { Ok(()) }\n\
                      async fn handle_inventory(_: Event) -> Result<()> { Ok(()) }\n\
                      async fn handle_log(_: Event) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Async Event Processing",
        1,
        Body {
            params: "mut subscription: ClusterSubscription",
            context: "async fn expensive_processing(_: Event) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Subscription Lifecycle",
        1,
        Body {
            params: CLUSTER,
            context: "fn process(_: Event) {}",
        },
    ),
    example(
        "Put and Get",
        1,
        Body {
            params: "client: &ClusterClient, session_data: Bytes",
            context: "#[derive(serde::Deserialize)] struct Session {}",
        },
    ),
    example(
        "Without TTL",
        1,
        Body {
            params: "client: &ClusterClient, config_data: Bytes",
            context: "",
        },
    ),
    example(
        "Delete",
        1,
        Body {
            params: CLUSTER,
            context: "fn audit_log(_: &str, _: Bytes) {}",
        },
    ),
    example(
        "Conditional writes",
        1,
        Body {
            params: CLUSTER,
            context: "fn edit(value: &Bytes) -> Bytes { value.clone() }",
        },
    ),
    example(
        "Watch",
        1,
        Body {
            params: "client: &Client",
            context: "fn apply_update(_: &str, _: &Bytes) {}\nfn remove(_: &str) {}",
        },
    ),
    example(
        "Retained Watch",
        1,
        Body {
            params: "client: &Client",
            context: "",
        },
    ),
    example(
        "Counters",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example(
        "Concurrent Cache Operations",
        1,
        Body {
            params: "client: Arc<ClusterClient>",
            context: "fn process(_: Bytes) {}",
        },
    ),
    example(
        "Cache Namespacing",
        1,
        Body {
            params: "client: &ClusterClient, data1: Bytes, data2: Bytes, data3: Bytes, ttl: Option<u64>",
            context: "",
        },
    ),
    example(
        "Consumer Groups",
        1,
        Body {
            params: CLUSTER,
            context: "async fn handle(_: &Bytes, _: u32) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Dead letters",
        1,
        Body {
            params: CLUSTER,
            context: "fn worth_retrying(_: u64) -> bool { true }",
        },
    ),
    example(
        "Creating, moving and deleting a group",
        1,
        Body {
            params: CLUSTER,
            context: "",
        },
    ),
    example(
        "Creating, moving and deleting a group",
        2,
        Body {
            params: CLUSTER,
            context: "enum Job { Done, Running, Failed, Poison }\n\
                      fn check(_: &GroupRecord) -> Job { Job::Done }",
        },
    ),
    example(
        "Atomic commits",
        1,
        Body {
            params: "cluster: &ClusterClient",
            context: "",
        },
    ),
    example(
        "Atomic commits",
        2,
        Body {
            params: "cluster: &ClusterClient, ticks: Vec<Vec<u8>>",
            context: "",
        },
    ),
    example(
        "Atomic commits",
        3,
        Body {
            params: "cluster: &ClusterClient, district: u32, tick: Bytes, state: Bytes, next: u64",
            context: "",
        },
    ),
    example(
        "Clusters",
        1,
        Body {
            params: "seeds: Vec<SocketAddr>, config: ClientConfig",
            context: "fn handle(_: u32, _: Event) {}",
        },
    ),
    example(
        "Connection Management",
        1,
        Body {
            params: "seeds: Vec<SocketAddr>, config: ClientConfig",
            context: "",
        },
    ),
    example(
        "Connection Management",
        2,
        Body {
            params: "config: ClientConfig",
            context: "",
        },
    ),
    example(
        "Reconnecting",
        1,
        Body {
            params: "seeds: Vec<SocketAddr>, config: ClientConfig",
            context: "",
        },
    ),
    example(
        "Collecting Metrics",
        1,
        Body {
            params: "",
            context: "",
        },
    ),
    example(
        "Timing Measurements",
        1,
        Body {
            params: "",
            context: "",
        },
    ),
    example("Connection Pooling", 1, File { context: "" }),
    example("Error Recovery", 1, File { context: "" }),
    example(
        "Resource Cleanup",
        1,
        Body {
            params: "client: &ClusterClient, subscription: ClusterSubscription",
            context: "",
        },
    ),
    example("Batching for Throughput", 1, File { context: "" }),
    example("Integration Tests", 1, File { context: "" }),
];

/// The blocks of the resilience guide. Its examples continue from a client or
/// a config the reader already has, so most are `Body`.
const RESILIENT_EXAMPLES: &[Example] = &[
    example(
        "Connecting to a cluster",
        1,
        Body {
            params: "config: ClientConfig",
            context: "",
        },
    ),
    example(
        "Reconnection",
        1,
        Body {
            params: "seeds: Vec<Seed>, config: ClientConfig",
            context: "",
        },
    ),
    example(
        "Authentication that lasts",
        1,
        Body {
            params: "seeds: Vec<Seed>, mut config: ClientConfig, policy: ReconnectPolicy",
            context: "",
        },
    ),
    example(
        "When a broker rejects the token",
        1,
        Body {
            params: "client: &ClusterClient, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Errors and retry classes",
        1,
        Body {
            params: "cluster: &ClusterClient, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Writes whose outcome is unknown",
        1,
        Body {
            params: "cluster: &Arc<ClusterClient>, payload: Vec<u8>",
            context: "",
        },
    ),
    example(
        "Conditional publish",
        1,
        Body {
            params: "cluster: &ClusterClient, ticks: Vec<Vec<u8>>",
            context: "",
        },
    ),
    example(
        "Resume one shard",
        1,
        Body {
            params: "cluster: &Arc<ClusterClient>, checkpoint: Checkpoint",
            context: "struct Checkpoint;\n\
                      impl Checkpoint {\n\
                      async fn load(&self) -> Result<Option<u64>> { Ok(None) }\n\
                      async fn save(&self, _: u64) -> Result<()> { Ok(()) }\n}\n\
                      async fn handle(_: &Bytes) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Resume a sharded stream",
        1,
        Body {
            params: "cluster: &Arc<ClusterClient>, checkpoint: Positions",
            context: "struct Positions;\n\
                      impl Positions {\n\
                      async fn load(&self) -> Result<Option<ShardOffsets>> { Ok(None) }\n\
                      async fn save(&self, _: ShardOffsets) -> Result<()> { Ok(()) }\n}\n\
                      async fn handle(_: u32, _: &Event) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Slow consumers",
        1,
        Body {
            params: "seeds: Vec<Seed>, mut config: ClientConfig",
            context: "async fn slow_work(_: Event) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Consumer groups under failure",
        1,
        Body {
            params: "client: &ClusterClient, stop: &std::sync::atomic::AtomicBool",
            context: "async fn charge(_: &[u8], _: u32) -> Result<()> { Ok(()) }",
        },
    ),
    example(
        "Shutting down cleanly",
        1,
        Body {
            params: "client: &ClusterClient, subscription: ClusterSubscription, \
                     producer: IdempotentProducer",
            context: "",
        },
    ),
];

/// A docs page whose Rust blocks are compiled, and the file they are
/// generated into.
struct Page {
    /// The page's file name under `docs-site/src/content/docs/clients/`.
    file: &'static str,
    /// The generated file under `tests/docs_examples/`.
    generated: &'static str,
    examples: &'static [Example],
}

const PAGES: &[Page] = &[
    Page {
        file: "rust.md",
        generated: "rust_md.rs",
        examples: RUST_MD_EXAMPLES,
    },
    Page {
        file: "resilient-clients.mdx",
        generated: "resilient_clients_mdx.rs",
        examples: RESILIENT_EXAMPLES,
    },
];

/// What every generated file starts with, after its own two lines.
const HEADER_BODY: &str = "\
// FELIX_BLESS_DOCS=1 cargo test -p felix-client --test docs_examples_sync

/// What a `Body` example has in scope besides its own `use` lines.
#[allow(unused_imports)]
mod prelude {
    pub(crate) use anyhow::{Context, Result};
    pub(crate) use bytes::Bytes;
    pub(crate) use felix_client::*;
    pub(crate) use std::net::SocketAddr;
    pub(crate) use std::sync::Arc;
    pub(crate) use std::time::Duration;
}
";

fn header(page: &Page) -> String {
    format!(
        "// @generated by tests/docs_examples_sync.rs from\n\
         // docs-site/src/content/docs/clients/{}. Do not edit; regenerate with\n{HEADER_BODY}",
        page.file
    )
}

/// Every example is compiled and never called, many stop at a value a reader
/// would go on to use, and a `pub` item on the page is public in the reader's
/// crate, not this one.
const ALLOW: &str = "#![allow(dead_code, unused_variables, unused_assignments, \
                     unreachable_code, unreachable_pub)]";

struct Block {
    heading: String,
    nth: usize,
    line: usize,
    code: String,
}

fn page_path(page: &Page) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../docs-site/src/content/docs/clients")
        .join(page.file)
}

fn docs_site_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../docs-site")
}

fn generated_path(page: &Page) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/docs_examples")
        .join(page.generated)
}

/// Every ```` ```rust ```` block with the heading it sits under.
fn rust_blocks(page: &str) -> Vec<Block> {
    let mut blocks: Vec<Block> = Vec::new();
    let mut heading = String::new();
    let mut fence: Option<bool> = None; // Some(is_rust) inside a fence
    for (index, line) in page.lines().enumerate() {
        match fence {
            None if line.starts_with("```") => {
                let is_rust = line.trim_start_matches('`').trim() == "rust";
                if is_rust {
                    let nth = 1 + blocks.iter().filter(|b| b.heading == heading).count();
                    blocks.push(Block {
                        heading: heading.clone(),
                        nth,
                        line: index + 1,
                        code: String::new(),
                    });
                }
                fence = Some(is_rust);
            }
            None if line.starts_with('#') => {
                heading = line.trim_start_matches('#').trim().to_string();
            }
            None => {}
            Some(_) if line.starts_with("```") => fence = None,
            Some(true) => {
                let code = &mut blocks.last_mut().expect("inside a rust block").code;
                code.push_str(line);
                code.push('\n');
            }
            Some(false) => {}
        }
    }
    blocks
}

fn module_name(heading: &str, nth: usize) -> String {
    let mut name = String::new();
    for c in heading.chars() {
        if c.is_ascii_alphanumeric() {
            name.push(c.to_ascii_lowercase());
        } else if !name.ends_with('_') {
            name.push('_');
        }
    }
    format!("{}_{nth}", name.trim_matches('_'))
}

fn indent(code: &str, by: &str) -> String {
    code.lines()
        .map(|line| {
            if line.is_empty() {
                "\n".to_string()
            } else {
                format!("{by}{line}\n")
            }
        })
        .collect()
}

fn generate(spec: &Page, page: &str) -> Result<String, String> {
    let file = spec.file;
    let blocks = rust_blocks(page);
    let mut problems = Vec::new();
    for block in &blocks {
        if !spec
            .examples
            .iter()
            .any(|e| e.heading == block.heading && e.nth == block.nth)
        {
            problems.push(format!(
                "{file} line {}: block {} under \"{}\" has no entry in the page's examples",
                block.line, block.nth, block.heading
            ));
        }
    }
    for example in spec.examples {
        if !blocks
            .iter()
            .any(|b| b.heading == example.heading && b.nth == example.nth)
        {
            problems.push(format!(
                "The examples list block {} under \"{}\", which {file} does not have",
                example.nth, example.heading
            ));
        }
    }
    if !problems.is_empty() {
        return Err(problems.join("\n"));
    }

    let mut out = header(spec);
    for block in &blocks {
        let example = spec
            .examples
            .iter()
            .find(|e| e.heading == block.heading && e.nth == block.nth)
            .expect("checked above");
        let name = module_name(&block.heading, block.nth);
        writeln!(out).unwrap();
        writeln!(
            out,
            "/// {file} line {}: \"{}\", block {}.",
            block.line, block.heading, block.nth
        )
        .unwrap();
        writeln!(out, "mod {name} {{\n    {ALLOW}").unwrap();
        match example.wrap {
            File { context } => {
                out.push_str(&indent(&block.code, "    "));
                if !context.is_empty() {
                    out.push('\n');
                    out.push_str(&indent(context, "    "));
                }
            }
            Body { params, context } => {
                out.push_str("    use super::prelude::*;\n");
                if !context.is_empty() {
                    out.push('\n');
                    out.push_str(&indent(context, "    "));
                }
                writeln!(out, "\n    async fn example({params}) -> Result<()> {{").unwrap();
                out.push_str(&indent(&block.code, "        "));
                out.push_str("        Ok(())\n    }\n");
            }
        }
        out.push_str("}\n");
    }
    Ok(out)
}

#[test]
fn generated_examples_match_the_page() {
    for spec in PAGES {
        // A packaged crate has no docs site next to it. With the site present, a
        // missing page is a failure, not something to skip.
        if !docs_site_dir().is_dir() {
            continue;
        }
        let page = std::fs::read_to_string(page_path(spec))
            .unwrap_or_else(|err| panic!("read {}: {err}", spec.file));
        let expected = generate(spec, &page).unwrap_or_else(|problems| panic!("{problems}"));
        let path = generated_path(spec);
        if std::env::var_os("FELIX_BLESS_DOCS").is_some() {
            std::fs::write(&path, &expected).expect("write the generated examples");
            continue;
        }
        let actual = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            actual == expected,
            "{} is out of date with {}; regenerate it with\n\
             FELIX_BLESS_DOCS=1 cargo test -p felix-client --test docs_examples_sync",
            path.display(),
            spec.file
        );
    }
}
