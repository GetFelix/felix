//! Compiles the Rust examples from the docs site.
//!
//! `docs_examples/rust_md.rs` and `docs_examples/resilient_clients_mdx.rs` are
//! generated from `rust.md` and `resilient-clients.mdx` under
//! `docs-site/src/content/docs/clients/` by the `docs_examples_sync` test,
//! which fails when a page and its file disagree. Most examples need
//! a broker, so they are compiled here and not run.

// Generated code keeps the page's own formatting.
#[rustfmt::skip]
#[path = "docs_examples/rust_md.rs"]
mod rust_md;

#[rustfmt::skip]
#[path = "docs_examples/resilient_clients_mdx.rs"]
mod resilient_clients_mdx;
