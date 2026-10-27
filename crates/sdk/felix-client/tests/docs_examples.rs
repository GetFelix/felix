//! Compiles the Rust examples from the docs site.
//!
//! `docs_examples/rust_md.rs` is generated from
//! `docs-site/src/content/docs/clients/rust.md` by the `docs_examples_sync`
//! test, which fails when the page and this file disagree. The tutorials get
//! the same treatment in `docs_examples/tutorials_mdx.rs`, one module per page. Most examples need
//! a broker, so they are compiled here and not run.

// Generated code keeps the page's own formatting.
#[rustfmt::skip]
#[path = "docs_examples/rust_md.rs"]
mod rust_md;

#[rustfmt::skip]
#[path = "docs_examples/tutorials_mdx.rs"]
mod tutorials_mdx;
