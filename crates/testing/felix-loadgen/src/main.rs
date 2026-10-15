//! `felix-loadgen`: drive a remote Felix cluster and measure it.
//!
//! ```text
//! felix-loadgen --brokers 10.0.0.4:5000,10.0.0.5:5000 \
//!     --tenant t1 --token-file /run/felix/token \
//!     --scenario pubsub --fanout 10 --payload-bytes 256 --total 20000
//! ```
//!
//! Output: human-readable lines in the shape `scripts/perf` already parses,
//! plus one `LOADGEN_JSON {...}` line per case — the machine contract the
//! Azure runner consumes. The scenarios themselves are the library half of
//! this crate.

mod args;

use anyhow::{Context, Result};

use crate::args::parse_args;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build runtime")?;
    runtime.block_on(run())
}

async fn run() -> Result<()> {
    let args = parse_args()?;
    let result = felix_loadgen::run(&args.common, &args.scenario).await?;
    felix_loadgen::emit_json(&result);
    Ok(())
}
