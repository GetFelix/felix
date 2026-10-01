//! Load generator for a *remote* Felix cluster.
//!
//! The measuring instrument of the real-network perf suite
//! (`docs/perf-real-network.md`). Every other driver in this repository is
//! loopback-bound — `latency-demo` runs an in-process broker, `soak` spawns
//! its own child — so this is the one that dials addresses it is given and
//! measures what a deployment's client would feel: acknowledgement round
//! trips, publish-to-delivery latency, cache/counter round trips, and watch
//! fanout delivery, all through the real routed paths (forwards and
//! redirects included).
//!
//! It measures the cluster; it is not part of it. Brokers and the control
//! plane under test run release artifacts. This binary may be built from a
//! pinned ref on the load-generator machine — the instrument's build does not
//! contaminate the measurement, so long as it is built *before* any run.
//!
//! ```text
//! felix-loadgen --brokers 10.0.0.4:5000,10.0.0.5:5000 \
//!     --tenant t1 --token-file /run/felix/token \
//!     --scenario pubsub --fanout 10 --payload-bytes 256 --total 20000
//! ```
//!
//! Output: human-readable lines in the shape `scripts/perf` already parses,
//! plus one `LOADGEN_JSON {...}` line per case — the machine contract the
//! Azure runner consumes.

mod args;
mod scenarios;
mod stats;
mod tls;

use anyhow::{Context, Result, bail};

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
    match args.scenario.as_str() {
        "pubsub" => scenarios::pubsub(&args.common, &args.stream, args.binary).await,
        "cache" => scenarios::cache(&args.common, &args.cache).await,
        "counter" => scenarios::counter(&args.common, &args.cache).await,
        "watch" => scenarios::watch(&args.common, &args.cache).await,
        "queue" => scenarios::queue(&args.common, &args.stream).await,
        "retained" => scenarios::retained(&args.common, &args.cache).await,
        "ingest" => scenarios::ingest(&args.common, &args.stream, &args.ingest).await,
        other => bail!(
            "unknown scenario {other:?} (pubsub | cache | counter | watch | queue | retained | ingest)"
        ),
    }
}
