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
//! The scenarios are a library so `felix bench` runs the same code. The
//! `felix-loadgen` binary is a flag parser over [`run`] plus
//! [`emit_json`]; its stdout and its `LOADGEN_JSON` line are the contract the
//! Azure runner and `scripts/perf` parse.

mod scenarios;
mod stats;
mod tls;

pub use scenarios::{Common, IngestOptions, Scenario, run};
pub use stats::emit_json;
