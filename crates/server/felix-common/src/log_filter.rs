//! The log filter the services use when `RUST_LOG` is unset.

/// Filter for unit tests without `RUST_LOG`. In-process Raft clusters emit
/// per-tick info and heartbeat warnings that would bury real output.
pub const QUIET_TEST_LOG: &str = "warn,openraft=error";

/// Filter for a running service without `RUST_LOG`.
pub const DEFAULT_LOG: &str = "info";

/// The filter for an unset `RUST_LOG`. Callers pass `cfg!(test)`, which this
/// crate cannot see for them.
pub const fn default_filter(quiet: bool) -> &'static str {
    if quiet { QUIET_TEST_LOG } else { DEFAULT_LOG }
}
