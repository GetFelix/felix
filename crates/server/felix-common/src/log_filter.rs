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

/// `RUST_LOG` when it parses, else [`default_filter`].
#[cfg(feature = "logging")]
pub fn env_filter(quiet: bool) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter(quiet)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_only_in_tests() {
        assert_eq!(default_filter(true), QUIET_TEST_LOG);
        assert_eq!(default_filter(false), DEFAULT_LOG);
    }
}
