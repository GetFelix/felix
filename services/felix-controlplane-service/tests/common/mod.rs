//! Helpers shared by the integration tests that run as binaries of their own.
//!
//! Each test binary compiles its own copy of this module, and no single
//! binary uses every helper — so per-target dead-code analysis is noise here.
#![allow(dead_code)]

pub(crate) async fn read_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

/// A schema name no other caller will produce.
///
/// `pid + timestamp` is not enough. Tests in one binary run on parallel
/// threads, so the pid is identical, and two threads starting together can read
/// the same timestamp whenever the clock's granularity is coarser than the gap
/// between them — which is how two tests came to ask for the same schema and
/// the second was refused:
///
/// ```text
/// duplicate key value violates unique constraint "pg_namespace_nspname_index"
/// Key (nspname)=(felix_migrate_31654_1789668951690256729) already exists
/// ```
///
/// `CREATE SCHEMA IF NOT EXISTS` does not save it: two concurrent creates of
/// the same name race in Postgres and one gets exactly that error, so the
/// uniqueness has to be real rather than papered over at the call site.
///
/// The counter is what makes it real. The pid separates concurrent test
/// binaries, the timestamp keeps a name readable and tells runs apart, and the
/// counter guarantees that two calls in one process differ however close
/// together they are.
pub(crate) fn unique_schema(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed),
    )
}

// No `#[cfg(test)]`: this module is compiled into integration test binaries,
// which are already test crates, so the gate would remove these entirely.
mod tests {
    use super::*;

    /// The condition that broke it: many names asked for at once, from one
    /// process. A timestamp alone repeats here whenever the clock is coarser
    /// than the gap between two calls.
    #[test]
    fn names_are_unique_within_one_process() {
        let names: std::collections::HashSet<String> =
            (0..2_000).map(|_| unique_schema("felix_test")).collect();
        assert_eq!(
            names.len(),
            2_000,
            "two calls produced the same schema name, which Postgres refuses \
             with a duplicate-key error rather than reusing",
        );
    }

    /// And across threads, which is how tests in one binary actually run.
    #[test]
    fn names_are_unique_across_threads() {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..500)
                        .map(|_| unique_schema("felix_test"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let names: std::collections::HashSet<String> = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("thread"))
            .collect();
        assert_eq!(names.len(), 4_000);
    }

    #[test]
    fn a_name_is_a_usable_identifier() {
        let name = unique_schema("felix_test");
        assert!(name.starts_with("felix_test_"));
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "a schema name has to be quotable without escaping: {name}",
        );
    }
}

/// A free localhost port for a child process to bind later, and to bind again
/// after a restart.
///
/// Not `bind(":0")`: that draws from the ephemeral range, which is also where
/// every outgoing connection gets its source port. Between releasing the port
/// here and the child binding it — or while a killed child is down — any
/// connection on the machine can take it, and the child exits with "Address
/// already in use". CI hit exactly that. This hands out ports below the
/// ephemeral range instead (32768 on Linux, 49152 on macOS), each at most
/// once per process; the pid spreads concurrent test binaries apart.
pub(crate) fn reserve_port() -> std::net::SocketAddr {
    use std::sync::atomic::{AtomicU32, Ordering};
    const FIRST: u32 = 20_000;
    const SPAN: u32 = 12_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);

    let base = std::process::id().wrapping_mul(7_919) % SPAN;
    for _ in 0..SPAN {
        let port = FIRST + (base + NEXT.fetch_add(1, Ordering::Relaxed)) % SPAN;
        if let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", port as u16)) {
            return listener.local_addr().expect("local addr");
        }
    }
    panic!("no free port between {FIRST} and {}", FIRST + SPAN);
}
