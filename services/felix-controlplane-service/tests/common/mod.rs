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
