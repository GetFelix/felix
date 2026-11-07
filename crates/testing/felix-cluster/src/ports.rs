//! Picking addresses that are free right now.
//!
//! Every process the harness starts binds a port it was told to bind, so the
//! harness has to choose them. It asks the OS for an ephemeral port, notes it,
//! and closes the socket — which leaves a window where something else could take
//! it. That window is why callers get a fresh port per attempt rather than a
//! cached one, and why start-up retries.
use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::Mutex;

use anyhow::{Context, Result};

/// Ports this process has handed out, so concurrent or back-to-back picks
/// never return the same one after the OS frees it. The sets are cleared once
/// they grow large, since a port from long ago is as good as new.
static HANDED_TCP: Mutex<Option<HashSet<u16>>> = Mutex::new(None);
static HANDED_UDP: Mutex<Option<HashSet<u16>>> = Mutex::new(None);
const HANDED_LIMIT: usize = 8_000;

/// Record `port` as handed out. False if it already was.
fn claim(set: &Mutex<Option<HashSet<u16>>>, port: u16) -> bool {
    let mut guard = set.lock().unwrap_or_else(|e| e.into_inner());
    let handed = guard.get_or_insert_with(HashSet::new);
    if handed.len() >= HANDED_LIMIT {
        handed.clear();
    }
    handed.insert(port)
}

/// A free TCP port on loopback, for the HTTP listeners.
pub fn free_tcp() -> Result<SocketAddr> {
    pick_unclaimed(&HANDED_TCP, || {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral TCP port")?;
        listener.local_addr().context("read TCP port")
    })
}

fn pick_unclaimed(
    set: &Mutex<Option<HashSet<u16>>>,
    pick: impl Fn() -> Result<SocketAddr>,
) -> Result<SocketAddr> {
    for _ in 0..50 {
        let addr = pick()?;
        if claim(set, addr.port()) {
            return Ok(addr);
        }
    }
    anyhow::bail!("no port this process has not already handed out after 50 attempts")
}

/// A free TCP port on loopback that `taken` does not claim: one already handed
/// to the same process, whose socket was closed and so is free to the OS.
pub fn free_tcp_except(taken: impl Fn(u16) -> bool) -> Result<SocketAddr> {
    pick_except(free_tcp, taken)
}

/// A free UDP port on loopback, for the QUIC listeners.
pub fn free_udp() -> Result<SocketAddr> {
    pick_unclaimed(&HANDED_UDP, || {
        let socket = UdpSocket::bind("127.0.0.1:0").context("bind ephemeral UDP port")?;
        socket.local_addr().context("read UDP port")
    })
}

/// A free UDP port on loopback that `taken` does not claim. See
/// [`free_tcp_except`].
pub fn free_udp_except(taken: impl Fn(u16) -> bool) -> Result<SocketAddr> {
    pick_except(free_udp, taken)
}

fn pick_except(
    pick: impl Fn() -> Result<SocketAddr>,
    taken: impl Fn(u16) -> bool,
) -> Result<SocketAddr> {
    const ATTEMPTS: usize = 50;
    for _ in 0..ATTEMPTS {
        let addr = pick()?;
        if !taken(addr.port()) {
            return Ok(addr);
        }
    }
    anyhow::bail!("no free port outside those already picked after {ATTEMPTS} attempts")
}

/// A run of `count` consecutive free UDP ports on loopback.
///
/// A broker with several client listeners binds consecutive ports, so the
/// harness has to hand it a base with room after it. The OS will not allocate a
/// run, so this probes: take an ephemeral port as the base and try to bind the
/// rest, starting over if any is taken. The same close-then-rebind window as
/// [`free_udp`] applies, which is why start-up retries.
pub fn free_udp_run(count: usize) -> Result<SocketAddr> {
    const ATTEMPTS: usize = 50;
    for _ in 0..ATTEMPTS {
        let base = free_udp()?;
        let Some(last) = base.port().checked_add(count as u16 - 1) else {
            continue;
        };
        let held: Vec<_> = (base.port()..=last)
            .map_while(|port| UdpSocket::bind(("127.0.0.1", port)).ok())
            .collect();
        if held.len() == count {
            for port in base.port()..=last {
                claim(&HANDED_UDP, port);
            }
            return Ok(base);
        }
    }
    anyhow::bail!("no run of {count} consecutive free UDP ports after {ATTEMPTS} attempts")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_port_is_never_handed_out_twice() {
        let (mut tcp, mut udp) = (HashSet::new(), HashSet::new());
        for _ in 0..200 {
            // Dropped at once, so the OS is free to reuse it.
            assert!(tcp.insert(free_tcp().expect("tcp").port()));
            assert!(udp.insert(free_udp().expect("udp").port()));
        }
    }

    #[test]
    fn a_pick_after_a_port_was_taken_is_a_different_port() {
        let first = free_tcp().expect("tcp");
        // Another process takes it before the broker can.
        let _thief = TcpListener::bind(first).expect("take the port");
        let second = free_tcp().expect("tcp");
        assert_ne!(first.port(), second.port());
    }
}
