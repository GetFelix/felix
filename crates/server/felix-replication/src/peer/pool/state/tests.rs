//! The pool's policy, tested without a peer on the other end.
//!
//! What a real peer would add here is a socket, and the rules below do not
//! depend on one: they decide which connection a request lands on, when a
//! refused peer may be dialled again, and which connections a reaper may take
//! away. Those are the parts that misbehave under load, and they are all
//! decisions about local state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::*;

/// A connection with no socket behind it, whose liveness, pending work, and age
/// are set by the test rather than by a peer.
struct FakeConnection {
    live: AtomicBool,
    pending: AtomicBool,
    idle_for: Duration,
    closed_with: Mutex<Option<String>>,
}

impl FakeConnection {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            pending: AtomicBool::new(false),
            idle_for: Duration::ZERO,
            closed_with: Mutex::new(None),
        })
    }

    fn idle(idle_for: Duration) -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            pending: AtomicBool::new(false),
            idle_for,
            closed_with: Mutex::new(None),
        })
    }

    fn die(&self) {
        self.live.store(false, Ordering::Release);
    }

    fn closed(&self) -> Option<String> {
        self.closed_with.lock().clone()
    }
}

impl Pooled for FakeConnection {
    fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    fn idle_for(&self) -> Duration {
        self.idle_for
    }

    fn close(&self, reason: &str) {
        *self.closed_with.lock() = Some(reason.to_string());
    }
}

fn state(connections: Vec<Arc<FakeConnection>>) -> PeerState<FakeConnection> {
    PeerState {
        connections,
        ..Default::default()
    }
}

/// Which connection a request lands on.
mod selection {
    use super::*;

    /// Growing to the configured size takes precedence over reuse. A pool of
    /// one available connection would otherwise never open a second, because
    /// the first is always there to hand back.
    #[test]
    fn a_pool_below_its_target_asks_for_another_connection() {
        let mut state = state(vec![FakeConnection::new()]);
        assert!(state.next(2).is_none());
    }

    #[test]
    fn a_full_pool_hands_back_a_connection() {
        let connection = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&connection)]);
        assert!(Arc::ptr_eq(
            &state.next(1).expect("a connection"),
            &connection
        ));
    }

    /// Requests spread across the connections rather than piling onto one: a
    /// QUIC stream is ordered, so a large batch on the same connection holds up
    /// everything queued behind it.
    #[test]
    fn requests_are_spread_across_the_connections() {
        let a = FakeConnection::new();
        let b = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&a), Arc::clone(&b)]);

        let handed_out: Vec<Arc<FakeConnection>> = (0..4)
            .map(|_| state.next(2).expect("a connection"))
            .collect();

        assert!(Arc::ptr_eq(&handed_out[0], &a));
        assert!(Arc::ptr_eq(&handed_out[1], &b));
        assert!(Arc::ptr_eq(&handed_out[2], &a), "the cursor should wrap");
        assert!(Arc::ptr_eq(&handed_out[3], &b));
    }

    /// The cursor is only ever advanced, so it has to survive passing `usize`'s
    /// end without indexing out of the pool.
    #[test]
    fn a_wrapped_cursor_still_selects_a_connection() {
        let mut state = state(vec![FakeConnection::new(), FakeConnection::new()]);
        state.cursor = usize::MAX;
        assert!(state.next(2).is_some());
        assert!(state.next(2).is_some(), "the cursor wrapped past the end");
    }

    /// An empty pool asks for a connection rather than dividing by its own
    /// length. A target of zero cannot reach here from configuration, which
    /// filters it out, but the remainder in `next` is one step from a panic on
    /// the forwarding path either way.
    #[test]
    fn an_empty_pool_has_nothing_to_hand_back() {
        let mut state = state(Vec::new());
        assert!(state.next(1).is_none());
        assert!(
            state.next(0).is_none(),
            "an empty pool divided by its own length"
        );
    }
}

/// Connections the pool must stop handing out.
mod reaping {
    use super::*;

    #[test]
    fn a_dead_connection_is_dropped_and_the_live_ones_stay() {
        let dead = FakeConnection::new();
        let live = FakeConnection::new();
        dead.die();
        let mut state = state(vec![Arc::clone(&dead), Arc::clone(&live)]);

        state.drop_dead();

        assert_eq!(state.connections.len(), 1);
        assert!(Arc::ptr_eq(&state.connections[0], &live));
    }

    #[test]
    fn a_dead_connection_is_not_counted_as_live() {
        let dead = FakeConnection::new();
        dead.die();
        let state = state(vec![dead, FakeConnection::new()]);
        assert_eq!(state.live_connections(), 1);
    }

    #[test]
    fn an_idle_connection_is_closed_and_evicted() {
        let idle = FakeConnection::idle(Duration::from_secs(60));
        let mut state = state(vec![Arc::clone(&idle)]);

        state.evict_idle(Duration::from_secs(30));

        assert!(state.connections.is_empty());
        assert_eq!(idle.closed().as_deref(), Some("idle"));
    }

    #[test]
    fn a_connection_younger_than_the_timeout_is_kept() {
        let young = FakeConnection::idle(Duration::from_secs(10));
        let mut state = state(vec![Arc::clone(&young)]);

        state.evict_idle(Duration::from_secs(30));

        assert_eq!(state.connections.len(), 1);
        assert_eq!(young.closed(), None);
    }

    /// **A connection with work outstanding is not idle**, however long ago it
    /// was last handed out. Reaping it would fail waiters that a healthy peer is
    /// still answering.
    #[test]
    fn a_connection_awaiting_a_response_is_not_reaped_however_old() {
        let waiting = FakeConnection::idle(Duration::from_secs(600));
        waiting.pending.store(true, Ordering::Release);
        let mut state = state(vec![Arc::clone(&waiting)]);

        state.evict_idle(Duration::from_secs(30));

        assert_eq!(
            state.connections.len(),
            1,
            "a connection with a waiter was reaped"
        );
        assert_eq!(waiting.closed(), None);
    }

    #[test]
    fn closing_the_peer_closes_every_connection_with_the_reason() {
        let a = FakeConnection::new();
        let b = FakeConnection::new();
        let mut state = state(vec![Arc::clone(&a), Arc::clone(&b)]);

        state.close_all("shutting down");

        assert!(state.connections.is_empty());
        assert_eq!(a.closed().as_deref(), Some("shutting down"));
        assert_eq!(b.closed().as_deref(), Some("shutting down"));
    }
}

/// The window in which a refused peer is not dialled again.
mod backoff {
    use super::*;

    #[test]
    fn a_peer_that_has_not_failed_is_not_in_backoff() {
        let state = state(Vec::new());
        assert!(!state.in_backoff());
        assert_eq!(state.backoff_remaining(), None);
    }

    #[test]
    fn a_peer_inside_its_window_is_in_backoff() {
        let mut state = state(Vec::new());
        state.backoff_until = Some(Instant::now() + Duration::from_secs(30));

        assert!(state.in_backoff());
        assert!(state.backoff_remaining().expect("remaining") <= Duration::from_secs(30));
    }

    /// An elapsed deadline reads as "dial again", not as a huge remaining wait.
    /// Subtracting the other way round would leave a peer unreachable for as
    /// long as it had been unreachable already.
    #[test]
    fn an_elapsed_window_is_over_rather_than_wrapping() {
        let mut state = state(Vec::new());
        state.backoff_until = Some(Instant::now() - Duration::from_secs(30));

        assert!(!state.in_backoff());
        assert_eq!(state.backoff_remaining(), None);
    }
}
