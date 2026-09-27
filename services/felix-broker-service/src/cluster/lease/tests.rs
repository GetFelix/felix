//! Lease expiry, deterministically.
//!
//! `start_paused` gives tokio's clock to the test, so expiry is driven by
//! advancing time rather than by sleeping through it. That is what makes these
//! exact rather than approximate. Production leases run on `CLOCK_BOOTTIME`,
//! so these build theirs on tokio's clock with `on_tokio_clock`.
use super::*;

const LEASE: Duration = Duration::from_secs(4);

/// A quarter is held back, so a 4s lease is usable for 3s.
#[test]
fn a_quarter_of_the_lease_is_surrendered_as_margin() {
    let lease = LeaseState::new(LEASE);
    assert_eq!(lease.usable(), Duration::from_secs(3));
}

/// A broker has no authority until its first heartbeat is accepted.
///
/// Starting valid would let one that never successfully registered serve for a
/// full lease period.
#[tokio::test(start_paused = true)]
async fn a_lease_starts_invalid() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    assert!(!lease.looks_valid());
    assert!(!lease.is_valid_now());
}

#[tokio::test(start_paused = true)]
async fn a_renewed_lease_is_valid_until_its_usable_life_elapses() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    lease.renew();
    assert!(lease.is_valid_now());

    // Just inside.
    tokio::time::advance(Duration::from_millis(2_999)).await;
    assert!(lease.is_valid_now(), "still inside the usable window");

    // Just past.
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(!lease.is_valid_now(), "the usable window has elapsed");
}

/// The case the commit-boundary check exists for.
///
/// A broker suspended past its expiry wakes with the cached flag still saying
/// yes. Only the clock knows otherwise, and a write must not be committed on the
/// strength of the stale flag.
#[tokio::test(start_paused = true)]
async fn a_broker_suspended_past_expiry_fails_the_commit_check_while_admission_still_passes() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    lease.renew();

    // Nothing refreshes the cached flag: this is the suspension.
    tokio::time::advance(Duration::from_secs(10)).await;

    assert!(
        lease.looks_valid(),
        "the cached flag is stale by construction here -- that is the premise",
    );
    assert!(
        !lease.is_valid_now(),
        "the clock check must refuse, or a suspended broker writes after losing its lease",
    );
}

/// Renewal extends the lease, which is what a heartbeat is for.
#[tokio::test(start_paused = true)]
async fn renewing_extends_the_lease() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    lease.renew();

    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(2)).await;
        lease.renew();
        assert!(lease.is_valid_now(), "a renewed lease must not expire");
    }

    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !lease.is_valid_now(),
        "renewal stopped, so the lease lapses"
    );
}

/// A late renewal must not resurrect an expired lease into the past.
#[tokio::test(start_paused = true)]
async fn renewal_never_moves_backwards() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    tokio::time::advance(Duration::from_secs(10)).await;
    lease.renew();
    let late = lease.is_valid_now();

    // A renewal recorded out of order -- two heartbeat responses racing -- must
    // not move the anchor backwards and shorten the lease.
    lease.renewed_at_millis.fetch_max(1, Ordering::Release);
    assert_eq!(
        lease.is_valid_now(),
        late,
        "an out-of-order renewal must not shorten the lease",
    );
}

/// Surrender is immediate, for a broker told it is no longer a member.
#[tokio::test(start_paused = true)]
async fn surrender_takes_effect_at_once() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    lease.renew();
    assert!(lease.is_valid_now());

    lease.surrender();
    assert!(!lease.looks_valid());
    assert!(
        !lease.is_valid_now(),
        "surrender must not wait out the margin"
    );
}

/// The refresh task moves the cached flag to false on its own, so the admission
/// path stops accepting without needing a publish to discover it.
#[tokio::test(start_paused = true)]
async fn the_refresh_task_invalidates_the_cached_flag() {
    let lease = Arc::new(LeaseState::on_tokio_clock(LEASE));
    lease.renew();
    let shutdown = CancellationToken::new();
    let task = Arc::clone(&lease).spawn_refresh(shutdown.clone());

    tokio::time::advance(Duration::from_secs(4)).await;
    // Let the ticker run at the advanced time.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;

    assert!(
        !lease.looks_valid(),
        "the admission path should stop accepting without waiting for a publish",
    );

    shutdown.cancel();
    let _ = task.await;
}

/// A stall between sending a heartbeat and handling its answer must not
/// lengthen the lease. Anchored at arrival, a stalled broker holds authority
/// for the lease *plus* the stall — past what the control plane granted.
#[tokio::test(start_paused = true)]
async fn a_pause_between_sending_and_handling_does_not_extend_the_lease() {
    let lease = LeaseState::on_tokio_clock(LEASE);

    // The heartbeat goes out, and the control plane records it here.
    let sent = lease.now();

    // The broker then stalls for most of the usable lease before it gets round
    // to handling the response.
    tokio::time::advance(Duration::from_millis(2_500)).await;
    lease.renew_at(sent);

    // Half a second of the 3s usable window is left, measured from when the
    // control plane started counting.
    assert!(
        lease.is_valid_now(),
        "the lease should still have time on it"
    );
    tokio::time::advance(Duration::from_millis(499)).await;
    assert!(lease.is_valid_now());

    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(
        !lease.is_valid_now(),
        "the lease outlived the window the control plane granted: it was \
         anchored when the answer was handled rather than when the heartbeat \
         was sent, so a stalled broker keeps serving past its expiry",
    );
}

/// An out-of-order response cannot shorten the lease. Anchoring at send makes
/// this reachable — two heartbeats in flight carry different anchors, and the
/// older one must not win.
#[tokio::test(start_paused = true)]
async fn an_older_heartbeat_handled_late_does_not_move_the_anchor_back() {
    let lease = LeaseState::on_tokio_clock(LEASE);

    let first = lease.now();
    tokio::time::advance(Duration::from_millis(1_000)).await;
    let second = lease.now();

    // The later heartbeat is handled first, then the earlier one arrives.
    lease.renew_at(second);
    lease.renew_at(first);

    // Still anchored at the later one: 3s of usable lease from `second`.
    tokio::time::advance(Duration::from_millis(2_999)).await;
    assert!(
        lease.is_valid_now(),
        "a late-arriving older heartbeat shortened the lease",
    );
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(!lease.is_valid_now());
}

#[tokio::test(start_paused = true)]
async fn remaining_counts_down_from_the_usable_life_to_zero() {
    let lease = LeaseState::on_tokio_clock(LEASE);
    assert_eq!(lease.remaining(), Duration::ZERO, "never renewed");
    lease.renew();
    assert_eq!(lease.remaining(), Duration::from_secs(3));

    tokio::time::advance(Duration::from_millis(2_500)).await;
    assert_eq!(lease.remaining(), Duration::from_millis(500));

    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(lease.remaining(), Duration::ZERO);
}

/// A production lease is judged on the system (boottime) clock.
#[test]
fn a_production_lease_runs_on_the_system_clock() {
    let lease = LeaseState::new(LEASE);
    assert!(matches!(lease.clock, LeaseClock::System));
}

/// Validity is read from the lease clock and nothing else: tokio's clock
/// standing still (as `CLOCK_MONOTONIC` does across a suspend) must not keep a
/// lease alive once the lease clock has moved past it.
#[tokio::test(start_paused = true)]
async fn validity_follows_the_lease_clock_not_tokio() {
    let millis = Arc::new(AtomicU64::new(1_000));
    let lease = LeaseState::with_clock(LEASE, LeaseClock::Manual(Arc::clone(&millis)));
    lease.renew();
    assert!(lease.is_valid_now());

    // Tokio time moves; the lease clock does not. Still valid.
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(
        lease.is_valid_now(),
        "the lease is not measured on tokio's clock"
    );

    // The host was suspended: the lease clock jumped, tokio's did not.
    millis.fetch_add(3_001, Ordering::AcqRel);
    assert!(
        !lease.is_valid_now(),
        "a lease must lapse on the clock that counts suspended time",
    );
    assert_eq!(lease.remaining(), Duration::ZERO);
}
