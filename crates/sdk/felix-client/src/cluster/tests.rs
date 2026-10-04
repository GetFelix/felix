mod cache_routing;
mod connections;
mod decisions;
mod error_codes;
mod keyed_producer;
mod offsets;
mod sharded_lag;
mod stub_broker;

use std::time::Duration;

use super::retry::{ReconnectPolicy, is_terminal};

fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 5,
        backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(2),
        deadline: None,
    }
}

/// **Every delay is inside its ceiling, and the ceiling doubles.** Full
/// jitter means the delay is somewhere in `[0, ceiling]`, so the property
/// worth pinning is the bound rather than the value.
#[test]
fn backoff_stays_inside_a_doubling_ceiling() {
    let policy = policy();
    for attempt in 0..6 {
        let ceiling = policy
            .backoff
            .saturating_mul(1u32 << attempt)
            .min(policy.max_backoff);
        for _ in 0..50 {
            let delay = policy.delay_before(attempt);
            assert!(
                delay <= ceiling,
                "attempt {attempt}: {delay:?} exceeds its ceiling {ceiling:?}",
            );
        }
    }
}

#[test]
fn backoff_is_capped_by_max_backoff() {
    let policy = policy();
    for attempt in 0..20 {
        assert!(policy.delay_before(attempt) <= policy.max_backoff);
    }
}

/// **The delay actually varies.** A "jitter" that returned the same number
/// every time would satisfy the bound above and still send every client of
/// a cluster at a freshly promoted broker in step.
#[test]
fn backoff_is_jittered_rather_than_fixed() {
    let policy = ReconnectPolicy {
        backoff: Duration::from_secs(1),
        ..policy()
    };
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        seen.insert(policy.delay_before(0).as_micros());
        std::thread::sleep(Duration::from_micros(50));
    }
    assert!(
        seen.len() > 5,
        "the backoff produced {} distinct delays; it is not jittered",
        seen.len()
    );
}

#[test]
fn a_forbidden_error_is_terminal() {
    assert!(is_terminal(&anyhow::anyhow!("forbidden")));
    assert!(is_terminal(
        &anyhow::anyhow!("publish failed").context("forbidden")
    ));
}

/// A cursor error is terminal by construction, and typed, so it does not
/// depend on matching prose.
#[test]
fn a_cursor_error_is_terminal() {
    let err: anyhow::Error = crate::SubscribeCursorError {
        reason: felix_wire::CursorErrorReason::TooOld,
        requested: 5,
        available: 100,
    }
    .into();
    assert!(is_terminal(&err));
}

/// **The failures a failover produces are retried.** These are the whole
/// point of the policy, and classifying one of them as terminal would turn
/// a recoverable blip into a lost publish.
#[test]
fn transient_failures_are_retried() {
    for message in [
        "connection lost",
        "publish commit timeout",
        "the batch is durable here but did not reach a majority within 5s",
        "shard leadership moved before the batch could reach a quorum",
        "no peer transport: this broker cannot forward to broker-2",
        "stream orders cannot be subscribed to right now: owner unavailable",
        // A broker promoted a moment ago has not opened the shard yet and
        // says exactly this. Classifying it as terminal broke
        // `records_published_across_a_failover_are_all_readable`.
        "stream not found: tenant=t1 namespace=ns stream=orders",
        "unknown tenant t1",
        "unknown namespace ns",
    ] {
        assert!(
            !is_terminal(&anyhow::anyhow!(message.to_string())),
            "{message:?} should be retried",
        );
    }
}

/// An error nobody has classified is retried, because a wasted attempt is
/// cheaper than a lost operation.
#[test]
fn an_unrecognised_error_is_retried() {
    assert!(!is_terminal(&anyhow::anyhow!("something nobody foresaw")));
}
