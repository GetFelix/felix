//! How a [`super::ClusterClient`] paces its retries, and what it does after
//! each kind of failure.

use std::time::{Duration, Instant};

use felix_wire::{ErrorCode, RetryClass};

use crate::BrokerError;

/// How long `not_found` stays worth retrying, counted from the first one an
/// operation got.
///
/// A broker learns streams from the control plane, so one promoted a moment
/// ago says `not_found` for a stream it is about to serve, until its next sync
/// (every 2 s by default). Two of those is plenty; past that the stream really
/// is missing, and retrying on would hide a typo or a deleted stream behind
/// whatever attempt budget the caller configured.
pub(crate) const NOT_FOUND_GRACE: Duration = Duration::from_secs(5);

/// How long to wait between reconnection attempts, how many to make, and how
/// long the whole thing may take.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Attempts to reach *some* broker before giving up. Each attempt tries
    /// every endpoint.
    pub attempts: usize,
    /// The first retry waits somewhere in `[0, backoff]`. The ceiling doubles
    /// each attempt up to `max_backoff`.
    pub backoff: Duration,
    /// The longest any one wait can be.
    pub max_backoff: Duration,
    /// A ceiling on the whole operation, across every attempt and every sleep.
    ///
    /// Attempt counts alone do not bound time: five attempts against a broker
    /// that takes its full publish timeout to answer is minutes, which is not a
    /// number anybody chose.
    ///
    /// **`None` by default, and deliberately.** A deadline shorter than one
    /// attempt's own timeout prevents any retry at all — the first attempt
    /// spends the whole budget and the loop exits having tried once. The
    /// client's publish timeout is already tens of seconds, so any useful
    /// default here would have to be derived from that rather than picked, and
    /// picking one silently turns a client that recovers from a failover into
    /// one that does not.
    ///
    /// A caller that knows its own latency budget should set it.
    pub deadline: Option<Duration>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            // Enough to outlast a failover on a healthy cluster — the control
            // plane has to notice the leader is gone, which is bounded by its
            // expiry timeout — without waiting out a cluster that is simply
            // down.
            attempts: 5,
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
            deadline: None,
        }
    }
}

impl ReconnectPolicy {
    /// How long to wait before attempt `attempt` (0-based), jittered.
    ///
    /// **Full jitter: uniform over `[0, ceiling]`, not the ceiling itself.**
    /// Every client of a cluster notices a failover at the same moment, and an
    /// unjittered backoff has all of them retry in step — arriving together at
    /// whichever broker was just promoted, which is the moment it can least
    /// afford a thundering herd. The broker's own peer pool jitters its redials
    /// for exactly this reason.
    pub(crate) fn delay_before(&self, attempt: usize) -> Duration {
        let ceiling = self
            .backoff
            .saturating_mul(1u32 << attempt.min(16) as u32)
            .min(self.max_backoff);
        ceiling.mul_f64(jitter_fraction())
    }
}

/// What a retrying loop does after one failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Next {
    /// Return the error.
    Fail,
    /// Drop the cached route and try again now, through the entry broker.
    Reroute,
    /// Wait out the backoff, but no less than `at_least`, and try again.
    Backoff { at_least: Duration },
}

/// What a retrying loop remembers between attempts.
#[derive(Debug, Default)]
pub(crate) struct Retrying {
    not_found_since: Option<Instant>,
}

impl Retrying {
    /// Decide what follows `error`, keeping the `not_found` clock.
    pub(crate) fn next(&mut self, error: &anyhow::Error, attempt: Attempt) -> Next {
        let not_found = error
            .downcast_ref::<BrokerError>()
            .is_some_and(|broker| broker.code == ErrorCode::NotFound);
        let not_found_for = not_found.then(|| {
            self.not_found_since
                .get_or_insert_with(Instant::now)
                .elapsed()
        });
        next_step(
            error,
            Attempt {
                not_found_for,
                ..attempt
            },
        )
    }
}

/// The facts about a failed attempt that change what to do next.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Attempt {
    /// It went to a cached shard owner or leader, not the entry broker.
    pub(crate) routed: bool,
    /// Sending again is acceptable even if the first may have landed: the
    /// batch carries an idempotent sequence, or the caller asked for at least
    /// once.
    pub(crate) resend_ambiguous: bool,
    /// How long this operation has been answered `not_found`.
    pub(crate) not_found_for: Option<Duration>,
}

/// What to do after `error`, decided by the broker's retry class when it sent
/// one.
///
/// - `fatal`: fail.
/// - `outcome_unknown`: the write may have landed. Only re-sent when the
///   caller said that is acceptable.
/// - `retry` / `redirect` (`shard_unavailable`, `draining`, `not_leader`):
///   nothing was applied. Through a cached route that route is stale, so drop
///   it and go through the entry broker at once; from the entry broker itself,
///   back off, since it is the cluster that has to settle. `stale_claim` is
///   `retry` too, but says nothing about the route: back off.
/// - `retry_after` (`overloaded`, `not_found`): back off, for at least as long
///   as the broker asked; `not_found` only within [`NOT_FOUND_GRACE`].
///
/// An error without a code comes from a peer that did not negotiate codes, or
/// from the transport, and goes through [`is_terminal`].
pub(crate) fn next_step(error: &anyhow::Error, attempt: Attempt) -> Next {
    let backoff = Next::Backoff {
        at_least: Duration::ZERO,
    };
    let Some(broker) = error.downcast_ref::<BrokerError>() else {
        return if is_terminal(error) {
            Next::Fail
        } else {
            backoff
        };
    };
    match broker.retry {
        RetryClass::Fatal => Next::Fail,
        RetryClass::OutcomeUnknown if attempt.resend_ambiguous => backoff,
        RetryClass::OutcomeUnknown => Next::Fail,
        // A stale claim comes from the shard's leader, so the route is fine.
        RetryClass::Retry | RetryClass::Redirect
            if attempt.routed && broker.code != ErrorCode::StaleClaim =>
        {
            Next::Reroute
        }
        RetryClass::Retry | RetryClass::Redirect => backoff,
        RetryClass::RetryAfter => {
            if broker.code == ErrorCode::NotFound
                && attempt
                    .not_found_for
                    .is_some_and(|waited| waited >= NOT_FOUND_GRACE)
            {
                return Next::Fail;
            }
            let asked = broker
                .detail
                .as_ref()
                .and_then(|detail| detail.retry_after_ms)
                .map(Duration::from_millis)
                .unwrap_or_default();
            Next::Backoff { at_least: asked }
        }
    }
}

/// Whether a broker this client was routed to -- a cached owner or one a
/// redirect named -- has said it no longer serves the shard, so the entry
/// broker should be asked again.
pub(crate) fn route_went_stale(error: &anyhow::Error) -> bool {
    let attempt = Attempt {
        routed: true,
        ..Attempt::default()
    };
    next_step(error, attempt) == Next::Reroute
}

/// Whether a failed publish means the broker in hand should be replaced.
///
/// A coded answer means the broker is up and answering, so reconnecting would
/// only tear down healthy connections -- unless the answer is that it is
/// shutting down. No code means a dead connection or a peer that predates
/// codes, and replacing the client is what this wrapper has always done then.
pub(crate) fn wants_reconnect(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<BrokerError>() {
        Some(broker) => broker.code == ErrorCode::Draining,
        None => true,
    }
}

/// Whether an uncoded error is worth another attempt.
///
/// **Unknown errors are retried.** Without a code this is matching on prose,
/// and prose changes. A misclassified retryable error costs one wasted
/// attempt; a misclassified terminal error costs the operation. Defaulting to
/// "retry" puts the cheaper mistake on the likely side.
///
/// **"Not found" is not terminal.** A broker promoted a moment ago answers
/// "stream not found" for the stream it is about to serve, and treating that as
/// terminal breaks the failover recovery this policy exists for.
///
/// What is left is the credential: a permission the token does not carry fails
/// the same way on every broker for as long as the token lives.
pub(crate) fn is_terminal(error: &anyhow::Error) -> bool {
    // Terminal by construction rather than by matching prose: the offset asked
    // for is not available, or the broker refused an idempotent batch for a
    // reason a re-send cannot mend.
    if error
        .downcast_ref::<crate::SubscribeCursorError>()
        .is_some()
        || error.downcast_ref::<crate::PublishRefused>().is_some()
    {
        return true;
    }
    format!("{error:#}").to_lowercase().contains("forbidden")
}

/// A uniform fraction in `[0, 1)`, without taking an RNG dependency.
///
/// The same approach the broker's reconnect backoff uses. Its only job is to
/// decorrelate clients that all woke up together, so it needs to be unbiased
/// rather than unpredictable.
fn jitter_fraction() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000_000) / 1_000_000.0
}
