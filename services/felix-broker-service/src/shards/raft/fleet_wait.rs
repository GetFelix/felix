//! Holding member opens until the fleet's features are known.
//!
//! A member reads some fleet features once, when it opens: which fsync mode
//! its log runs, and whether a one-replica shard may re-form itself. After a
//! restart the open pass can run before the broker's registration has
//! answered, when a feature the fleet finalized still reads as off. So the
//! pass holds members until every such feature is decided, for at most
//! [`FLEET_WAIT`]. See `docs/per-shard-raft.md`, "The broker runs the gate
//! after its start".

use std::time::Duration;

use felix_common::fleet::{self, FleetFeature, FleetView};
use tokio::time::Instant;

use super::RaftShards;

/// The fleet features a member reads once, at its open.
const OPEN_READS: [FleetFeature; 2] = [fleet::RAFT_WRITTEN_COMMIT, fleet::RAFT_ALONE_REFORM];

/// How long the open pass holds members for a registration's answer. Past
/// it they open on what is known, which is the strict side of each choice.
pub(super) const FLEET_WAIT: Duration = Duration::from_secs(10);

/// How often the hold checks whether the fleet has been adopted, to wake
/// the pass.
const POLL: Duration = Duration::from_millis(20);

/// Whether every feature a member reads at its open is decided in `view`.
pub(super) fn decided(view: FleetView<'_>) -> bool {
    OPEN_READS
        .iter()
        .all(|feature| view.decides(*feature).is_some())
}

/// The open pass's hold for the fleet, from the first pass it held a
/// member.
#[derive(Debug, Default)]
pub(super) struct FleetWait {
    since: Option<Instant>,
    gave_up: bool,
}

/// What [`FleetWait::check`] decided.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Hold {
    Open,
    /// The first hold: the caller starts what wakes the pass.
    Began,
    Held,
    /// [`FLEET_WAIT`] passed with the fleet still unknown; logged once.
    GaveUp,
}

impl FleetWait {
    pub(super) fn check(&mut self, decided: bool, now: Instant) -> Hold {
        if decided || self.gave_up {
            return Hold::Open;
        }
        match self.since {
            None => {
                self.since = Some(now);
                Hold::Began
            }
            Some(since) if now.duration_since(since) >= FLEET_WAIT => {
                self.gave_up = true;
                Hold::GaveUp
            }
            Some(_) => Hold::Held,
        }
    }
}

impl RaftShards {
    /// Whether members may open on the fleet as this broker knows it. A
    /// start that votes before its restore check does not wait: its
    /// members open to vote while the control plane is down.
    pub(super) fn fleet_known(&self) -> bool {
        if self
            .votes_unchecked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return true;
        }
        let hold = self
            .fleet_wait
            .lock()
            .check(decided(self.fleet.view()), Instant::now());
        match hold {
            Hold::Open => true,
            Hold::Held => false,
            Hold::Began => {
                tracing::info!(
                    "Raft members wait for this broker's registration to answer which fleet features are enabled"
                );
                self.wake_when_fleet_known();
                false
            }
            Hold::GaveUp => {
                tracing::warn!(
                    waited = ?FLEET_WAIT,
                    "the fleet's features are still unknown; Raft members open as if none were \
                     enabled, and each keeps that until it opens again"
                );
                true
            }
        }
    }

    /// Wake the open pass once the fleet is decided, or once the hold gives
    /// up. The pass also runs on its own timer; this only saves the wait.
    fn wake_when_fleet_known(&self) {
        let (Some(wake), Ok(handle)) = (
            self.leading.wake.get().cloned(),
            tokio::runtime::Handle::try_current(),
        ) else {
            return;
        };
        let fleet = std::sync::Arc::clone(&self.fleet);
        let until = Instant::now() + FLEET_WAIT;
        handle.spawn(async move {
            while !decided(fleet.view()) && Instant::now() < until {
                tokio::time::sleep(POLL).await;
            }
            wake.notify_one();
        });
    }
}
