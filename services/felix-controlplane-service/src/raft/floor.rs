//! The block a member without its vote holds past the group's failure
//! tolerance, and the operator's waiver that lifts it.
//!
//! A member that lost its volume serves leaders only from a floor it learns
//! from the other members' terms ([`super::RaftHandle::serves_leader`]). The
//! floor is proven only while every majority holding this member also holds
//! a member that reported a term. When the rest of such a majority lost
//! their volumes too, nobody left can prove the terms this member promised,
//! and serving a leader could lose an acknowledged write. The member blocks
//! until enough members report a term, or an operator names the lost members
//! and accepts the loss. See `docs/metadata-raft-design.md` ("Past the
//! failure tolerance").
use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use super::NodeId;

/// How long a member blocks before it reports the block as an error and
/// accepts a waiver.
pub(crate) const DEFAULT_REPORT_AFTER: Duration = Duration::from_secs(60);

/// Whether members that reported a term prove the floor: every majority of a
/// group of `members` that holds this member holds one of the `with_terms`
/// reporters.
pub(super) fn proven(members: usize, with_terms: usize) -> bool {
    members.saturating_sub(with_terms) < members / 2 + 1
}

/// What [`FloorGate::judge`] decided.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    /// The floor is proven.
    Proven,
    /// Not proven, and an operator accepted these members as lost.
    Waived(Waiver),
    /// Not proven. `report` is true the first time the block outlasts the
    /// report timeout.
    Blocked { report: bool },
}

/// An operator's word that `members` lost their volumes for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Waiver {
    pub(super) members: BTreeSet<NodeId>,
    pub(super) reason: String,
}

/// The current block, as readiness and the waiver route see it.
#[derive(Debug, Clone)]
pub(super) struct Blocked {
    since: Instant,
    /// The other members that said they withhold their vote.
    pub(super) withheld: BTreeSet<NodeId>,
    pub(super) with_terms: usize,
    pub(super) members: usize,
    /// The block outlasted the report timeout.
    pub(super) reported: bool,
}

#[derive(Debug, Default)]
struct State {
    blocked: Option<Blocked>,
    waiver: Option<Waiver>,
}

/// Shared between the floor loop, readiness, and the waiver route.
#[derive(Debug, Default)]
pub(super) struct FloorGate {
    state: Mutex<State>,
}

impl FloorGate {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Judge one round in which every other member answered. A waiver is
    /// used at most once, and only if it names every withheld member.
    pub(super) fn judge(
        &self,
        members: usize,
        withheld: &BTreeSet<NodeId>,
        with_terms: usize,
        report_after: Duration,
    ) -> Verdict {
        let mut state = self.state();
        if proven(members, with_terms) {
            *state = State::default();
            metrics::gauge!("felix_meta_raft_floor_blocked").set(0.0);
            return Verdict::Proven;
        }
        if let Some(waiver) = state
            .waiver
            .take_if(|waiver| withheld.is_subset(&waiver.members))
        {
            *state = State::default();
            metrics::gauge!("felix_meta_raft_floor_blocked").set(0.0);
            return Verdict::Waived(waiver);
        }
        let since = state
            .blocked
            .as_ref()
            .map_or_else(Instant::now, |b| b.since);
        let was_reported = state.blocked.as_ref().is_some_and(|b| b.reported);
        let reported = was_reported || since.elapsed() >= report_after;
        state.blocked = Some(Blocked {
            since,
            withheld: withheld.clone(),
            with_terms,
            members,
            reported,
        });
        if reported {
            metrics::gauge!("felix_meta_raft_floor_blocked").set(1.0);
        }
        Verdict::Blocked {
            report: reported && !was_reported,
        }
    }

    /// The block, once it has outlasted the report timeout.
    pub(super) fn reported(&self) -> Option<Blocked> {
        self.state().blocked.clone().filter(|b| b.reported)
    }

    /// Record a waiver for the current block. Refused unless the block has
    /// been reported and `waiver` names every member it waits on, so a
    /// waiver never outlives the block it was given for or covers a member
    /// the operator did not name.
    pub(super) fn waive(&self, waiver: Waiver) -> Result<(), String> {
        if waiver.reason.trim().is_empty() {
            return Err("give a reason: it is logged with the waiver".to_string());
        }
        let mut state = self.state();
        let Some(blocked) = state.blocked.as_ref().filter(|b| b.reported) else {
            return Err("this member is not blocked past the failure tolerance".to_string());
        };
        if !blocked.withheld.is_subset(&waiver.members) {
            return Err(format!(
                "the block waits on members {:?}: name each of them",
                blocked.withheld
            ));
        }
        state.waiver = Some(waiver);
        Ok(())
    }
}
