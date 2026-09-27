//! Replication: the leader ships committed records to the followers of every
//! shard it leads ([`driver`], [`ship_once`]), and a follower stores them
//! ([`replica`]).
//!
//! # One cursor per follower
//!
//! A follower is a position in this shard's log, nothing more. The leader keeps
//! the next offset it believes each follower wants, reads that range from its
//! own log, and ships it. The follower's answer is what moves the cursor —
//! never the send itself, because a batch that was sent is not a batch that was
//! stored.
//!
//! That is why the cursor can go *backwards*. A follower that has lost records,
//! or that was rebuilt, answers `LogGap` naming the offset it actually wants,
//! and the leader resumes there. Resume needs no separate negotiation and no
//! state on disk: the follower is the authority on its own position, and it
//! says so in every refusal.
//!
//! # What stops, and what retries
//!
//! `LogConflict` and `FencedEpoch` stop this shard's replication to that
//! follower. Neither converges by trying again: the first means the two logs
//! disagree about bytes already stored, and the second means this broker is no
//! longer the leader, so it has no business shipping anything. Everything else
//! is transient and retried.
//!
//! # What bounds it
//!
//! One batch in flight per follower, and each batch is bounded by
//! `max_batch_bytes` read from the log. So the memory a lagging follower costs
//! the leader is one batch, not the distance it is behind — and a follower that
//! stops answering stops consuming anything at all, because the next read does
//! not start until the last answer arrives.

pub mod driver;
mod follower;
pub mod halted;
pub mod metrics;
pub mod quorum;
mod rebuild;
pub mod replica;
pub mod reporter;
mod ship;
mod throttle;

pub use follower::{CATCH_UP_BOUND, FollowerCursor, Halt, caught_up, lag_records};
pub use quorum::{majority_of, quorum_offset, quorum_offset_without};
pub use rebuild::{RebuildPolicy, Rebuilds};
pub use replica::ReplicaHandler;
pub use ship::{Progress, read_answer, ship_once, ship_once_with};
pub use throttle::MoveThrottle;

#[cfg(test)]
mod tests;
