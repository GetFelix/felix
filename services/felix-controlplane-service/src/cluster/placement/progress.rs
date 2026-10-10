//! Whether a restore's new copy is still moving, as this instance has watched
//! it across passes.
//!
//! A report is one instant, and one that leaves the copy out (a batch that did
//! not land) says nothing about whether it will move again. Only a position
//! seen over several passes does, and the store does not keep one: writing it
//! there would start a new generation every pass. So each instance remembers
//! what it saw. One without the history, after a restart or a lease change,
//! starts the window again, which delays giving up but never gives up a copy
//! that is moving.
use std::collections::HashMap;

use super::CaughtUp;
use crate::model::{MoveReason, ShardAssignment, ShardKey};

/// Restores in flight, by shard, new copy and generation: the copy's last
/// reported position, and when this instance saw it change.
#[derive(Debug, Default)]
pub(super) struct CopyProgress {
    seen: HashMap<(ShardKey, String, u64), Seen>,
}

#[derive(Debug, Clone, Copy)]
struct Seen {
    offset: Option<u64>,
    at_millis: u64,
}

impl CopyProgress {
    /// Note where every restore's new copy is in `caught_up`, and answer when
    /// each was last seen to move. Restores no longer in `existing` are
    /// forgotten.
    pub(super) fn observe(
        &mut self,
        existing: &[ShardAssignment],
        caught_up: &dyn CaughtUp,
    ) -> HashMap<(ShardKey, String), u64> {
        let Some(now) = caught_up.as_of_millis() else {
            return HashMap::new();
        };
        let mut seen = HashMap::new();
        for assignment in existing {
            let Some(joining) = assignment.joining.as_deref() else {
                continue;
            };
            if assignment.move_reason != Some(MoveReason::Restore) {
                continue;
            }
            // Offsets are only comparable within one leader's generation.
            let offset = (caught_up.reported_generation(&assignment.key)
                == Some(assignment.generation))
            .then(|| caught_up.reported_offset(&assignment.key, joining))
            .flatten();
            let id = (
                assignment.key.clone(),
                joining.to_string(),
                assignment.generation,
            );
            let entry = match self.seen.remove(&id) {
                // Left out of the report, or no further on: not moving.
                Some(last) if offset.is_none() || offset <= last.offset => last,
                _ => Seen {
                    offset,
                    at_millis: now,
                },
            };
            seen.insert(id, entry);
        }
        self.seen = seen;
        self.seen
            .iter()
            .map(|((key, joining, _), seen)| ((key.clone(), joining.clone()), seen.at_millis))
            .collect()
    }
}
