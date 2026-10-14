//! Keeping a change-feed cursor from moving backwards.
//!
//! On Raft each control-plane member answers from its own applied state, so a
//! broker that reaches a lagging member can get a `next_seq` below the cursor
//! it already holds, or a snapshot older than what it has applied. Taking
//! either would replay changes it has already applied, or roll its state back.
//! The same answer also comes from a control plane whose sequence really did
//! start over, such as one on the memory store after a restart, or a restore.
//! Only time tells them apart: a lagging member catches up, a reset does not.
use std::time::Duration;

use tokio::time::Instant;

/// How long a feed has to answer only from behind the cursor before the
/// broker takes it as a reset rather than a lagging member.
pub(crate) const RESET_AFTER: Duration = Duration::from_secs(60);

/// What to do with one answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// At or past the cursor: apply it.
    Fresh,
    /// Behind the cursor: ignore it and ask again later.
    Stale,
    /// Behind the cursor for [`RESET_AFTER`]: the feed was reset, so start
    /// over from what it says.
    Reset,
}

/// Tracks how long one feed has answered from behind its cursor.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BehindCursor {
    since: Option<Instant>,
}

impl BehindCursor {
    /// Judge an answer that is (`behind`) or is not behind the cursor.
    pub(crate) fn judge(&mut self, behind: bool, now: Instant) -> Verdict {
        if !behind {
            self.since = None;
            return Verdict::Fresh;
        }
        let since = *self.since.get_or_insert(now);
        if now.saturating_duration_since(since) >= RESET_AFTER {
            self.since = None;
            Verdict::Reset
        } else {
            Verdict::Stale
        }
    }
}

#[cfg(test)]
mod tests;
