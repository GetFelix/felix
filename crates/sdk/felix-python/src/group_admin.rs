//! Group administration: the typed answers `group_create`, `group_seek` and
//! `group_describe` give back. See `docs/queues.md` for what a group's
//! position and counts mean.

use pyo3::prelude::*;

/// Where `group_seek` or `group_create` left a group on one shard.
#[pyclass(module = "felix", frozen, get_all)]
pub struct GroupPosition {
    /// The offset the group resumes from.
    pub offset: u64,
    /// False when `group_create` found the group already there and left it
    /// alone.
    pub moved: bool,
}

#[pymethods]
impl GroupPosition {
    fn __repr__(&self) -> String {
        format!(
            "GroupPosition(offset={}, moved={})",
            self.offset,
            if self.moved { "True" } else { "False" }
        )
    }
}

impl From<felix_client::GroupPosition> for GroupPosition {
    fn from(position: felix_client::GroupPosition) -> Self {
        Self {
            offset: position.offset,
            moved: position.moved,
        }
    }
}

/// Where a group stands on one shard, from `group_describe`.
#[pyclass(module = "felix", frozen, get_all)]
pub struct GroupInfo {
    /// Everything below this is finished. `None` for a group with no cursor
    /// on the shard, which starts at the beginning of the log when polled.
    pub committed: Option<u64>,
    /// The shard's committed tail.
    pub tail: u64,
    /// Records handed out and not yet settled. The shard leader's memory, so
    /// it restarts from zero when the leader changes.
    pub in_flight: u64,
    /// Records owed again after a nack or a lapsed claim. Leader memory, like
    /// `in_flight`.
    pub owed: u64,
    /// Records the group gave up on.
    pub dead_letters: u64,
}

#[pymethods]
impl GroupInfo {
    /// Records published to the shard that the group has not finished.
    #[getter]
    fn lag(&self) -> u64 {
        self.tail.saturating_sub(self.committed.unwrap_or(0))
    }

    fn __repr__(&self) -> String {
        format!(
            "GroupInfo(committed={:?}, tail={}, in_flight={}, owed={}, dead_letters={})",
            self.committed, self.tail, self.in_flight, self.owed, self.dead_letters
        )
    }
}

impl From<felix_client::GroupInfo> for GroupInfo {
    fn from(info: felix_client::GroupInfo) -> Self {
        Self {
            committed: info.committed,
            tail: info.tail,
            in_flight: info.in_flight,
            owed: info.owed,
            dead_letters: info.dead_letters,
        }
    }
}
