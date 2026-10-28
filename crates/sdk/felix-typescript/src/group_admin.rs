//! Group administration: the typed answers `groupCreate`, `groupSeek` and
//! `groupDescribe` resolve with.

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types::or_null;

/// Where `groupSeek` or `groupCreate` left a group on one shard.
#[napi(object)]
pub struct GroupPosition {
    /// The offset the group resumes from.
    pub offset: BigInt,
    /// False when `groupCreate` found the group already there and left it
    /// alone.
    pub moved: bool,
}

impl From<felix_client::GroupPosition> for GroupPosition {
    fn from(position: felix_client::GroupPosition) -> Self {
        Self {
            offset: BigInt::from(position.offset),
            moved: position.moved,
        }
    }
}

/// Where a group stands on one shard, from `groupDescribe`.
#[napi(object)]
pub struct GroupInfo {
    /// Everything below this is finished. Null for a group with no cursor on
    /// the shard, which starts at the beginning of the log when polled.
    pub committed: Either<BigInt, Null>,
    /// The shard's committed tail.
    pub tail: BigInt,
    /// Records handed out and not yet settled. The shard leader's memory, so
    /// it restarts from zero when the leader changes.
    pub in_flight: BigInt,
    /// Records owed again after a nack or a lapsed claim. Leader memory, like
    /// `inFlight`.
    pub owed: BigInt,
    /// Records the group gave up on.
    pub dead_letters: BigInt,
    /// Records published to the shard that the group has not finished.
    pub lag: BigInt,
}

impl From<felix_client::GroupInfo> for GroupInfo {
    fn from(info: felix_client::GroupInfo) -> Self {
        Self {
            committed: or_null(info.committed.map(BigInt::from)),
            tail: BigInt::from(info.tail),
            in_flight: BigInt::from(info.in_flight),
            owed: BigInt::from(info.owed),
            dead_letters: BigInt::from(info.dead_letters),
            lag: BigInt::from(info.lag()),
        }
    }
}
