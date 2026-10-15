//! Stream model definitions and patch/change payloads.
//!
//! Defines stream identifiers, configuration fields, and change-log payloads
//! used by the control-plane store and API handlers.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct Stream {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub kind: StreamKind,
    pub shards: u32,
    /// How many brokers hold a copy of each shard, leader included.
    ///
    /// `1` is leader-only and is the default, so a stream created before this
    /// existed — or by a caller that does not set it — behaves exactly as it did.
    /// A `Quorum` stream needs at least 3 for a majority to mean anything.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub consistency: ConsistencyLevel,
    pub delivery: DeliveryGuarantee,
    pub durable: bool,
    /// The region this stream's data belongs to. When set, placement puts its
    /// leader and every replica only in this region or in one it has a bridge
    /// to (`FELIX_REGION_BRIDGES`). `None` places it anywhere, as streams
    /// always were. Fixed at creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// How routing keys map to shards. Fixed at creation: remapping a live
    /// stream would move keys between shards and break per-key order.
    /// Absent is `modulo`, the mapping every stream had before there was a
    /// choice.
    #[serde(default, skip_serializing_if = "StreamRouting::is_modulo")]
    pub routing: StreamRouting,
    /// Which creation of this name the stream is, given when it is created.
    /// A stream deleted and created again under the same name gets a new one,
    /// and brokers keep each one's records apart. 0 is a stream created
    /// before the fleet finalized `stream_incarnations`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub incarnation: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// The payload of a single stream's `Deleted` change: the stream, when it has
/// an incarnation. What brokers act on is the change's `deleted_through`.
///
/// None for incarnation 0, as before incarnations, so Raft members on
/// different builds record the same change.
pub(crate) fn deleted_stream_payload(stream: Stream) -> Option<Stream> {
    (stream.incarnation != 0).then_some(stream)
}

/// The incarnation a stream created once incarnations are finalized gets:
/// the `proposed` one (the creating request's clock, in microseconds), raised
/// above the name's tombstone. Above every earlier incarnation of the name
/// either way, and above any given before a restore of the control plane to
/// an earlier point, which rewinds the tombstones but not the clock. Never 0.
pub(crate) fn next_incarnation(proposed: u64, deleted_through: Option<u64>) -> u64 {
    proposed.max(deleted_through.map_or(1, |deleted| deleted.saturating_add(1)))
}

/// A proposal for [`next_incarnation`]: this clock in microseconds, below
/// 2^53 so it reads back exactly as a JSON number anywhere.
pub(crate) fn proposed_incarnation() -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        .unwrap_or(1);
    micros.clamp(1, (1 << 53) - 1)
}

/// How a stream maps routing keys to shards. The names are the ones
/// `felix_wire::routing::ShardRouting` uses, since brokers and clients read
/// this value as that type.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamRouting {
    /// `hash(key) % shards`.
    #[default]
    Modulo,
    /// Jump consistent hashing. Only given to a stream created after the
    /// `jump_hash_routing` fleet feature was finalized.
    JumpHash,
}

impl StreamRouting {
    pub fn is_modulo(&self) -> bool {
        *self == Self::Modulo
    }

    /// The name it is stored and sent under.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Modulo => "modulo",
            Self::JumpHash => "jump_hash",
        }
    }

    /// The reverse of [`Self::as_str`].
    pub fn parse(value: &str) -> Option<Self> {
        [Self::Modulo, Self::JumpHash]
            .into_iter()
            .find(|routing| routing.as_str() == value)
    }
}

/// Leader-only. The value a stream has unless it asks for more, and the value
/// every stream written before replication existed reads back as.
pub(crate) fn default_replication_factor() -> u32 {
    1
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamChange {
    pub seq: u64,
    pub op: StreamChangeOp,
    pub key: StreamKey,
    pub stream: Option<Stream>,
    /// On a `Deleted` change written once the fleet finalized
    /// `stream_incarnations`: every incarnation of the name up to and
    /// including this one is deleted, so brokers delete their data. Absent
    /// before the finalize, when a recreate at incarnation 0 may still reopen
    /// the deleted stream's logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_through: Option<u64>,
}

/// A deleted stream name: every incarnation of it up to and including
/// `incarnation` is deleted. Kept for good, one per name, and served with the
/// stream snapshot, so a broker that missed the delete deletes the data.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct StreamTombstone {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub incarnation: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamPatchRequest {
    pub retention: Option<RetentionPolicy>,
    pub consistency: Option<ConsistencyLevel>,
    pub delivery: Option<DeliveryGuarantee>,
    pub durable: Option<bool>,
}

/// How long, and how much of, a durable stream's log each broker keeps. A
/// bound left unset is the broker's own (`FELIX_DURABLE_RETENTION_SECONDS`,
/// `FELIX_DURABLE_RETENTION_BYTES`).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub max_age_seconds: Option<u64>,
    pub max_size_bytes: Option<u64>,
}

impl RetentionPolicy {
    /// Zero would be a bound no log can meet, since the segment being written
    /// is never deleted. Past `i64::MAX` the store cannot hold it.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("max_age_seconds", self.max_age_seconds),
            ("max_size_bytes", self.max_size_bytes),
        ] {
            match value {
                Some(0) => {
                    return Err(format!(
                        "retention.{name} must be greater than zero; omit it for no bound"
                    ));
                }
                Some(value) if value > i64::MAX as u64 => {
                    return Err(format!("retention.{name} is too large"));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub enum StreamKind {
    Stream,
    Queue,
    Cache,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub enum ConsistencyLevel {
    Leader,
    Quorum,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
}

#[cfg(test)]
mod tests;
