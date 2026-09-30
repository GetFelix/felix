//! Shipping, against a follower that answers on command and a real log.
//!
//! The subject is the cursor: where the leader believes a follower is, and what
//! is allowed to move that belief. A real cluster produces the interesting
//! answers rarely and never on demand, so they come from a script.

mod answers;
mod commit_offset;
mod eligibility;
mod lag;
mod quorum;
mod rebuild;
mod shipping;
mod trimmed_history;

use std::net::SocketAddr;
use std::sync::Mutex;

use bytes::Bytes;
use felix_broker::DurableStorage;
use felix_broker::StreamLog;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{
    ErrorCode, InternalMessage, ReplicaLog, ReplicateRecords, ShardRef, batch_checksum,
};
use felix_wire::internal::{ReplicateError, ReplicateOk};
use tempfile::TempDir;

use super::rebuild::*;
use super::ship::*;
use super::*;
use crate::peer::{PeerError, PeerRequester};

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const GENERATION: u64 = 4;

/// A follower that answers from a script and records what it was sent.
struct ScriptedFollower {
    answers: Mutex<std::collections::VecDeque<std::result::Result<InternalMessage, PeerError>>>,
    sent: Mutex<Vec<ReplicateRecords>>,
    /// Base offsets this follower was offered, as distinct from records sent.
    offered: Mutex<Vec<u64>>,
    /// Base offsets this follower was told to rebuild from.
    rebuilds: Mutex<Vec<u64>>,
}

impl ScriptedFollower {
    fn new(
        answers: impl IntoIterator<Item = std::result::Result<InternalMessage, PeerError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            sent: Mutex::new(Vec::new()),
            offered: Mutex::new(Vec::new()),
            rebuilds: Mutex::new(Vec::new()),
        }
    }

    /// Base offsets this follower was offered.
    fn offered(&self) -> Vec<u64> {
        self.offered.lock().expect("lock").clone()
    }

    /// Base offsets this follower was told to rebuild from.
    fn rebuilds(&self) -> Vec<u64> {
        self.rebuilds.lock().expect("lock").clone()
    }

    /// The offsets and payloads of every batch this follower was sent.
    fn sent(&self) -> Vec<(u64, Vec<String>)> {
        self.sent
            .lock()
            .expect("lock")
            .iter()
            .map(|batch| {
                (
                    batch.first_offset,
                    batch
                        .payloads
                        .iter()
                        .map(|p| String::from_utf8(p.to_vec()).expect("utf8"))
                        .collect(),
                )
            })
            .collect()
    }
}

impl PeerRequester for ScriptedFollower {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        match message {
            InternalMessage::ReplicateRecords(batch) => self.sent.lock().expect("lock").push(batch),
            InternalMessage::ReplicateBootstrap(request) => {
                self.offered.lock().expect("lock").push(request.base_offset)
            }
            InternalMessage::ReplicateRebuild(request) => {
                assert_eq!(request.log, ReplicaLog::Stream);
                self.rebuilds
                    .lock()
                    .expect("lock")
                    .push(request.base_offset)
            }
            other => panic!("shipping sent a {:?}", other.kind()),
        }
        self.answers
            .lock()
            .expect("lock")
            .pop_front()
            .unwrap_or(Err(PeerError::Unavailable {
                node_id: "broker-b".to_string(),
                detail: "the script ran out".to_string(),
            }))
    }
}

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        generation: GENERATION,
    }
}

fn cursor(next_offset: u64) -> FollowerCursor {
    FollowerCursor::new(
        "broker-b",
        "10.0.0.4:7002".parse().expect("addr"),
        next_offset,
    )
}

/// A follower that has answered, at this generation, that it holds the log up
/// to `offset`.
fn answered(offset: u64) -> FollowerCursor {
    let mut cursor = cursor(offset);
    cursor.confirmed = offset;
    cursor
}

fn stored(durable_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateOk(ReplicateOk {
        correlation_id: 0,
        durable_offset,
    })
}

fn refused(code: ErrorCode, expected_offset: u64) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id: 0,
        code,
        expected_offset,
        detail: "no".to_string(),
    })
}

/// A leader's log holding `values` at offsets 0..n.
async fn leader_log(values: &[&str]) -> (StreamLog, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for value in values {
        log.append(&[Bytes::copy_from_slice(value.as_bytes())])
            .await
            .expect("append");
    }
    (log, dir)
}

const BATCH_BYTES: usize = 1024 * 1024;
/// Small enough that a batch carries one record, so a cursor that jumped is
/// visible as a gap in what was sent rather than needing a size calculation.
const ONE_RECORD_BYTES: usize = 1;

fn vec_of(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}
