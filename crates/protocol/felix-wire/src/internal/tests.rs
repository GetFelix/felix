//! Round-trips, byte-exact golden vectors, and malformed input.
//!
//! The malformed cases matter most. A peer is authenticated, not assumed
//! correct: every one of these must be an error, and none may panic.

mod codec;
mod error_code;
mod fence;
mod forward;
mod framing;
mod golden;
mod replicate;

use bytes::{BufMut, Bytes, BytesMut};

use super::*;
use crate::error::Error;

fn shard() -> ShardRef {
    ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        generation: 7,
    }
}

/// The legacy kind: no credential. Kept as a fixture because its layout is
/// frozen and the golden vector below pins it.
fn forward() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 42,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")],
        credential: String::new(),
    })
}

fn authorized_forward() -> InternalMessage {
    InternalMessage::ForwardPublish(ForwardPublish {
        correlation_id: 42,
        shard: shard(),
        ack: AckMode::OnCommit,
        payloads: vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")],
        credential: "eyJ.token.sig".to_string(),
    })
}

fn every_message() -> Vec<InternalMessage> {
    vec![
        forward(),
        authorized_forward(),
        InternalMessage::ForwardPublishOk(ForwardPublishOk {
            correlation_id: 42,
            first_offset: 100,
            last_offset: 101,
        }),
        InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: 42,
            code: ErrorCode::StaleRoute,
            detail: "generation 7 is ahead of mine".to_string(),
        }),
        InternalMessage::NotLeader(NotLeader {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
            advertise_addr: "10.0.0.5:7000".to_string(),
            generation: 9,
        }),
        InternalMessage::Hello(Hello {
            correlation_id: 42,
            node_id: "broker-a".to_string(),
            capabilities: None,
        }),
        InternalMessage::HelloOk(HelloOk {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
            capabilities: None,
        }),
        InternalMessage::Hello(Hello {
            correlation_id: 42,
            node_id: "broker-a".to_string(),
            capabilities: Some(PeerCapabilities::FENCE),
        }),
        InternalMessage::HelloOk(HelloOk {
            correlation_id: 42,
            node_id: "broker-b".to_string(),
            capabilities: Some(PeerCapabilities::NONE),
        }),
        InternalMessage::Fence(Fence {
            correlation_id: 42,
            shard: shard(),
            log: ReplicaLog::Stream,
        }),
        InternalMessage::FenceOk(FenceOk {
            correlation_id: 42,
            log_end: 120,
            commit_offset: 100,
            last_generation: 6,
        }),
        InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 42,
            shard: shard(),
            log: ReplicaLog::Stream,
            from_offset: 100,
            max_bytes: 1 << 20,
        }),
        replicate(),
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 42,
            durable_offset: 102,
        }),
        InternalMessage::ReplicateError(ReplicateError {
            correlation_id: 42,
            code: ErrorCode::LogGap,
            expected_offset: 100,
            detail: "batch starts at 105, expected 100".to_string(),
        }),
        InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ReplicateRebuild(ReplicateRebuild {
            correlation_id: 42,
            shard: shard(),
            log: ReplicaLog::Counters,
            base_offset: 5_000,
        }),
        InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 42,
            shard: shard(),
            op: CacheOpKind::Put,
            key: "session:abc".to_string(),
            value: Bytes::from_static(b"payload"),
            ttl_ms: 30_000,
            credential: String::new(),
        }),
        InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 42,
            shard: shard(),
            op: CacheOpKind::Put,
            key: "session:abc".to_string(),
            value: Bytes::from_static(b"payload"),
            ttl_ms: 30_000,
            credential: "eyJ.token.sig".to_string(),
        }),
        InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 42,
            shard: shard(),
            op: CacheOpKind::Get,
            key: "session:abc".to_string(),
            value: Bytes::new(),
            ttl_ms: 0,
            credential: String::new(),
        }),
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id: 42,
            value: Some(Bytes::from_static(b"payload")),
        }),
        InternalMessage::ForwardCacheOk(ForwardCacheOk {
            correlation_id: 42,
            value: None,
        }),
        InternalMessage::ForwardCacheError(ForwardCacheError {
            correlation_id: 42,
            code: ErrorCode::Unavailable,
            detail: "cache scope not found".to_string(),
        }),
        InternalMessage::ReplicateCacheRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")],
            marks: Vec::new(),
            commit_offset: None,
        }),
        InternalMessage::ReplicateCacheBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ReplicateGroupRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"cursor")],
            marks: Vec::new(),
            commit_offset: None,
        }),
        InternalMessage::ReplicateGroupBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ReplicateDeadLetterRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"dead letter")],
            marks: Vec::new(),
            commit_offset: None,
        }),
        InternalMessage::ReplicateDeadLetterBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ReplicateCounterRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"delta")],
            marks: Vec::new(),
            commit_offset: None,
        }),
        InternalMessage::ReplicateCounterBootstrap(ReplicateBootstrap {
            correlation_id: 42,
            shard: shard(),
            base_offset: 5_000,
        }),
        InternalMessage::ReplicateMarkedRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![
                Bytes::from_static(b"plain"),
                Bytes::from_static(b"first"),
                Bytes::from_static(b"second"),
            ],
            marks: vec![
                ProducerMark::None,
                ProducerMark::Opens {
                    producer_id: u64::MAX,
                    sequence: 9,
                    len: 2,
                },
                ProducerMark::Continues,
            ],
            commit_offset: None,
        }),
        InternalMessage::ReplicateRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"a")],
            marks: Vec::new(),
            commit_offset: Some(90),
        }),
        InternalMessage::ReplicateMarkedRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 100,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"first")],
            marks: vec![ProducerMark::Opens {
                producer_id: 3,
                sequence: 1,
                len: 1,
            }],
            commit_offset: Some(100),
        }),
        InternalMessage::ReplicateCacheRecords(ReplicateRecords {
            correlation_id: 42,
            shard: shard(),
            first_offset: 7,
            checksum: 0x0102_0304,
            payloads: vec![Bytes::from_static(b"put")],
            marks: Vec::new(),
            commit_offset: Some(0),
        }),
    ]
}

fn replicate() -> InternalMessage {
    let payloads = vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")];
    InternalMessage::ReplicateRecords(ReplicateRecords {
        correlation_id: 42,
        shard: shard(),
        first_offset: 100,
        checksum: 0x0102_0304,
        payloads,
        marks: Vec::new(),
        commit_offset: None,
    })
}
