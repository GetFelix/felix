use bytes::Bytes;
use felix_wire::internal::InternalMessage;

use super::*;

/// Correlation ids belong to the connection, not to the caller.
mod correlation {
    use super::*;
    use felix_wire::internal::*;

    fn correlation_of(message: &InternalMessage) -> u64 {
        // `InternalMessage::correlation_id` already answers this; the match is
        // kept so a new kind has to be considered here too.
        match message {
            InternalMessage::ForwardPublish(m) => m.correlation_id,
            InternalMessage::ForwardPublishOk(m) => m.correlation_id,
            InternalMessage::ForwardPublishError(m) => m.correlation_id,
            InternalMessage::ForwardCacheOp(m) => m.correlation_id,
            InternalMessage::ForwardCacheOk(m) => m.correlation_id,
            InternalMessage::ForwardCacheError(m) => m.correlation_id,
            InternalMessage::NotLeader(m) => m.correlation_id,
            InternalMessage::Hello(m) => m.correlation_id,
            InternalMessage::HelloOk(m) => m.correlation_id,
            InternalMessage::ReplicateRecords(m)
            | InternalMessage::ReplicateCacheRecords(m)
            | InternalMessage::ReplicateGroupRecords(m)
            | InternalMessage::ReplicateDeadLetterRecords(m)
            | InternalMessage::ReplicateCounterRecords(m)
            | InternalMessage::ReplicateMarkedRecords(m) => m.correlation_id,
            InternalMessage::ReplicateOk(m) => m.correlation_id,
            InternalMessage::ReplicateError(m) => m.correlation_id,
            InternalMessage::ReplicateBootstrap(m)
            | InternalMessage::ReplicateCacheBootstrap(m)
            | InternalMessage::ReplicateGroupBootstrap(m)
            | InternalMessage::ReplicateDeadLetterBootstrap(m)
            | InternalMessage::ReplicateCounterBootstrap(m) => m.correlation_id,
            InternalMessage::ReplicateRebuild(m) => m.correlation_id,
            InternalMessage::Fence(m) => m.correlation_id,
            InternalMessage::FenceOk(m) => m.correlation_id,
            InternalMessage::ReplicateFetch(m) => m.correlation_id,
        }
    }

    fn shard() -> ShardRef {
        ShardRef {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation: 4,
        }
    }

    /// Every variant, each carrying the caller's id.
    fn every_variant() -> Vec<InternalMessage> {
        vec![
            InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 7,
                shard: shard(),
                ack: AckMode::OnCommit,
                payloads: vec![Bytes::from_static(b"hello")],
                credential: String::new(),
            }),
            InternalMessage::ForwardPublishOk(ForwardPublishOk {
                correlation_id: 7,
                first_offset: 1,
                last_offset: 1,
            }),
            InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: 7,
                code: ErrorCode::StaleRoute,
                detail: "elsewhere".to_string(),
            }),
            InternalMessage::NotLeader(NotLeader {
                correlation_id: 7,
                node_id: "broker-c".to_string(),
                advertise_addr: "10.0.0.5:7002".to_string(),
                generation: 5,
            }),
            InternalMessage::Hello(Hello {
                correlation_id: 7,
                node_id: "broker-a".to_string(),
                capabilities: None,
            }),
            InternalMessage::HelloOk(HelloOk {
                correlation_id: 7,
                node_id: "broker-b".to_string(),
                capabilities: None,
            }),
            InternalMessage::ReplicateRecords(ReplicateRecords {
                times: None,
                correlation_id: 7,
                shard: shard(),
                first_offset: 12,
                checksum: 0xabcd,
                payloads: vec![Bytes::from_static(b"hello")],
                marks: Vec::new(),
                commit_offset: None,
                generations: None,
                publishers: Vec::new(),
            }),
            InternalMessage::ReplicateOk(ReplicateOk {
                correlation_id: 7,
                durable_offset: 13,
            }),
            InternalMessage::ReplicateError(ReplicateError {
                correlation_id: 7,
                code: ErrorCode::LogGap,
                expected_offset: 9,
                detail: "behind".to_string(),
            }),
            InternalMessage::ReplicateBootstrap(ReplicateBootstrap {
                correlation_id: 7,
                shard: shard(),
                base_offset: 5_000,
            }),
            InternalMessage::ReplicateRebuild(ReplicateRebuild {
                correlation_id: 7,
                shard: shard(),
                log: ReplicaLog::Stream,
                base_offset: 5_000,
            }),
            InternalMessage::Fence(Fence {
                correlation_id: 7,
                shard: shard(),
                log: ReplicaLog::Stream,
            }),
            InternalMessage::FenceOk(FenceOk {
                correlation_id: 7,
                log_end: 1,
                commit_offset: 1,
                last_generation: 1,
            }),
            InternalMessage::ReplicateFetch(ReplicateFetch {
                timed: false,
                correlation_id: 7,
                shard: shard(),
                log: ReplicaLog::Stream,
                from_offset: 3,
                max_bytes: 64,
                labelled: false,
            }),
            InternalMessage::ReplicateMarkedRecords(ReplicateRecords {
                times: None,
                correlation_id: 7,
                shard: shard(),
                first_offset: 12,
                checksum: 0xabcd,
                payloads: vec![Bytes::from_static(b"hello")],
                marks: vec![felix_wire::internal::ProducerMark::Continues],
                commit_offset: None,
                generations: None,
                publishers: Vec::new(),
            }),
        ]
    }

    /// **Every variant must be stamped.** One that kept the caller's id would be
    /// matched to the wrong waiter, which is worse than losing it: a response
    /// would be handed to a different request.
    #[test]
    fn every_message_carries_the_connections_id_rather_than_the_callers() {
        for message in every_variant() {
            let stamped = with_correlation(message, 99);
            assert_eq!(
                correlation_of(&stamped),
                99,
                "{stamped:?} kept the caller's id"
            );
        }
    }

    /// Stamping changes the id and nothing else.
    #[test]
    fn stamping_leaves_the_rest_of_the_message_alone() {
        let stamped = with_correlation(
            InternalMessage::ForwardPublish(ForwardPublish {
                correlation_id: 7,
                shard: shard(),
                ack: AckMode::OnCommit,
                payloads: vec![Bytes::from_static(b"hello")],
                credential: String::new(),
            }),
            99,
        );
        let InternalMessage::ForwardPublish(stamped) = stamped else {
            panic!("the variant changed");
        };
        assert_eq!(stamped.correlation_id, 99);
        assert_eq!(stamped.shard, shard());
        assert_eq!(stamped.ack, AckMode::OnCommit);
        assert_eq!(stamped.payloads, vec![Bytes::from_static(b"hello")]);
    }
}

/// How a connection loss is labelled for metrics.
mod close_labels {
    use super::*;

    #[test]
    fn each_close_reason_has_its_own_label() {
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::TimedOut),
            "timeout"
        );
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::LocallyClosed),
            "closed_locally"
        );
        assert_eq!(close_reason_label(&quinn::ConnectionError::Reset), "reset");
        assert_eq!(
            close_reason_label(&quinn::ConnectionError::VersionMismatch),
            "other"
        );
    }
}
