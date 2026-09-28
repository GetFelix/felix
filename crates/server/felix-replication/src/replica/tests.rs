//! The checks standing between a shipped batch and this broker's disk.
//!
//! These are about *authority* rather than position: whether the sender is
//! still the leader, and whether this broker is a replica at all. Position is
//! `felix_broker::replication`'s subject and is tested there.
use std::collections::HashMap;

use bytes::Bytes;
use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{ShardRef, batch_checksum};
use tempfile::TempDir;

use super::*;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const LOCAL: &str = "broker-b";

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn key() -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        kind: felix_router::ShardKind::Stream,
    }
}

/// A router in which `LOCAL` is a follower of the shard at `generation`, unless
/// `replicas` says otherwise.
fn router_with(replicas: &[&str], generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        ("broker-a".to_string(), node("broker-a", 7001)),
        (LOCAL.to_string(), node(LOCAL, 7002)),
        ("broker-c".to_string(), node("broker-c", 7003)),
    ]
    .into_iter()
    .collect();
    let table = RoutingTable::build(
        [(
            key(),
            "broker-a".to_string(),
            replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

fn broker_on(dir: &std::path::Path) -> Arc<Broker> {
    let storage = felix_broker::DurableStorage::open(
        dir,
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage))
}

async fn broker_with_storage() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    (broker_on(dir.path()), dir)
}

fn batch(generation: u64, first_offset: u64, values: &[&str]) -> ReplicateRecords {
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect();
    ReplicateRecords {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation,
        },
        first_offset,
        checksum: batch_checksum(&payloads, &[]),
        payloads,
        marks: Vec::new(),
        commit_offset: None,
    }
}

fn refusal(answer: &InternalMessage) -> &ReplicateError {
    match answer {
        InternalMessage::ReplicateError(err) => err,
        other => panic!("expected a refusal, got {:?}", other.kind()),
    }
}

/// A follower at the leader's epoch stores the batch and reports its durable
/// mark.
#[tokio::test]
async fn a_follower_at_the_leaders_epoch_stores_the_batch() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    let answer = handler
        .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
        .await;

    match answer {
        InternalMessage::ReplicateOk(ok) => {
            assert_eq!(ok.durable_offset, 2);
            assert_eq!(ok.correlation_id, 1);
        }
        other => panic!("expected an acknowledgement, got {:?}", other.kind()),
    }
}

/// **A superseded leader is refused.** Its records may have been written after
/// it lost the shard, and a follower that stored them would hold bytes no
/// current leader ever ordered. The fence does not lift, so this is not
/// retryable.
#[tokio::test]
async fn a_leader_at_an_older_epoch_is_fenced() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 5));

    let answer = handler
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::FencedEpoch);
    assert!(
        !refused.code.is_retryable(),
        "a fenced leader was told to retry"
    );
}

/// A follower whose watch is behind refuses *for now* — retryable, because the
/// assignment is on its way and the leader is not at fault.
#[tokio::test]
async fn a_follower_behind_the_epoch_refuses_retryably() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 3));

    let answer = handler
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleRoute);
    assert!(refused.code.is_retryable());
}

/// **A broker outside the replica set stores nothing.** Otherwise any peer
/// could place bytes on any broker's disk by naming a shard.
#[tokio::test]
async fn a_broker_outside_the_replica_set_is_refused() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&["broker-c"], 4));

    let answer = handler
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// A shard this broker has never heard of reads as "behind", not as a refusal:
/// the assignment may simply not have arrived, and refusing permanently would
/// strand a follower whose watch is a moment late.
#[tokio::test]
async fn an_unknown_shard_is_treated_as_a_late_watch() {
    let (broker, _dir) = broker_with_storage().await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let handler = ReplicaHandler::new(broker, router);

    let answer = handler
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::StaleRoute);
    assert!(refused.code.is_retryable());
}

/// A gap is reported with the offset the leader should resume from, so the
/// repair needs no separate negotiation.
#[tokio::test]
async fn a_gap_names_the_offset_to_resume_from() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));
    handler
        .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
        .await;

    let answer = handler
        .apply(batch(4, 7, &["h"]), felix_broker::LogKind::Stream)
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogGap);
    assert_eq!(refused.expected_offset, 2);
}

/// A conflict is reported as its own code, and is not retryable: two logs that
/// disagree do not converge by resending.
#[tokio::test]
async fn a_conflict_is_reported_as_divergence() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));
    handler
        .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
        .await;

    let answer = handler
        .apply(
            batch(4, 0, &["a", "DIFFERENT"]),
            felix_broker::LogKind::Stream,
        )
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogConflict);
    assert!(!refused.code.is_retryable());
}

/// **A replica with nowhere to put records says so.** Accepting and keeping
/// nothing would let the leader count this broker toward a quorum that does not
/// exist.
#[tokio::test]
async fn a_replica_without_durable_storage_refuses() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    let answer = handler
        .apply(batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
        .await;

    assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
}

/// The answer always carries the request's correlation id, whatever the
/// outcome. A requester that could not match a refusal would wait out its
/// timeout instead of acting on it.
#[tokio::test]
async fn every_answer_carries_the_requests_correlation_id() {
    let (broker, _dir) = broker_with_storage().await;
    let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

    for generation in [3, 4, 5] {
        let mut request = batch(generation, 0, &["a"]);
        request.correlation_id = 99;
        assert_eq!(
            handler
                .apply(request, felix_broker::LogKind::Stream)
                .await
                .correlation_id(),
            99
        );
    }
}

/// Placing a shard log where the leader's surviving log begins.
mod bootstrap {
    use super::*;
    use felix_wire::internal::ReplicateBootstrap;

    const BASE: u64 = 5_000;

    fn offer(generation: u64, base_offset: u64) -> ReplicateBootstrap {
        ReplicateBootstrap {
            correlation_id: 1,
            shard: ShardRef {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
                shard: 0,
                generation,
            },
            base_offset,
        }
    }

    /// **A replica with nothing takes the offer**, and reports that it now
    /// stands at the offered base — which is where the leader resumes.
    #[tokio::test]
    async fn a_replica_holding_nothing_places_its_log_at_the_offered_base() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, BASE),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        assert_eq!(log.base_offset(), BASE);
    }

    /// Records shipped after a bootstrap land at the leader's offsets, which is
    /// the whole point of placing the log rather than starting at zero.
    #[tokio::test]
    async fn records_after_a_bootstrap_land_at_the_leaders_offsets() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        let answer = handler
            .apply(batch(4, BASE, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, BASE + 2),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
    }

    /// **A replica holding records refuses.** Discarding them is an operator's
    /// decision, and a log placed over them would have a hole between what it
    /// held and what it was given.
    #[tokio::test]
    async fn a_replica_holding_records_refuses_to_be_rebased() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;

        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        let refused = refusal(&answer);
        assert_eq!(refused.code, ErrorCode::LogConflict);
        assert!(!refused.code.is_retryable());
    }

    /// **A base inside what this broker already holds is accepted.**
    ///
    /// Retention and compaction trim brokers at their own pace, so a leader
    /// offering from a base above this broker's is ordinary rather than a
    /// conflict. The two logs meet — everything from the offered base up to
    /// `tail` is here — so there is nothing to refuse, and refusing is how a
    /// replica set shrinks over successive failovers with no error to point at.
    #[tokio::test]
    async fn a_base_inside_what_is_held_is_accepted_and_resumes_at_the_tail() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(
                batch(4, 0, &["a", "b", "c", "d"]),
                felix_broker::LogKind::Stream,
            )
            .await;

        // The leader has trimmed below 2 and offers from there.
        let answer = handler
            .bootstrap(offer(4, 2), felix_broker::LogKind::Stream)
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(
                ok.durable_offset, 4,
                "the leader must resume at this broker's tail, not re-ship what it holds",
            ),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
    }

    /// A base *below* what this broker holds is a real hole, and still refused.
    ///
    /// The leader's records start before this broker's do, so everything
    /// between is on neither. Accepting would leave the replica set believing a
    /// follower holds a range it has never seen.
    #[tokio::test]
    async fn a_base_below_what_is_held_is_still_refused() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        // Place the log high, then offer from far below it.
        handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        let answer = handler
            .bootstrap(offer(4, 10), felix_broker::LogKind::Stream)
            .await;

        let refused = refusal(&answer);
        assert_eq!(refused.code, ErrorCode::LogConflict);
        assert!(
            refused.detail.contains("begin after"),
            "the refusal should say which way the gap runs: {}",
            refused.detail,
        );
    }

    /// Offering the same base twice is harmless: the second finds the log
    /// already placed there and agrees.
    #[tokio::test]
    async fn repeating_the_same_offer_is_harmless() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

        handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;
        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, BASE),
            other => panic!("a repeated offer was refused: {:?}", other.kind()),
        }
    }

    /// **The same fence applies to placing a log as to storing records.** A
    /// superseded leader must not be able to re-base a follower's shard, which
    /// would be a way round the check that guards the records themselves.
    #[tokio::test]
    async fn a_superseded_leader_cannot_place_a_log() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 5));

        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
    }

    /// And a broker outside the replica set cannot be given a shard at all.
    #[tokio::test]
    async fn a_broker_outside_the_replica_set_cannot_be_given_a_shard() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(broker, router_with(&["broker-c"], 4));

        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }

    /// A replica with nowhere to put records says so rather than accepting.
    #[tokio::test]
    async fn a_replica_without_durable_storage_refuses() {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        let handler = ReplicaHandler::new(broker, router_with(&[LOCAL], 4));

        let answer = handler
            .bootstrap(offer(4, BASE), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }
}

/// Dropping a divergent suffix, and refusing to when it is not one.
///
/// The repair #406 asks for. A follower holding an uncommitted record from a
/// leader that died rejoins instead of halting until an operator notices — but
/// only when the generation history says the divergence is inside the
/// generation it last accepted. Without that bound, truncating on a conflict
/// would discard records nothing has established are safe to lose.
mod divergence {
    use super::*;

    /// Advance the router to `generation`, the way a reassignment would.
    ///
    /// `check_role` fences a batch whose generation is behind the router's, so
    /// a follower cannot be walked through a leadership change without this.
    fn advance_to(router: &ShardRouter, generation: u64) {
        let nodes: HashMap<String, NodeRef> = [
            ("broker-a".to_string(), node("broker-a", 7001)),
            (LOCAL.to_string(), node(LOCAL, 7002)),
            ("broker-c".to_string(), node("broker-c", 7003)),
        ]
        .into_iter()
        .collect();
        let table = RoutingTable::build(
            [(
                key(),
                "broker-a".to_string(),
                vec![LOCAL.to_string()],
                generation,
            )],
            &nodes,
        );
        router.publish(table, &nodes);
    }

    /// The reviewer's scenario, from the follower's side: it holds an extra
    /// record from the old leader, and the new leader writes its own at that
    /// offset.
    #[tokio::test]
    async fn a_suffix_from_the_previous_generation_is_dropped_and_replication_resumes() {
        let (broker, _dir) = broker_with_storage().await;
        let router = router_with(&[LOCAL], 4);
        let handler = ReplicaHandler::new(Arc::clone(&broker), Arc::clone(&router));

        // Generation 4: two records both leaders agree on, then an orphan the
        // old leader never got acknowledged.
        handler
            .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;
        handler
            .apply(batch(4, 2, &["orphan"]), felix_broker::LogKind::Stream)
            .await;

        // The old leader dies and generation 5 reuses offset 2. Nothing here
        // is committed, so the new leader is sent back to compare from 0.
        advance_to(&router, 5);
        let answer = handler
            .apply(batch(5, 2, &["committed"]), felix_broker::LogKind::Stream)
            .await;
        assert_eq!(refusal(&answer).expected_offset, 0);
        let answer = handler
            .apply(
                batch(5, 0, &["a", "b", "committed"]),
                felix_broker::LogKind::Stream,
            )
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 3),
            other => panic!(
                "the follower should have dropped its orphan and stored the \
                 leader's record, got {:?}",
                other.kind()
            ),
        }

        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        let stored: Vec<String> = log
            .read_from(0, 1024 * 1024)
            .await
            .expect("read")
            .into_iter()
            .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
            .collect();
        assert_eq!(
            stored,
            vec!["a", "b", "committed"],
            "the orphan is still there, so this broker would serve it if promoted",
        );
    }

    /// A divergence reaching below the generation this follower last accepted
    /// is not a repairable suffix, and it halts rather than guessing.
    #[tokio::test]
    async fn a_divergence_before_the_last_generation_still_halts() {
        let (broker, _dir) = broker_with_storage().await;
        let router = router_with(&[LOCAL], 4);
        let handler = ReplicaHandler::new(Arc::clone(&broker), Arc::clone(&router));

        handler
            .apply(batch(4, 0, &["a", "b", "c"]), felix_broker::LogKind::Stream)
            .await;
        // Generation 5 compares 0..3 and appends at 3, so the history says 5
        // begins there.
        advance_to(&router, 5);
        handler
            .apply(
                batch(5, 0, &["a", "b", "c", "d"]),
                felix_broker::LogKind::Stream,
            )
            .await;

        // Now a batch disagreeing at offset 1 — well before generation 5.
        let answer = handler
            .apply(batch(5, 1, &["different"]), felix_broker::LogKind::Stream)
            .await;

        let refused = refusal(&answer);
        assert_eq!(
            refused.code,
            ErrorCode::LogConflict,
            "a divergence below the last accepted generation was repaired \
             anyway, which discards records nothing says are uncommitted",
        );
    }

    /// A leader disagreeing with *itself* is not a repairable suffix.
    ///
    /// Same generation on both batches, so nothing has been deposed and there
    /// is no uncommitted suffix to drop. Repairing here would let a leader
    /// rewrite its own history — which is what the invariant forbids, and what
    /// an earlier version of this bound allowed until
    /// `a_conflict_is_reported_as_divergence` caught it.
    #[tokio::test]
    async fn a_leader_disagreeing_with_itself_is_not_repaired() {
        let (broker, _dir) = broker_with_storage().await;
        let router = router_with(&[LOCAL], 4);
        let handler = ReplicaHandler::new(Arc::clone(&broker), Arc::clone(&router));

        handler
            .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;

        let answer = handler
            .apply(
                batch(4, 0, &["a", "DIFFERENT"]),
                felix_broker::LogKind::Stream,
            )
            .await;

        assert_eq!(
            refusal(&answer).code,
            ErrorCode::LogConflict,
            "a leader was allowed to rewrite records it wrote itself",
        );
    }

    /// A dropped suffix leaves the stream's replay ring too.
    ///
    /// A restart refills the ring from disk, uncommitted tail included. When
    /// the new leader's records end below where the dropped ones did, storing
    /// them never moved the ring on, so this broker, once promoted, served the
    /// dropped records to readers alongside the ones that replaced them.
    #[tokio::test]
    async fn a_dropped_suffix_is_not_served_from_the_replay_ring() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let broker = broker_on(dir.path());
            let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
            handler
                .apply(
                    batch(4, 0, &["a", "b", "orphan-1", "orphan-2"]),
                    felix_broker::LogKind::Stream,
                )
                .await;
            broker
                .durable_storage()
                .expect("storage")
                .shutdown()
                .await
                .expect("shutdown");
        }

        let broker = broker_on(dir.path());
        broker.register_tenant(TENANT).await.expect("tenant");
        broker
            .register_namespace(TENANT, NAMESPACE)
            .await
            .expect("namespace");
        broker
            .register_stream(
                TENANT,
                NAMESPACE,
                STREAM,
                felix_broker::StreamMetadata {
                    durable: true,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        // From 0, so the whole older-generation tail is compared.
        let answer = handler
            .apply(batch(5, 0, &["a", "b", "x"]), felix_broker::LogKind::Stream)
            .await;
        match &answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 3),
            InternalMessage::ReplicateError(err) => {
                panic!("expected the suffix dropped and the batch stored: {err:?}")
            }
            other => panic!("expected ReplicateOk, got {:?}", other.kind()),
        }

        let resumed = broker
            .subscribe_from(
                TENANT,
                NAMESPACE,
                STREAM,
                0,
                felix_broker::StartPosition::Offset(0),
            )
            .await
            .expect("subscribe");
        let mut served = Vec::new();
        if let Some(range) = &resumed.history {
            for record in broker
                .read_durable(TENANT, NAMESPACE, STREAM, 0, range.from_offset, 1024 * 1024)
                .await
                .expect("history")
            {
                if record.offset < range.until_offset {
                    served.push((record.offset, record.payload));
                }
            }
        }
        served.extend(resumed.backlog.iter().cloned());
        let served: Vec<(u64, String)> = served
            .into_iter()
            .map(|(offset, payload)| (offset, String::from_utf8(payload.to_vec()).expect("utf8")))
            .collect();
        assert_eq!(
            served,
            vec![
                (0, "a".to_string()),
                (1, "b".to_string()),
                (2, "x".to_string())
            ],
            "a reader was served records the follower dropped",
        );
    }

    /// With no generation history there is no bound, so nothing is truncated.
    ///
    /// Every shard written before the history existed is in this state, and a
    /// broker that truncated on a bare conflict would be repairing on a guess.
    #[tokio::test]
    async fn a_conflict_with_no_history_halts() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

        // Written straight to the log, so no generation is ever recorded.
        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        log.append(&[Bytes::from_static(b"a")])
            .await
            .expect("append");

        let answer = handler
            .apply(batch(4, 0, &["different"]), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::LogConflict);
    }
}

/// Rebuilding at the leader's request: the one path that discards records.
mod rebuild {
    use super::*;
    use felix_wire::internal::{ReplicaLog, ReplicateRebuild};

    fn request(generation: u64, base_offset: u64) -> ReplicateRebuild {
        ReplicateRebuild {
            correlation_id: 1,
            shard: ShardRef {
                tenant_id: TENANT.to_string(),
                namespace: NAMESPACE.to_string(),
                stream: STREAM.to_string(),
                shard: 0,
                generation,
            },
            log: ReplicaLog::Stream,
            base_offset,
        }
    }

    /// **The records go, and the log restarts at the leader's base.** This
    /// is the case a bootstrap refuses: the broker holds records, and only
    /// the leader may decide they are not worth keeping.
    #[tokio::test]
    async fn a_replica_holding_records_discards_them_and_restarts_at_the_base() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(batch(4, 0, &["a", "b", "c"]), felix_broker::LogKind::Stream)
            .await;

        let answer = handler.rebuild(request(4, 500)).await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 500),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        assert_eq!(log.base_offset(), 500);
        assert_eq!(
            log.tail_offset().await.expect("tail"),
            500,
            "records survived the rebuild"
        );
    }

    /// Records shipped after a rebuild land at the leader's offsets, and the
    /// generation history that would have refused them as a conflict is gone.
    #[tokio::test]
    async fn records_after_a_rebuild_land_at_the_leaders_offsets() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(batch(4, 0, &["a", "b", "c"]), felix_broker::LogKind::Stream)
            .await;
        handler.rebuild(request(4, 500)).await;

        let answer = handler
            .apply(batch(4, 500, &["x", "y"]), felix_broker::LogKind::Stream)
            .await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 502),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
    }

    /// A rebuild can go backwards too: a leader whose base is below this
    /// broker's records.
    #[tokio::test]
    async fn a_rebuild_may_move_the_base_down() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .bootstrap(
                felix_wire::internal::ReplicateBootstrap {
                    correlation_id: 1,
                    shard: request(4, 0).shard,
                    base_offset: 900,
                },
                felix_broker::LogKind::Stream,
            )
            .await;
        handler
            .apply(batch(4, 900, &["a"]), felix_broker::LogKind::Stream)
            .await;

        let answer = handler.rebuild(request(4, 100)).await;

        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 100),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
        let answer = handler
            .apply(batch(4, 100, &["b"]), felix_broker::LogKind::Stream)
            .await;
        match answer {
            InternalMessage::ReplicateOk(ok) => assert_eq!(ok.durable_offset, 101),
            other => panic!("expected an acknowledgement, got {:?}", other.kind()),
        }
    }

    /// **The same fence applies as to storing records.** A superseded leader
    /// telling a follower to discard its copy is the most damage a stale
    /// leader could do, and the one thing the check most has to stop.
    #[tokio::test]
    async fn a_superseded_leader_cannot_rebuild_a_follower() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
        handler
            .apply(batch(5, 0, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;

        let answer = handler.rebuild(request(4, 500)).await;

        assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        assert_eq!(
            log.tail_offset().await.expect("tail"),
            2,
            "records were discarded"
        );
    }

    /// And a broker outside the replica set holds nothing the leader may
    /// discard.
    #[tokio::test]
    async fn a_broker_outside_the_replica_set_is_not_rebuilt() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(broker, router_with(&["broker-c"], 4));

        let answer = handler.rebuild(request(4, 500)).await;

        assert_eq!(refusal(&answer).code, ErrorCode::Unauthorized);
    }
}

/// What a follower holds on to across a restart and a change of leader: the
/// newest generation it accepted, and the commit offset the leader told it.
mod replica_state {
    use super::*;

    fn committed(
        generation: u64,
        first_offset: u64,
        values: &[&str],
        commit: u64,
    ) -> ReplicateRecords {
        ReplicateRecords {
            commit_offset: Some(commit),
            ..batch(generation, first_offset, values)
        }
    }

    async fn stored(broker: &Broker) -> Vec<String> {
        broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open")
            .read_from(0, 1024 * 1024)
            .await
            .expect("read")
            .into_iter()
            .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
            .collect()
    }

    /// **A restart does not forget which leaders were replaced.** After one,
    /// the routing view is rebuilt and can lag behind what this follower had
    /// already accepted; a deposed leader matching that stale view must still
    /// be refused, or it counts this broker toward a quorum it no longer has.
    #[tokio::test]
    async fn a_leader_older_than_one_accepted_is_refused_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let broker = broker_on(dir.path());
            let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5));
            let answer = handler
                .apply(batch(5, 0, &["a"]), felix_broker::LogKind::Stream)
                .await;
            assert!(matches!(answer, InternalMessage::ReplicateOk(_)));
        }

        // Restarted, with a routing view that still says generation 4.
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        let answer = handler
            .apply(batch(4, 1, &["stale"]), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::FencedEpoch);
        assert_eq!(stored(&broker).await, vec!["a"]);
    }

    /// A follower learns no more of the commit offset than the batch left it
    /// level with the leader on.
    #[tokio::test]
    async fn the_commit_offset_is_capped_at_what_the_batch_stored() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

        handler
            .apply(
                committed(4, 0, &["a", "b"], 10),
                felix_broker::LogKind::Stream,
            )
            .await;

        let log = broker
            .durable_storage()
            .expect("storage")
            .open_stream(TENANT, NAMESPACE, STREAM, 0)
            .expect("open");
        assert_eq!(log.commit_offset(), 2);
    }

    /// **A committed record is never dropped as a dead leader's suffix.** The
    /// same shape as the repairable case, except the old leader said the
    /// record was committed: a newer leader disagreeing with it is a fault to
    /// surface, and dropping it would lose an acknowledged write.
    #[tokio::test]
    async fn a_suffix_below_the_commit_offset_is_not_dropped() {
        let (broker, _dir) = broker_with_storage().await;
        let router = router_with(&[LOCAL], 4);
        let handler = ReplicaHandler::new(Arc::clone(&broker), Arc::clone(&router));
        handler
            .apply(batch(4, 0, &["a", "b"]), felix_broker::LogKind::Stream)
            .await;
        handler
            .apply(
                committed(4, 2, &["acked"], 3),
                felix_broker::LogKind::Stream,
            )
            .await;

        // Generation 5 reuses offset 2, as it would for an unacknowledged one.
        let nodes: HashMap<String, NodeRef> = [
            ("broker-a".to_string(), node("broker-a", 7001)),
            (LOCAL.to_string(), node(LOCAL, 7002)),
        ]
        .into_iter()
        .collect();
        router.publish(
            RoutingTable::build(
                [(key(), "broker-a".to_string(), vec![LOCAL.to_string()], 5)],
                &nodes,
            ),
            &nodes,
        );
        let answer = handler
            .apply(batch(5, 2, &["other"]), felix_broker::LogKind::Stream)
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::LogConflict);
        assert_eq!(stored(&broker).await, vec!["a", "b", "acked"]);
    }

    /// **A rebuild does not discard committed records either.** The leader
    /// asking may not hold them, and this copy may be the last one.
    #[tokio::test]
    async fn a_rebuild_that_would_discard_committed_records_is_refused() {
        let (broker, _dir) = broker_with_storage().await;
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        handler
            .apply(
                committed(4, 0, &["a", "b", "c"], 2),
                felix_broker::LogKind::Stream,
            )
            .await;

        let answer = handler
            .rebuild(felix_wire::internal::ReplicateRebuild {
                correlation_id: 1,
                shard: batch(4, 0, &[]).shard,
                log: felix_wire::internal::ReplicaLog::Stream,
                base_offset: 0,
            })
            .await;

        assert_eq!(refusal(&answer).code, ErrorCode::LogConflict);
        assert_eq!(stored(&broker).await, vec!["a", "b", "c"]);
    }
}

/// The records `LOCAL` holds for the shard, as strings.
async fn held(broker: &Broker) -> Vec<String> {
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.read_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}

/// **A newer leader is sent back to what this follower wrote under an older
/// generation.** Offset 2 is a dead leader's unacknowledged write; the new
/// leader holds something else there. A batch starting at this follower's
/// tail would compare nothing and keep it, so the follower answers with a gap
/// at the first record past its commit offset, and the resend finds and drops
/// the stale one.
#[tokio::test]
async fn a_newer_leader_compares_what_an_older_generation_left() {
    let (broker, _dir) = broker_with_storage().await;
    let old = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let mut first = batch(4, 0, &["a", "b"]);
    first.commit_offset = Some(2);
    old.apply(first, felix_broker::LogKind::Stream).await;
    old.apply(batch(4, 2, &["stale"]), felix_broker::LogKind::Stream)
        .await;

    let new = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    let answer = new
        .apply(batch(6, 3, &["d"]), felix_broker::LogKind::Stream)
        .await;
    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogGap);
    assert_eq!(
        refused.expected_offset, 2,
        "sent back past the commit offset"
    );

    new.apply(batch(6, 2, &["c", "d"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(held(&broker).await, ["a", "b", "c", "d"]);
}

/// Comparing can take several batches. The older generation stays the last
/// one recorded until it is done, so a conflict in a later batch is still its
/// suffix and dropped, rather than read as the new leader disagreeing with
/// itself.
#[tokio::test]
async fn comparing_an_older_generation_over_several_batches_still_repairs() {
    let (broker, _dir) = broker_with_storage().await;
    let old = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    old.apply(
        batch(4, 0, &["a", "b", "c", "stale"]),
        felix_broker::LogKind::Stream,
    )
    .await;

    let new = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    let answer = new
        .apply(batch(6, 4, &["e"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(refusal(&answer).expected_offset, 0);
    let answer = new
        .apply(batch(6, 0, &["a", "b"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );
    new.apply(batch(6, 2, &["c", "d", "e"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(held(&broker).await, ["a", "b", "c", "d", "e"]);
}
