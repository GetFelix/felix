//! A promoted shard waits for the fence before it serves:
//! `fencing[b]` in `docs/formal/FelixShard.tla`, which `Serving` excludes.

use super::*;

fn fencing_lifecycle() -> ShardLifecycle {
    let mut own = lifecycle();
    own.fence_promotions();
    own
}

/// **A promoted shard does not serve until replication says it is fenced**,
/// and the write fence refuses a write that got admitted meanwhile.
#[test]
fn a_promotion_waits_in_fencing_until_the_fence_is_done() {
    let mut own = fencing_lifecycle();

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
        Action::Open {
            key: key(0),
            generation: 3,
            new_term: true,
            fence: true,
        }
    );
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);
    assert_eq!(own.phase(&key(0)), Phase::Fencing);
    assert!(!own.may_serve(&key(0)));
    assert!(own.servable().is_empty());
    assert_eq!(own.fence().awaiting_promotion(&key(0)), Some(3));
    assert!(own.fence().admit(&key(0), 3).is_err());

    // Re-delivered while fencing: nothing new to do.
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
        Action::None
    );
    assert!(
        !own.fenced(&key(0), 2),
        "a stale generation must not open it"
    );

    assert!(own.fenced(&key(0), 3));
    assert_eq!(own.phase(&key(0)), Phase::Active);
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert!(own.fence().admit(&key(0), 3).is_ok());
}

/// A move's destination takes over from a leader that drained into it, which
/// the model does not fence and neither does the broker.
#[test]
fn a_moves_destination_is_not_held_for_the_fence() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&moving_to("broker-a", 2, "active")));

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open { fence: false, .. }
    ));
    assert_eq!(own.opened(&key(0), 4), Opened::Activated);
}

/// A cancelled move hands the shard back to the leader that was draining it,
/// with its fenced writes; the model's `Retake`, unfenced.
#[test]
fn a_cancelled_move_is_not_held_for_the_fence() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.fenced(&key(0), 2);
    let draining = ShardAssignment {
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string()],
        state: "draining".to_string(),
        successor: Some("broker-b".to_string()),
        ..assigned_to("broker-a", 3)
    };
    own.observe(&key(0), Some(&draining));
    assert_eq!(own.opened(&key(0), 3), Opened::Draining);

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open { fence: false, .. }
    ));
}

/// A new generation reaching a shard still fencing starts the fence again at
/// that generation. Until the reopen finishes the routes still name the old
/// one, and the shard keeps waiting at it, so replication does not ship the
/// unfenced log to followers that may hold records it lacks.
#[test]
fn a_new_generation_while_fencing_keeps_the_promotion_pending() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(own.opened(&key(0), 3), Opened::Fencing);

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open {
            key: key(0),
            generation: 4,
            new_term: true,
            fence: true,
        }
    );
    assert_eq!(own.fence().awaiting_promotion(&key(0)), Some(3));

    assert_eq!(own.opened(&key(0), 4), Opened::Fencing);
    assert_eq!(own.fence().awaiting_promotion(&key(0)), Some(4));
}

/// Reassigned away while fencing: it never served, and it lets go like an
/// open shard does.
#[test]
fn a_shard_reassigned_while_fencing_is_released() {
    let mut own = fencing_lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    assert!(matches!(
        own.observe(&key(0), Some(&assigned_to("broker-b", 4))),
        Action::Release { .. }
    ));
    assert_eq!(own.fence().awaiting_promotion(&key(0)), None);
    assert!(!own.fenced(&key(0), 3));
}

/// Opening a promoted shard through the real gate, with `generation_start`
/// finalized and a broker whose stream is `durable` or in-memory.
mod generation_start {
    use super::*;
    use felix_replication::promotion::PromotionGate;
    use felix_storage::log::{FsyncMode, LogConfig};

    struct Promoted {
        gate: crate::shards::lifecycle::promotion::LifecycleGate,
        own: std::sync::Arc<tokio::sync::Mutex<ShardLifecycle>>,
        broker: std::sync::Arc<felix_broker::Broker>,
        storage: std::sync::Arc<felix_broker::DurableStorage>,
        _dir: tempfile::TempDir,
    }

    /// `key(0)` promoted here at generation 3 and fencing, on a broker with
    /// durable storage whatever the stream is.
    async fn promoted(durable: bool) -> Promoted {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = felix_broker::DurableStorage::open(
            dir.path(),
            LogConfig {
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage");
        let broker = felix_broker::Broker::new(felix_storage::EphemeralCache::new().into())
            .with_durable_storage(storage.clone());
        let key = key(0);
        broker
            .register_tenant(&key.tenant_id)
            .await
            .expect("tenant");
        broker
            .register_namespace(&key.tenant_id, &key.namespace)
            .await
            .expect("namespace");
        broker
            .register_stream(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                felix_broker::StreamMetadata {
                    durable,
                    shards: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
        let broker = std::sync::Arc::new(broker);
        // A durable log the leader inherited something in.
        if durable {
            broker
                .publish(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    "inherited".into(),
                )
                .await
                .expect("publish");
        }

        let mut own = fencing_lifecycle();
        own.observe(&key, Some(&assigned_to("broker-a", 3)));
        assert_eq!(own.opened(&key, 3), Opened::Fencing);
        let router = std::sync::Arc::new(felix_router::ShardRouter::new(
            "broker-a",
            "us-west-2",
            felix_router::RegionRouter::new("us-west-2".to_string()),
        ));
        let ingress = std::sync::Arc::new(crate::shards::routing::IngressRouter::new(
            router,
            std::sync::Arc::clone(own.fence()),
        ));
        let finalized = [felix_common::fleet::GENERATION_START.name()];
        let fleet = felix_common::fleet::FleetGate::new(finalized);
        fleet.observe(finalized);
        let own = std::sync::Arc::new(tokio::sync::Mutex::new(own));
        let storage = std::sync::Arc::new(storage);
        let gate = crate::shards::lifecycle::promotion::LifecycleGate::new(
            std::sync::Arc::clone(&own),
            ingress,
            std::sync::Arc::clone(&storage),
            std::sync::Arc::clone(&broker),
            std::sync::Arc::new(fleet),
        );
        Promoted {
            gate,
            own,
            broker,
            storage,
            _dir: dir,
        }
    }

    /// **An in-memory stream's promoted shard opens once `generation_start`
    /// is finalized (#930).** Its publishes never reach the shard's log, so
    /// there is nothing inherited for a start record to cover; demanding one
    /// kept the shard closed for good, retrying every pass.
    #[tokio::test]
    async fn an_in_memory_shard_opens_and_takes_writes() {
        let promoted = promoted(false).await;

        assert!(
            promoted.gate.open(&key(0), 3).await,
            "the shard stayed closed"
        );
        let own = promoted.own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Active);
        assert!(own.fence().admit(&key(0), 3).is_ok());
        drop(own);
        promoted
            .broker
            .publish("t1", "ns", "orders", "after".into())
            .await
            .expect("publish after the promotion");
    }

    /// A durable stream still gets its record before it serves: skipping it
    /// would count the inherited record on a majority alone (Figure 8).
    #[tokio::test]
    async fn a_durable_shard_writes_its_record_before_it_opens() {
        let promoted = promoted(true).await;

        assert!(promoted.gate.open(&key(0), 3).await);
        assert_eq!(promoted.own.lock().await.phase(&key(0)), Phase::Active);
        let log = promoted
            .storage
            .open_stream("t1", "ns", "orders", 0)
            .expect("log");
        assert_eq!(
            felix_replication::quorum::generation_start(&log, 3).await,
            Some(1),
        );
    }
}
