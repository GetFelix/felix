//! The determinism harness, and the behavioural guarantees the snapshot
//! format makes.
//!
//! The harness is the cheap test that catches the expensive bug: one command
//! script covering every command — including the multi-entity operations
//! (expiry over many nodes, a tenant cascade) where a HashMap iteration
//! order or a clock read would leak — applied to two independent state
//! machines, which must serialize **byte-identical** snapshots. Any
//! nondeterminism in apply shows up here as a failed byte comparison, on
//! every run, rather than as two Raft replicas quietly disagreeing in
//! production.
use super::*;

/// **The determinism harness.** Two state machines, one script, byte-equal
/// snapshots. A clock read, a generated value, or a HashMap iteration order
/// anywhere in apply fails this on every run.
#[tokio::test]
async fn the_same_log_produces_byte_identical_snapshots() {
    let first = machine();
    let second = machine();
    run_script(&first).await;
    run_script(&second).await;

    let first_snapshot = crate::raft::AppStateMachine::snapshot(&first).await;
    let second_snapshot = crate::raft::AppStateMachine::snapshot(&second).await;
    assert!(!first_snapshot.is_empty());
    assert_eq!(
        first_snapshot, second_snapshot,
        "two replicas applying the same commands serialized different state",
    );
}

/// Restore is exact: a third machine restored from a snapshot serializes
/// the same bytes, and — the half that matters to brokers — answers the
/// change feeds identically, sequence numbers included.
#[tokio::test]
async fn a_restored_machine_is_indistinguishable() {
    let original = machine();
    run_script(&original).await;
    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;

    let restored = machine();
    crate::raft::AppStateMachine::restore(&restored, &snapshot).await;

    assert_eq!(
        snapshot,
        crate::raft::AppStateMachine::snapshot(&restored).await,
        "export → import → export must be a fixed point",
    );

    let a = original.store().node_changes(0).await.expect("changes");
    let b = restored.store().node_changes(0).await.expect("changes");
    assert_eq!(a.next_seq, b.next_seq);
    assert_eq!(a.items.len(), b.items.len());

    let a = original.store().stream_snapshot().await.expect("snapshot");
    let b = restored.store().stream_snapshot().await.expect("snapshot");
    assert_eq!(a.next_seq, b.next_seq, "watch checkpoints must survive");
}

/// The resnapshot signal survives a restore: a consumer whose checkpoint
/// fell out of the retained window gets told so by the restored store
/// exactly as the original would have — `first seq > since` — instead of a
/// quiet gap.
#[tokio::test]
async fn an_evicted_change_window_reads_the_same_after_restore() {
    let config = StoreConfig {
        changes_limit: 3,
        change_retention_max_rows: Some(3),
    };
    let original = machine_with(config.clone());
    for i in 0..10 {
        original
            .dispatch(MetaCommand::CreateTenant {
                tenant: tenant(&format!("t-{i}")),
            })
            .await
            .expect("create");
    }

    let before = original.store().tenant_changes(0).await.expect("changes");
    assert!(
        before.items[0].seq > 0,
        "the premise: seq 0 has been evicted"
    );

    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;
    let restored = machine_with(config);
    crate::raft::AppStateMachine::restore(&restored, &snapshot).await;
    let after = restored.store().tenant_changes(0).await.expect("changes");

    assert_eq!(before.items[0].seq, after.items[0].seq);
    assert_eq!(before.next_seq, after.next_seq);
}

/// A follower that installs a snapshot holds the leader's refresh tokens, in
/// the leader's state: a revoked token stays revoked there, and a live one
/// can be spent once.
#[tokio::test]
async fn a_restored_machine_has_the_leaders_refresh_tokens() {
    use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};

    let token = |id: &str, principal: &str| RefreshToken {
        token_id: id.to_string(),
        tenant_id: "t-a".to_string(),
        principal_id: principal.to_string(),
        groups: Vec::new(),
        secret_hash: "hash".to_string(),
        family_id: format!("family-{id}"),
        issued_at_secs: 1_000,
        expires_at_secs: 10_000,
        used: false,
        revoked: false,
        narrowing: Some(crate::auth::refresh_token::Narrowing {
            requested: Some(vec!["stream.publish".to_string()]),
            resources: None,
            permissions: None,
            audience: "felix-broker".to_string(),
            may_act: None,
        }),
        rotated: None,
    };
    let leader = machine();
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::InsertRefreshToken {
            token: token("live", "p:alice"),
        },
        MetaCommand::InsertRefreshToken {
            token: token("revoked", "p:mallory"),
        },
        MetaCommand::RevokeRefreshTokensForPrincipal {
            tenant_id: "t-a".to_string(),
            principal_id: "p:mallory".to_string(),
        },
    ] {
        leader.dispatch(command).await.expect("apply");
    }

    let snapshot = crate::raft::AppStateMachine::snapshot(&leader).await;
    let follower = machine();
    crate::raft::AppStateMachine::restore(&follower, &snapshot).await;

    let store = follower.store();
    assert!(matches!(
        store
            .take_refresh_token("t-a", "live", 2_000)
            .await
            .expect("take"),
        RefreshTokenTake::Taken(_)
    ));
    assert_eq!(
        store
            .take_refresh_token("t-a", "revoked", 2_000)
            .await
            .expect("take"),
        RefreshTokenTake::Unusable
    );
}

/// A member restored from a snapshot holds the replica reports the leader
/// had, so it can still promote a replica of a leader that died before the
/// snapshot was taken: that leader will never report again.
#[tokio::test]
async fn a_restored_machine_can_fail_over_on_the_leaders_reports() {
    use crate::cluster::placement::{CaughtUp, ReplicaPositions};

    let leader = machine();
    let mut assignment = shard_assignment("t-a", "ns-1", "orders", "broker-0");
    assignment.replicas = vec!["broker-1".to_string()];
    let report = replica_report("t-a", "ns-1", "orders", crate::clock::now_millis());
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t-a", "ns-1"),
        },
        MetaCommand::CreateStream {
            stream: stream("t-a", "ns-1", "orders"),
        },
        MetaCommand::RegisterNode {
            node: node("broker-0", 7_000),
        },
        MetaCommand::RegisterNode {
            node: node("broker-1", 7_001),
        },
        MetaCommand::PutShardAssignmentIf {
            assignment: assignment.clone(),
            expected_generation: None,
        },
    ] {
        leader.dispatch(command).await.expect("apply");
    }
    let generation = leader
        .store()
        .get_shard_assignment(&assignment.key)
        .await
        .expect("assignment")
        .generation;
    let report = crate::model::ReplicaReport {
        generation,
        ..report
    };
    let stored = leader
        .dispatch(MetaCommand::RecordReplicaReport {
            report: report.clone(),
            leader: Some("broker-0".to_string()),
        })
        .await
        .expect("report");
    assert!(matches!(stored, MetaResponse::Unit), "{stored:?}");

    let snapshot = crate::raft::AppStateMachine::snapshot(&leader).await;
    let follower = machine();
    crate::raft::AppStateMachine::restore(&follower, &snapshot).await;

    assert_eq!(
        follower
            .store()
            .list_replica_reports()
            .await
            .expect("reports"),
        vec![report],
    );
    let positions = ReplicaPositions::load(follower.store().as_ref(), &Default::default())
        .await
        .expect("positions");
    assert!(positions.is_caught_up(&assignment.key, "broker-1"));
}

/// A snapshot from a member that predates snapshotted reports still loads,
/// with no reports.
#[tokio::test]
async fn a_snapshot_without_replica_reports_still_restores() {
    let original = machine();
    run_script(&original).await;
    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;
    let mut value: serde_json::Value = serde_json::from_slice(&snapshot).expect("json");
    let state = value["state"].as_object_mut().expect("state");
    assert!(state.remove("replica_reports").is_some());
    let old = serde_json::to_vec(&value).expect("json");

    let restored = machine();
    crate::raft::AppStateMachine::restore(&restored, &old).await;
    assert!(
        restored
            .store()
            .list_replica_reports()
            .await
            .expect("reports")
            .is_empty()
    );
    // Everything else came back as it was.
    let again = crate::raft::AppStateMachine::snapshot(&restored).await;
    let again: serde_json::Value = serde_json::from_slice(&again).expect("json");
    assert_eq!(again, value);
}

/// A jump-hash stream is still one after a restore.
#[tokio::test]
async fn a_restored_stream_keeps_its_routing() {
    let original = machine();
    run_script(&original).await;
    let mut jumpy = stream("t-a", "ns-3", "jumpy");
    jumpy.routing = crate::model::StreamRouting::JumpHash;
    original
        .dispatch(MetaCommand::CreateStream { stream: jumpy })
        .await
        .expect("create");
    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;

    let restored = machine();
    crate::raft::AppStateMachine::restore(&restored, &snapshot).await;
    let key = StreamKey {
        tenant_id: "t-a".to_string(),
        namespace: "ns-3".to_string(),
        stream: "jumpy".to_string(),
    };
    assert_eq!(
        restored
            .store()
            .get_stream(&key)
            .await
            .expect("stream")
            .routing,
        crate::model::StreamRouting::JumpHash
    );
}

/// A snapshot from a newer build, carrying a field this one does not know,
/// stops the member rather than restoring without it.
#[tokio::test]
#[should_panic(expected = "snapshot carries fields this build does not know")]
async fn a_snapshot_with_a_field_this_build_does_not_know_is_refused() {
    let original = machine();
    run_script(&original).await;
    let snapshot = crate::raft::AppStateMachine::snapshot(&original).await;
    let mut value: serde_json::Value = serde_json::from_slice(&snapshot).expect("json");
    value["state"]["streams"][0][1]["placement_hint"] = serde_json::json!("rack-aware");
    let newer = serde_json::to_vec(&value).expect("json");

    crate::raft::AppStateMachine::restore(&machine(), &newer).await;
}

/// An entry carrying a level-10 field raises the mark, whether or not it
/// succeeds; the mark rides the snapshot and sets the join floor. Entries at
/// a released level leave it at 0 and out of the snapshot, which a
/// v0.6.0-preview.7 member would refuse.
#[tokio::test]
async fn a_level_10_field_raises_the_mark_and_survives_a_snapshot() {
    use crate::raft::AppStateMachine;
    let original = machine();
    original
        .apply(&encode_command(&MetaCommand::CreateTenant {
            tenant: tenant("acme"),
        }))
        .await;
    assert_eq!(original.levels_used(), 0);
    assert_eq!(original.join_floor(), 0);
    let unmarked = original.snapshot().await;
    assert!(
        !String::from_utf8_lossy(&unmarked).contains("levels_used"),
        "a released-level snapshot carries the mark"
    );

    // `acme` has no namespaces; the entry's field is what counts.
    let flagged = MetaCommand::DeleteTenant {
        tenant_id: "missing".to_string(),
        take_shards: true,
    };
    assert_eq!(flagged.version(), 10);
    original.apply(&encode_command(&flagged)).await;
    assert_eq!(original.levels_used(), 10);
    assert_eq!(original.join_floor(), 10);

    let restored = machine();
    restored.restore(&original.snapshot().await).await;
    assert_eq!(restored.levels_used(), 10);
    assert_eq!(restored.join_floor(), 10);
    assert_eq!(restored.snapshot().await, original.snapshot().await);

    restored.restore(&unmarked).await;
    assert_eq!(restored.levels_used(), 0, "a restore replaces the mark");
    assert_eq!(restored.join_floor(), 0, "a restore replaces the floor");
}

/// The join floor is cached for the replication path; it follows a
/// finalized feature on apply and the restored state on restore.
#[tokio::test]
async fn the_join_floor_follows_apply_and_restore() {
    use crate::raft::AppStateMachine;
    let original = machine();
    let empty = original.snapshot().await;
    let jump_hash = felix_common::fleet::JUMP_HASH_ROUTING.name().to_string();
    let mut broker = node("broker-a", 7001);
    broker.status.features.insert(jump_hash.clone());
    original
        .apply(&encode_command(&MetaCommand::RegisterNodeInFleet {
            node: broker,
        }))
        .await;
    assert_eq!(original.join_floor(), 0);
    original
        .apply(&encode_command(&MetaCommand::FinalizeFleetFeature {
            feature: jump_hash,
        }))
        .await;

    assert_eq!(original.levels_used(), 0);
    assert_eq!(original.join_floor(), 3);

    let restored = machine();
    restored.restore(&original.snapshot().await).await;
    assert_eq!(restored.join_floor(), 3);
    restored.restore(&empty).await;
    assert_eq!(restored.join_floor(), 0);
}

/// A report carrying `by_round` needs level 11, so applying it raises the
/// mark and the join floor to 11; one without it leaves both alone. The
/// proposer strips it below 11 (`record_replica_report`), so only a group
/// already at 11 ever applies one.
#[tokio::test]
async fn a_report_by_round_raises_the_mark_to_11() {
    use crate::raft::AppStateMachine;
    let original = machine();
    let plain = MetaCommand::RecordReplicaReport {
        report: replica_report("acme", "ns", "orders", 1),
        leader: None,
    };
    assert!(plain.version() < 11);
    original.apply(&encode_command(&plain)).await;
    assert_eq!(original.levels_used(), 0);
    assert_eq!(original.join_floor(), 0);

    let by_round = MetaCommand::RecordReplicaReport {
        report: crate::model::ReplicaReport {
            by_round: true,
            leader_incarnation: Some(2),
            ..replica_report("acme", "ns", "orders", 2)
        },
        leader: None,
    };
    assert_eq!(by_round.version(), 11);
    original.apply(&encode_command(&by_round)).await;
    assert_eq!(original.levels_used(), 11);
    assert_eq!(original.join_floor(), 11);
}

/// **Storage identities are plain command data.** A registration carrying a
/// node's identity and a report carrying its followers' are stored as sent,
/// with nothing read from a clock or the node table, so two members applying
/// them serialize the same bytes and a restored member holds the same. Both
/// need level 12, so applying them raises the mark to it.
#[tokio::test]
async fn storage_identities_apply_identically() {
    use crate::raft::AppStateMachine;
    let id = "0123456789abcdef0123456789abcdef".to_string();
    let mut registered = node("broker-1", 7_001);
    registered.status.storage_id = Some(id.clone());
    let report = crate::model::ReplicaReport {
        follower_storage: [("broker-1".to_string(), id.clone())].into(),
        ..replica_report("t-a", "ns-1", "orders", 1_000)
    };
    let commands = vec![
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t-a", "ns-1"),
        },
        MetaCommand::CreateStream {
            stream: stream("t-a", "ns-1", "orders"),
        },
        MetaCommand::RegisterNode {
            node: node("broker-0", 7_000),
        },
        MetaCommand::RegisterNodeInFleet { node: registered },
        MetaCommand::PutShardAssignmentIf {
            assignment: shard_assignment("t-a", "ns-1", "orders", "broker-0"),
            expected_generation: None,
        },
    ];
    let (first, second) = (machine(), machine());
    for machine in [&first, &second] {
        for command in &commands {
            machine.dispatch(command.clone()).await.expect("apply");
        }
        let generation = machine
            .store()
            .get_shard_assignment(&report.key)
            .await
            .expect("assignment")
            .generation;
        let command = MetaCommand::RecordReplicaReport {
            report: crate::model::ReplicaReport {
                generation,
                ..report.clone()
            },
            leader: Some("broker-0".to_string()),
        };
        assert_eq!(command.version(), 12);
        machine.apply(&encode_command(&command)).await;
    }
    let snapshot = first.snapshot().await;
    assert_eq!(snapshot, second.snapshot().await);
    let held = first.store().list_replica_reports().await.expect("reports");
    assert_eq!(held[0].follower_storage, report.follower_storage);
    let node = first.store().get_node("broker-1").await.expect("node");
    assert_eq!(node.status.storage_id, Some(id));
    assert_eq!(first.levels_used(), 12);
    assert_eq!(first.join_floor(), 12);

    let restored = machine();
    restored.restore(&snapshot).await;
    assert_eq!(snapshot, restored.snapshot().await);
}
