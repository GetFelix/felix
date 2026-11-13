//! The group's identity through the apply loop: first record wins, and it
//! rides snapshots, restores and imports.
use super::*;
use crate::store::identity::ClusterIdentity;

fn record(group_id: &str, shard_engine: &str) -> MetaCommand {
    MetaCommand::RecordClusterIdentity {
        group_id: group_id.to_string(),
        shard_engine: shard_engine.to_string(),
    }
}

fn identity(group_id: &str, shard_engine: &str) -> ClusterIdentity {
    ClusterIdentity {
        group_id: group_id.to_string(),
        shard_engine: shard_engine.to_string(),
        adopts_existing: false,
    }
}

async fn answer(machine: &MetadataStateMachine, command: MetaCommand) -> ClusterIdentity {
    match machine.dispatch(command).await.expect("record") {
        MetaResponse::ClusterIdentity { identity } => identity,
        other => panic!("unexpected answer {other:?}"),
    }
}

#[tokio::test]
async fn the_first_identity_recorded_wins() {
    let machine = machine();
    assert_eq!(machine.store().recorded_identity().await, None);

    assert_eq!(
        answer(&machine, record("g-1", "hybrid")).await,
        identity("g-1", "hybrid")
    );
    // Later proposals, whatever they carry, are answered with the record.
    assert_eq!(
        answer(&machine, record("g-2", "raft")).await,
        identity("g-1", "hybrid")
    );
    assert_eq!(
        machine.store().recorded_identity().await,
        Some(identity("g-1", "hybrid"))
    );
}

/// Two replicas applying the same log end in the same state.
#[tokio::test]
async fn applying_the_same_records_gives_the_same_identity() {
    let (a, b) = (machine(), machine());
    for machine in [&a, &b] {
        answer(machine, record("g-1", "raft")).await;
        answer(machine, record("g-2", "hybrid")).await;
    }
    assert_eq!(
        a.store().recorded_identity().await,
        b.store().recorded_identity().await
    );
    assert_eq!(a.snapshot().await, b.snapshot().await);
}

#[tokio::test]
async fn a_snapshot_restore_carries_the_identity() {
    let original = machine();
    answer(&original, record("g-1", "raft")).await;

    let restored = machine();
    restored.restore(&original.snapshot().await).await;
    assert_eq!(
        restored.store().recorded_identity().await,
        Some(identity("g-1", "raft"))
    );
    // And a later record on the restored replica still loses.
    assert_eq!(
        answer(&restored, record("g-9", "hybrid")).await,
        identity("g-1", "raft")
    );
}

#[tokio::test]
async fn a_snapshot_taken_before_any_record_restores_to_none() {
    let original = machine();
    let restored = machine();
    answer(&restored, record("g-1", "raft")).await;
    restored.restore(&original.snapshot().await).await;
    assert_eq!(restored.store().recorded_identity().await, None);
}

#[tokio::test]
async fn an_import_adopts_the_exports_identity_and_keeps_its_own_when_it_has_none() {
    let source = machine();
    answer(&source, record("g-old", "raft")).await;
    let with_identity = source.store().export_state().await;
    let without = machine().store().export_state().await;

    let target = machine();
    answer(&target, record("g-new", "hybrid")).await;
    target
        .dispatch(MetaCommand::ImportState {
            state: Box::new(without),
            overwrite: true,
        })
        .await
        .expect("import without identity");
    assert_eq!(
        target.store().recorded_identity().await,
        Some(identity("g-new", "hybrid")),
        "an export with no identity leaves the group's own"
    );

    target
        .dispatch(MetaCommand::ImportState {
            state: Box::new(with_identity),
            overwrite: true,
        })
        .await
        .expect("import with identity");
    assert_eq!(
        target.store().recorded_identity().await,
        Some(identity("g-old", "raft")),
        "a restore is a restore of the exported group"
    );
}

#[tokio::test]
async fn an_exported_state_carries_the_identity() {
    let machine = machine();
    answer(&machine, record("g-1", "raft")).await;
    let exported = machine.store().export_state().await;
    let restored = InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    });
    restored.import_state(exported).await.expect("import");
    assert_eq!(
        restored.recorded_identity().await,
        Some(identity("g-1", "raft"))
    );
}

/// A group upgraded in place records that it already held streams, so its
/// brokers keep their data; one formed empty does not.
#[tokio::test]
async fn an_identity_recorded_over_streams_adopts_existing_data() {
    let empty = machine();
    empty
        .dispatch(MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        })
        .await
        .expect("tenant");
    assert!(
        !answer(&empty, record("g-1", "hybrid"))
            .await
            .adopts_existing,
        "a tenant alone holds no broker data"
    );

    let upgraded = machine();
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t-a", "ns-1"),
        },
        MetaCommand::CreateStream {
            needs_identity: false,
            names: false,
            stream: stream("t-a", "ns-1", "s"),
            incarnations: false,
        },
    ] {
        upgraded.dispatch(command).await.expect("create");
    }
    let recorded = answer(&upgraded, record("g-2", "hybrid")).await;
    assert!(recorded.adopts_existing);

    let restored = machine();
    restored.restore(&upgraded.snapshot().await).await;
    assert_eq!(restored.store().recorded_identity().await, Some(recorded));
}

/// Restoring an export taken before the identity existed brings back
/// streams that brokers already hold, so the group adopts them under its own
/// id rather than leaving wiping every broker as the only way out.
#[tokio::test]
async fn an_import_with_no_identity_over_streams_adopts_existing_data() {
    let source = machine();
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t-a", "ns-1"),
        },
        MetaCommand::CreateStream {
            needs_identity: false,
            names: false,
            stream: stream("t-a", "ns-1", "s"),
            incarnations: false,
        },
    ] {
        source.dispatch(command).await.expect("create");
    }
    let without = source.store().export_state().await;

    let target = machine();
    assert!(
        !answer(&target, record("g-new", "hybrid"))
            .await
            .adopts_existing
    );
    target
        .dispatch(MetaCommand::ImportState {
            state: Box::new(without),
            overwrite: true,
        })
        .await
        .expect("import without identity");
    assert_eq!(
        target.store().recorded_identity().await,
        Some(ClusterIdentity {
            adopts_existing: true,
            ..identity("g-new", "hybrid")
        })
    );
    // The import's own rule holds too: the restored stream starts no record
    // of the brokers placed in its epoch.
    let after = serde_json::to_value(target.store().export_state().await).expect("json");
    let unplaced = after["unplaced"].as_array().expect("unplaced");
    assert!(
        unplaced
            .iter()
            .any(|owner| owner.to_string().contains("\"s\"")),
        "{unplaced:?}"
    );
}

/// An export carrying an identity and streams: the import adopts that
/// identity as exported and still marks the streams unplaced.
#[tokio::test]
async fn an_import_with_an_identity_keeps_it_and_marks_streams_unplaced() {
    let source = machine_with_tenant().await;
    answer(&source, record("g-old", "raft")).await;
    source
        .dispatch(MetaCommand::CreateStream {
            needs_identity: true,
            names: false,
            stream: stream("t-a", "ns-1", "s"),
            incarnations: false,
        })
        .await
        .expect("create");
    let exported = source.store().export_state().await;

    let target = machine();
    answer(&target, record("g-new", "hybrid")).await;
    target
        .dispatch(MetaCommand::ImportState {
            state: Box::new(exported),
            overwrite: true,
        })
        .await
        .expect("import");
    assert_eq!(
        target.store().recorded_identity().await,
        Some(identity("g-old", "raft")),
        "recorded on a catalog with no streams yet, so it does not adopt"
    );
    let after = serde_json::to_value(target.store().export_state().await).expect("json");
    let unplaced = after["unplaced"].as_array().expect("unplaced");
    assert_eq!(unplaced.len(), 1, "{unplaced:?}");
}

/// A create proposed at level 32 that reaches the log before the record is
/// refused on every member and changes nothing. One proposed below 32
/// carries no mark and applies as before, so older members agree.
#[tokio::test]
async fn a_create_marked_for_the_identity_waits_for_the_record() {
    let machine = machine_with_tenant().await;
    let marked = [
        MetaCommand::CreateStream {
            needs_identity: true,
            names: false,
            stream: stream("t-a", "ns-1", "s"),
            incarnations: false,
        },
        MetaCommand::CreateCache {
            needs_identity: true,
            names: false,
            cache: cache("t-a", "ns-1", "c"),
            incarnations: false,
        },
        MetaCommand::CreateResources {
            needs_identity: true,
            names: false,
            streams: vec![stream("t-a", "ns-1", "r")],
            caches: vec![],
            incarnations: false,
        },
    ];
    for command in marked.clone() {
        let err = machine.dispatch(command).await.expect_err("refused");
        assert!(matches!(err, MetaError::IdentityPending(_)), "{err:?}");
    }
    assert!(!machine.store().held_data_catalog().await);

    machine_with_tenant()
        .await
        .dispatch(MetaCommand::CreateStream {
            needs_identity: false,
            names: false,
            stream: stream("t-a", "ns-1", "s"),
            incarnations: false,
        })
        .await
        .expect("an entry from below level 32 applies");

    assert!(
        !answer(&machine, record("g-1", "hybrid"))
            .await
            .adopts_existing
    );
    for command in marked {
        machine.dispatch(command).await.expect("created");
    }
}

async fn machine_with_tenant() -> MetadataStateMachine {
    let machine = machine();
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t-a"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t-a", "ns-1"),
        },
    ] {
        machine.dispatch(command).await.expect("create");
    }
    machine
}
