//! Individual commands through the apply loop: what each publishes, what
//! it refuses, and how it survives the envelope.
use super::*;

/// The heartbeat rule crosses the command layer intact: liveness updates,
/// and the changefeed does not move.
#[tokio::test]
async fn a_heartbeat_command_publishes_no_change() {
    let machine = machine();
    machine
        .dispatch(MetaCommand::RegisterNode {
            node: node("broker-1", 7_001),
        })
        .await
        .expect("register");
    let before = machine.store().node_changes(0).await.expect("changes");

    let response = machine
        .dispatch(MetaCommand::RecordNodeHeartbeat {
            node_id: "broker-1".to_string(),
            incarnation: 1,
            at_millis: 9_999,
        })
        .await
        .expect("heartbeat");
    let MetaResponse::Node { node } = response else {
        panic!("heartbeat answers with the node");
    };
    assert_eq!(node.status.last_heartbeat_at_millis, 9_999);

    let after = machine.store().node_changes(0).await.expect("changes");
    assert_eq!(
        before.next_seq, after.next_seq,
        "a heartbeat that published a change would evict real membership \
         events from the retention window",
    );
}

/// A conditional assignment write that finds another generation answers
/// stale through the byte layer and changes nothing.
#[tokio::test]
async fn a_stale_conditional_assignment_write_is_answered_not_applied() {
    let machine = machine();
    for command in [
        MetaCommand::CreateTenant {
            tenant: tenant("t1"),
        },
        MetaCommand::CreateNamespace {
            namespace: namespace("t1", "ns"),
        },
        MetaCommand::CreateStream {
            stream: stream("t1", "ns", "orders"),
        },
        MetaCommand::RegisterNode {
            node: node("broker-1", 7_001),
        },
        MetaCommand::RegisterNode {
            node: node("broker-2", 7_002),
        },
    ] {
        machine.dispatch(command).await.expect("setup");
    }
    let first = shard_assignment("t1", "ns", "orders", "broker-1");
    machine
        .dispatch(MetaCommand::PutShardAssignmentIf {
            assignment: first.clone(),
            expected_generation: None,
        })
        .await
        .expect("create");

    let late = MetaCommand::PutShardAssignmentIf {
        assignment: shard_assignment("t1", "ns", "orders", "broker-2"),
        expected_generation: None,
    };
    let response = crate::raft::AppStateMachine::apply(&machine, &encode_command(&late)).await;
    assert!(matches!(
        decode_result(&response).expect("decodes"),
        Ok(MetaResponse::StaleAssignment {
            current_generation: Some(0)
        })
    ));
    let stored = machine
        .store()
        .get_shard_assignment(&first.key)
        .await
        .expect("get");
    assert_eq!(stored.leader, "broker-1");
}

/// A command from a newer build is refused loudly and identically on every
/// replica — never skipped, which would fork state.
#[tokio::test]
async fn a_newer_command_version_is_refused_not_skipped() {
    let machine = machine();
    let mut envelope = serde_json::to_value(serde_json::json!({
        "v": COMMAND_VERSION + 1,
        "op": "create_tenant",
        "tenant": {"tenant_id": "t1", "display_name": "One"}
    }))
    .expect("envelope");
    envelope["v"] = serde_json::json!(COMMAND_VERSION + 1);
    let bytes = serde_json::to_vec(&envelope).expect("bytes");

    let response = crate::raft::AppStateMachine::apply(&machine, &bytes).await;
    let result = decode_result(&response).expect("decodes");
    assert!(matches!(result, Err(MetaError::Unsupported(_))));
    assert!(
        machine
            .store()
            .list_tenants()
            .await
            .expect("list")
            .is_empty(),
        "a refused command must change nothing",
    );
}

/// A variant added by a newer build arrives at the current version with an
/// `op` this build has never heard of. It is refused the same way and leaves
/// the snapshot byte-identical, so every member on this build still agrees.
#[tokio::test]
async fn an_unknown_command_variant_is_refused_without_touching_state() {
    let machine = machine();
    let before = crate::raft::AppStateMachine::snapshot(&machine).await;
    let bytes = serde_json::to_vec(&serde_json::json!({
        "v": COMMAND_VERSION,
        "op": "some_future_operation",
        "tenant_id": "t1"
    }))
    .expect("bytes");

    let response = crate::raft::AppStateMachine::apply(&machine, &bytes).await;
    let result = decode_result(&response).expect("decodes");
    assert!(
        matches!(result, Err(MetaError::Unsupported(_))),
        "{result:?}"
    );
    assert_eq!(
        crate::raft::AppStateMachine::snapshot(&machine).await,
        before,
        "a refused command must change nothing"
    );
}

/// The import command is the migration cutover: refused against a store
/// with any history unless the operator explicitly overwrites, and exact
/// when it lands — including the sequence positions broker watches resume
/// from.
#[tokio::test]
async fn an_import_replaces_everything_and_respects_the_guard() {
    // A populated source, exported the way the migration tool exports
    // Postgres: through the traits, sequence heads carried, windows empty.
    let source = machine();
    run_script(&source).await;
    let exported = crate::store::export::export_state_from(
        source.store().as_ref() as &(dyn crate::store::ControlPlaneAuthStore + Send + Sync)
    )
    .await
    .expect("export");
    let source_head = source
        .store()
        .shard_assignment_changes(0)
        .await
        .expect("changes")
        .next_seq;

    // A fresh store accepts it without ceremony.
    let fresh = machine();
    fresh
        .dispatch(MetaCommand::ImportState {
            state: Box::new(exported.clone()),
            overwrite: false,
        })
        .await
        .expect("import into unused store");
    assert_eq!(
        serde_json::to_vec(
            &fresh
                .store()
                .stream_snapshot()
                .await
                .expect("snap")
                .items
                .len()
        )
        .expect("len"),
        serde_json::to_vec(
            &source
                .store()
                .stream_snapshot()
                .await
                .expect("snap")
                .items
                .len()
        )
        .expect("len"),
    );

    // A broker checkpointed at the head continues with no resnapshot: an
    // empty page whose next_seq equals its checkpoint is "nothing new".
    let at_head = fresh
        .store()
        .shard_assignment_changes(source_head)
        .await
        .expect("changes");
    assert!(at_head.items.is_empty());
    assert_eq!(at_head.next_seq, source_head);

    // A broker behind the head gets the ordinary eviction signal — empty
    // page, next_seq ahead — and resnapshots exactly once.
    let behind = fresh.store().tenant_changes(0).await.expect("changes");
    assert!(behind.items.is_empty());
    assert!(behind.next_seq > 0, "the head must survive the migration");

    // A store with history refuses the import without overwrite...
    let used = machine();
    used.dispatch(MetaCommand::CreateTenant {
        tenant: tenant("t-existing"),
    })
    .await
    .expect("create");
    let refused = used
        .dispatch(MetaCommand::ImportState {
            state: Box::new(exported.clone()),
            overwrite: false,
        })
        .await;
    assert!(matches!(refused, Err(MetaError::Conflict(_))));

    // ...and replaces everything when the operator says so — the restore
    // ceremony.
    used.dispatch(MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: true,
    })
    .await
    .expect("overwrite import");
    assert!(
        used.store()
            .tenant_exists("t-existing")
            .await
            .map(|exists| !exists)
            .expect("exists"),
        "an overwrite import leaves nothing of the old state"
    );
}

/// The round trip every command takes: encode → decode is identity, so the
/// leader and its followers apply the same value.
#[tokio::test]
async fn commands_round_trip_through_the_envelope() {
    for command in script() {
        let decoded = decode_command(&encode_command(&command)).expect("round trip");
        assert_eq!(
            serde_json::to_value(&decoded).expect("value"),
            serde_json::to_value(&command).expect("value"),
        );
    }
}

/// A newer build's field on a command this build knows is refused, not
/// dropped: applying the rest would store a different stream than the
/// leader did (a jump-hash stream as modulo, say) with nothing to show it.
#[tokio::test]
async fn a_field_this_build_does_not_know_is_refused_not_dropped() {
    let machine = machine();
    machine
        .dispatch(MetaCommand::CreateTenant {
            tenant: tenant("t1"),
        })
        .await
        .expect("tenant");
    machine
        .dispatch(MetaCommand::CreateNamespace {
            namespace: namespace("t1", "ns"),
        })
        .await
        .expect("namespace");
    let before = crate::raft::AppStateMachine::snapshot(&machine).await;

    let mut envelope: serde_json::Value =
        serde_json::from_slice(&encode_command(&MetaCommand::CreateStream {
            stream: stream("t1", "ns", "orders"),
        }))
        .expect("json");
    envelope["stream"]["placement_hint"] = serde_json::json!("rack-aware");
    let bytes = serde_json::to_vec(&envelope).expect("bytes");

    let response = crate::raft::AppStateMachine::apply(&machine, &bytes).await;
    let result = decode_result(&response).expect("decodes");
    assert!(
        matches!(&result, Err(MetaError::Unsupported(message)) if message.contains("stream.placement_hint")),
        "{result:?}"
    );
    assert_eq!(
        crate::raft::AppStateMachine::snapshot(&machine).await,
        before,
        "a refused command must change nothing"
    );

    // The same field left empty says nothing this build misses.
    envelope["stream"]["placement_hint"] = serde_json::Value::Null;
    let bytes = serde_json::to_vec(&envelope).expect("bytes");
    let response = crate::raft::AppStateMachine::apply(&machine, &bytes).await;
    decode_result(&response)
        .expect("decodes")
        .expect("an empty unknown field is applied");
}
