//! What the leader rewrites before a command enters the log, and what it
//! leaves alone.
use super::*;

/// A retired op is answered as unsupported, the same on every member, and
/// never read as a live command.
#[test]
fn a_reserved_op_is_refused_by_name() {
    for op in RESERVED_OPS {
        let entry = format!(r#"{{"v":1,"op":"{op}"}}"#);
        match decode_command(entry.as_bytes()) {
            Err(MetaError::Unsupported(reason)) => {
                assert!(reason.contains("retired"), "{op}: {reason}")
            }
            other => panic!("{op} decoded: {other:?}"),
        }
    }
}

#[test]
fn a_heartbeat_is_restamped() {
    // Goes through `encode_command` rather than a hand-written string, so a
    // rename of the variant or its field breaks this instead of silently
    // turning `restamp` into a no-op.
    let encoded = encode_command(&MetaCommand::RecordNodeHeartbeat {
        node_id: "broker-1".to_string(),
        incarnation: 3,
        at_millis: 111,
    });

    let restamped = restamp(&encoded, 222).expect("a heartbeat carries a clock");
    match decode_command(&restamped).expect("decodes") {
        MetaCommand::RecordNodeHeartbeat {
            node_id,
            incarnation,
            at_millis,
        } => {
            assert_eq!(at_millis, 222);
            // Everything else survives the rewrite.
            assert_eq!(node_id, "broker-1");
            assert_eq!(incarnation, 3);
        }
        other => panic!("restamping changed the command: {other:?}"),
    }
}

#[test]
fn a_cutoff_is_not_a_clock() {
    // `expiry_before_millis` is already `now - timeout`. Substituting `now`
    // for it would expire every node in the cluster, so the leader must leave
    // it exactly as the sweep computed it.
    let encoded = encode_command(&MetaCommand::ExpireStaleNodes {
        expiry_before_millis: 500,
    });
    assert!(restamp(&encoded, 9_000).is_none());
}

#[test]
fn commands_without_a_clock_are_left_alone() {
    // `None` rather than a re-encoded copy: the proposer's exact bytes go to
    // the log, so a command from a newer build cannot be reshaped in passing.
    let encoded = encode_command(&MetaCommand::DeleteNode {
        node_id: "broker-1".to_string(),
        forget_reports: false,
    });
    assert!(restamp(&encoded, 9_000).is_none());
}

#[test]
fn a_command_this_build_cannot_read_is_proposed_unchanged() {
    // The forwarding path carries bytes, not types, so a rolling upgrade can
    // hand a leader a command from a build it does not know. Passing it
    // through untouched keeps the old behaviour; rewriting a shape we cannot
    // see would be the way to corrupt it.
    assert!(restamp(b"not json at all", 9_000).is_none());
    assert!(restamp(br#"{"v":99,"op":"something_new"}"#, 9_000).is_none());
}

#[test]
fn restamping_twice_is_the_same_as_once() {
    // A proposal may pass through an instance that believed it was the leader
    // and then forward to the real one.
    let encoded = encode_command(&MetaCommand::RecordNodeHeartbeat {
        node_id: "broker-1".to_string(),
        incarnation: 3,
        at_millis: 111,
    });
    let once = restamp(&encoded, 222).expect("restamps");
    let twice = restamp(&once, 222).expect("restamps again");
    assert_eq!(once, twice);
}

/// A replica report carries a clock reading for the same reason a heartbeat
/// does, and gets the same treatment: the leader's reading, not the
/// proposer's, is what enters the log.
#[test]
fn a_replica_report_is_restamped() {
    let report = crate::model::ReplicaReport {
        inherited_storage: Default::default(),
        given_up: false,
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: crate::model::ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        generation: 4,
        caught_up: ["broker-b".to_string()].into_iter().collect(),
        offsets: [("broker-b".to_string(), 10)].into_iter().collect(),
        reported_at_millis: 111,
        drained: false,
        leader_offset: None,
        halted: Default::default(),
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
    };
    let encoded = encode_command(&MetaCommand::RecordReplicaReport {
        report: report.clone(),
        leader: Some("broker-a".to_string()),
        readmit: false,
    });

    let restamped = restamp(&encoded, 222).expect("a report carries a clock");
    match decode_command(&restamped).expect("decodes") {
        MetaCommand::RecordReplicaReport {
            report: stamped,
            leader,
            ..
        } => {
            assert_eq!(leader.as_deref(), Some("broker-a"));
            assert_eq!(stamped.reported_at_millis, 222);
            assert_eq!(
                crate::model::ReplicaReport {
                    reported_at_millis: 111,
                    drained: false,
                    ..stamped
                },
                report,
                "something besides the clock changed",
            );
        }
        other => panic!("restamping changed the command: {other:?}"),
    }
}

/// A report entry written before the leadership check existed has no
/// `leader`, and must still decode: it applies unchecked, as it did then.
#[test]
fn a_report_without_a_leader_is_the_old_wire_form() {
    let report = crate::model::ReplicaReport {
        inherited_storage: Default::default(),
        given_up: false,
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: crate::model::ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        generation: 4,
        caught_up: Default::default(),
        offsets: Default::default(),
        reported_at_millis: 111,
        drained: false,
        leader_offset: None,
        halted: Default::default(),
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
    };
    let encoded = encode_command(&MetaCommand::RecordReplicaReport {
        report,
        leader: None,
        readmit: false,
    });
    let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json");
    assert!(json.get("leader").is_none(), "{json}");
    match decode_command(&encoded).expect("decodes") {
        MetaCommand::RecordReplicaReport { leader, .. } => assert_eq!(leader, None),
        other => panic!("decoded as {other:?}"),
    }
}

/// A stamped command still decodes to the same command. The id rides beside
/// it, not instead of it.
#[test]
fn a_request_id_does_not_disturb_the_command() {
    let encoded = encode_command(&MetaCommand::RecordNodeHeartbeat {
        node_id: "broker-1".to_string(),
        incarnation: 3,
        at_millis: 111,
    });
    let stamped = stamp_request_id(&encoded, "rid-1").expect("stamps");
    assert_eq!(request_id_of(&stamped).as_deref(), Some("rid-1"));
    match decode_command(&stamped).expect("decodes") {
        MetaCommand::RecordNodeHeartbeat {
            node_id,
            incarnation,
            at_millis,
        } => {
            assert_eq!(node_id, "broker-1");
            assert_eq!(incarnation, 3);
            assert_eq!(at_millis, 111);
        }
        other => panic!("wrong command: {other:?}"),
    }
}

/// **A stamped command is never restamped.** A forwarded proposal arrives
/// already carrying the id the client's instance gave it, and that is the id
/// the leader must deduplicate on -- a second one per hop would give one
/// logical write two identities and defeat the whole mechanism.
#[test]
fn an_already_stamped_command_keeps_its_id() {
    let encoded = encode_command(&MetaCommand::DeleteTenant {
        names: false,
        unassign: false,
        tenant_id: "acme".to_string(),
        take_shards: false,
        incarnations: false,
    });
    let stamped = stamp_request_id(&encoded, "first").expect("stamps");
    assert!(
        stamp_request_id(&stamped, "second").is_none(),
        "a second stamp must refuse rather than overwrite",
    );
    assert_eq!(request_id_of(&stamped).as_deref(), Some("first"));
}

/// An unstamped command carries no id, which is what a proposal from a peer
/// that predates this looks like.
#[test]
fn an_unstamped_command_has_no_request_id() {
    let encoded = encode_command(&MetaCommand::DeleteTenant {
        names: false,
        unassign: false,
        tenant_id: "acme".to_string(),
        take_shards: false,
        incarnations: false,
    });
    assert_eq!(request_id_of(&encoded), None);
    // And it must not appear on the wire at all, so an older peer sees the
    // bytes it has always seen.
    let json = std::str::from_utf8(&encoded).expect("utf8");
    assert!(!json.contains("rid"), "{json}");
}

/// The clock rewrite and the id are independent: restamping a heartbeat must
/// not drop the id it was proposed with.
#[test]
fn restamping_preserves_the_request_id() {
    let encoded = encode_command(&MetaCommand::RecordNodeHeartbeat {
        node_id: "broker-1".to_string(),
        incarnation: 3,
        at_millis: 111,
    });
    let stamped = stamp_request_id(&encoded, "rid-9").expect("stamps");
    let restamped = restamp(&stamped, 222).expect("a heartbeat carries a clock");
    assert_eq!(request_id_of(&restamped).as_deref(), Some("rid-9"));
}

/// The identity record needs every member to know the command.
#[test]
fn recording_the_cluster_identity_needs_level_32() {
    let record = MetaCommand::RecordClusterIdentity {
        group_id: "g".to_string(),
        shard_engine: "hybrid".to_string(),
    };
    assert_eq!(record.version(), 32);
}

/// A broker's root sequence needs every member to know the command.
#[test]
fn recording_a_root_sequence_needs_level_33() {
    let record = MetaCommand::RecordRootSequence {
        node_id: "b".to_string(),
        heard: crate::model::RootSequence {
            storage_id: "a".repeat(32),
            sequence: 1,
            incarnation: 1,
        },
    };
    assert_eq!(record.version(), 33);
}

/// A member that cannot apply a switch command would keep a hybrid shard
/// where the others hold a sealed or switched one.
#[test]
fn a_shard_switch_needs_level_34() {
    let key = crate::model::ShardKey {
        tenant_id: "t".to_string(),
        namespace: "n".to_string(),
        stream: "s".to_string(),
        shard: 0,
        kind: crate::model::ShardKind::Stream,
    };
    let seal = MetaCommand::SealShard {
        key: key.clone(),
        expected_generation: 1,
    };
    let side = crate::model::SealedSideLog {
        start: 0,
        end: 0,
        digest: "cd".repeat(32),
    };
    let report = MetaCommand::AcceptSealReport {
        key: key.clone(),
        report: crate::model::SealReport {
            generation: 1,
            offset: 0,
            start: 0,
            digest: "ab".repeat(32),
            dead_letters: side.clone(),
            cursors: side,
            voters: Default::default(),
        },
        sealer: "b".to_string(),
        keep_ordered: false,
    };
    let give_up = MetaCommand::GiveUpSeal {
        key: key.clone(),
        generation: 1,
        sealer: "b".to_string(),
    };
    // SWITCH_OPS is matched by name, so a renamed command would slip past
    // the level check without this.
    let copy_key = key;
    let ops: Vec<String> = [&seal, &report, &give_up]
        .into_iter()
        .map(|command| op_of(&encode_command(command)).expect("op"))
        .collect();
    assert_eq!(ops, SWITCH_OPS);
    let finalize = MetaCommand::FinalizeFleetFeature {
        feature: felix_common::fleet::SHARD_RAFT.name().to_string(),
        hold_unformed: false,
        hold_formed: false,
        keep_counter: false,
    };
    for command in [seal, report, give_up, finalize] {
        assert_eq!(command.version(), 34, "{command:?}");
        // The leader refuses it below the level, however it was proposed.
        let raw = encode_command(&command);
        let switch = !matches!(command, MetaCommand::FinalizeFleetFeature { .. });
        assert!(depends_on_level(&raw), "{command:?}");
        if switch {
            assert!(settle_for_level(&raw, 33).is_err(), "{command:?}");
            assert_eq!(settle_for_level(&raw, 34), Ok(None), "{command:?}");
        }
    }
    const { assert!(METADATA_VERSION >= 34) };
    let copy_id = encode_command(&MetaCommand::RecordRaftCopyId {
        key: copy_key,
        node_id: "b".to_string(),
        node_registered_at_millis: 1,
        node_incarnation: 1,
        copy_id: 1,
        keep_ordered: false,
        shard_incarnation: None,
        hold_target: false,
        group_epoch: None,
        keep_seed: false,
    });
    // Matched by name too.
    assert_eq!(op_of(&copy_id).as_deref(), Some(COPY_ID_OP));
    assert!(depends_on_level(&copy_id));
    assert!(settle_for_level(&copy_id, 35).is_err());
    assert_eq!(settle_for_level(&copy_id, 36), Ok(None));
}

/// **`joint_fence` is finalized only where a refusal report is kept.** A
/// leader whose fence cannot learn the set it leaves is renamed on its
/// refusal, which members before level 25 drop, so a group below it would
/// leave that shard waiting for good (#1872).
#[test]
fn joint_fence_waits_for_the_level_that_keeps_refusals() {
    let finalize = MetaCommand::FinalizeFleetFeature {
        feature: felix_common::fleet::JOINT_FENCE.name().to_string(),
        hold_unformed: false,
        hold_formed: false,
        keep_counter: false,
    };
    assert_eq!(finalize.version(), 25);
}

/// An import raises the placement incarnation only once every member applies
/// level 35, and one carrying an incarnation is refused below it.
#[tokio::test]
async fn an_import_raises_the_incarnation_from_level_35() {
    let import = br#"{"v":1,"op":"import_state","overwrite":true}"#;
    let level = super::super::PLACEMENT_INCARNATION_LEVEL;
    let settled = |level| -> serde_json::Value {
        let settled = settle_for_level(import, level)
            .expect("settles")
            .expect("rewritten");
        serde_json::from_slice(&settled).expect("json")
    };
    assert!(METADATA_VERSION >= level);
    assert!(settled(level - 1).get("raise_incarnation").is_none());
    assert_eq!(settled(level)["raise_incarnation"], true);
    let restored = super::super::RESTORED_ORDERED_LEVEL;
    assert!(METADATA_VERSION >= restored);
    assert!(settled(restored - 1).get("lose_ordered").is_none());
    assert_eq!(settled(restored)["lose_ordered"], true);

    let mut state = crate::store::memory::InMemoryStore::new(crate::store::StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    })
    .export_state()
    .await;
    state.placement_incarnation = 3;
    let carrying = encode_command(&MetaCommand::ImportState {
        state: Box::new(state),
        overwrite: true,
        mark_unplaced: false,
        raise_incarnation: false,
        lose_ordered: false,
    });
    assert!(settle_for_level(&carrying, level - 1).is_err());
    assert!(settle_for_level(&carrying, level).is_ok());
}

/// An escaped spelling of the import op is still an import, so it cannot
/// slip past the leader's level check.
#[test]
fn an_escaped_import_op_is_still_settled() {
    let escaped = br#"{"v":1,"op":"import\u005fstate","overwrite":true}"#;
    assert!(depends_on_level(escaped));
    assert!(settle_for_level(escaped, super::super::PLACED_LEVEL - 1).is_err());
    let settled = settle_for_level(escaped, super::super::PLACED_LEVEL)
        .expect("settles at the level")
        .expect("rewritten");
    let value: serde_json::Value = serde_json::from_slice(&settled).expect("json");
    assert_eq!(value["mark_unplaced"], true);
    assert!(!depends_on_level(
        br#"{"v":1,"op":"record_node_heartbeat"}"#
    ));
}

/// A node delete forgets the node's report stamps only once every member
/// applies level 37 (#2619); below it the delete is appended as sent.
#[test]
fn a_delete_forgets_report_stamps_from_level_37() {
    let delete = encode_command(&MetaCommand::DeleteNode {
        node_id: "broker-b".to_string(),
        forget_reports: false,
    });
    let level = super::super::FORGET_REPORTS_LEVEL;
    assert!(METADATA_VERSION >= level);
    assert!(depends_on_level(&delete));
    assert!(
        settle_for_level(&delete, level - 1)
            .expect("settles")
            .is_none()
    );
    let flagged = br#"{"v":1,"op":"delete_node","node_id":"broker-b","forget_reports":true}"#;
    let stripped = settle_for_level(flagged, level - 1)
        .expect("settles")
        .expect("rewritten");
    assert!(decode_command_with_level(&stripped).expect("decodes").1 < level);
    let settled = settle_for_level(&delete, level)
        .expect("settles")
        .expect("rewritten");
    let (settled, needs) = decode_command_with_level(&settled).expect("decodes");
    assert_eq!(needs, level);
    assert!(matches!(
        settled,
        MetaCommand::DeleteNode {
            forget_reports: true,
            ..
        }
    ));
}

/// A floor report is refused below level 38 however it reached the leader,
/// raw peer bytes included: a member that cannot apply it would keep no
/// floor where the others hold one.
#[test]
fn a_floor_report_is_refused_below_level_38() {
    let floors = encode_command(&MetaCommand::RecordRaftFloors {
        node_id: "b".to_string(),
        floors: vec![crate::model::RaftFloorReport {
            key: crate::model::ShardKey {
                tenant_id: "t".to_string(),
                namespace: "n".to_string(),
                stream: "s".to_string(),
                shard: 0,
                kind: crate::model::ShardKind::Stream,
            },
            incarnation: 0,
            epoch: 0,
            index: 1,
            term: 1,
            voters: None,
        }],
        mark_formed: false,
        vouched_voters: false,
        note_leader: false,
    });
    let level = super::super::RAFT_FLOOR_LEVEL;
    assert!(METADATA_VERSION >= level);
    // Matched by name too.
    assert_eq!(op_of(&floors).as_deref(), Some(FLOORS_OP));
    assert!(depends_on_level(&floors));
    let refused = settle_for_level(&floors, level - 1).expect_err("below the level");
    assert!(
        matches!(
            decode_result(&refused),
            Ok(Err(MetaError::Conflict(message))) if message.contains("offset floor")
        ),
        "{refused:?}"
    );
    assert_eq!(settle_for_level(&floors, level), Ok(None));
}

/// A copy id report and a seal report keep the epoch's ordered targets only
/// once every member applies level 39; a flag sent below it is taken off.
#[test]
fn ordered_targets_are_kept_from_level_39() {
    let level = super::super::RAFT_ORDERED_LEVEL;
    assert!(METADATA_VERSION >= level);
    for op in [COPY_ID_OP, ACCEPT_SEAL_OP] {
        let bare = format!(r#"{{"v":1,"op":"{op}"}}"#);
        let flag = |raw: &[u8], level| -> Option<serde_json::Value> {
            let settled = settle_for_level(raw, level).expect("settles")?;
            Some(serde_json::from_slice::<serde_json::Value>(&settled).expect("json")["keep_ordered"].clone())
        };
        assert_eq!(flag(bare.as_bytes(), level - 1), None, "{op}");
        assert_eq!(flag(bare.as_bytes(), level), Some(true.into()), "{op}");
        let sent = format!(r#"{{"v":1,"op":"{op}","keep_ordered":true}}"#);
        assert_eq!(
            flag(sent.as_bytes(), level - 1),
            Some(serde_json::Value::Null),
            "{op}"
        );
    }
}

/// A finalize settled at level 45 carries `hold_unformed`, so the entry
/// lifts the join floor to 45 (#2794); below it the field is left off.
#[test]
fn a_finalize_carries_the_hold_from_level_45() {
    let raw = encode_command(&MetaCommand::FinalizeFleetFeature {
        feature: felix_common::fleet::MAJORITY_ACK.name().to_string(),
        hold_unformed: false,
        hold_formed: false,
        keep_counter: false,
    });
    assert!(depends_on_level(&raw));
    assert_eq!(settle_for_level(&raw, 44), Ok(None));
    let settled = settle_for_level(&raw, 45)
        .expect("settled")
        .expect("edited");
    let (command, level) = decode_command_with_level(&settled).expect("decode");
    assert!(matches!(
        command,
        MetaCommand::FinalizeFleetFeature {
            hold_unformed: true,
            hold_formed: false,
            ..
        }
    ));
    assert_eq!(level, 45);
    let unsettled = settle_for_level(&settled, 44)
        .expect("settled")
        .expect("edited");
    assert_eq!(decode_command_with_level(&unsettled).expect("decode").1, 2);
    // From level 46 it carries the formed group's hold too (#2905).
    let settled = settle_for_level(&raw, 46)
        .expect("settled")
        .expect("edited");
    let (command, level) = decode_command_with_level(&settled).expect("decode");
    assert!(matches!(
        command,
        MetaCommand::FinalizeFleetFeature {
            hold_unformed: true,
            hold_formed: true,
            ..
        }
    ));
    assert_eq!(level, 46);
}

/// A copy id report settled at level 47 carries `keep_seed`, which lifts
/// the entry to 47; below it the flag is never added or kept.
#[test]
fn a_copy_id_report_keeps_the_seed_from_level_47() {
    let level = super::super::REFORM_RECORD_LEVEL;
    assert!(METADATA_VERSION >= level);
    let raw = encode_command(&MetaCommand::RecordRaftCopyId {
        key: crate::model::ShardKey {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        node_id: "b".to_string(),
        node_registered_at_millis: 1,
        node_incarnation: 1,
        copy_id: 1,
        keep_ordered: false,
        shard_incarnation: Some(1),
        hold_target: false,
        group_epoch: None,
        keep_seed: false,
    });
    let settled = settle_for_level(&raw, level)
        .expect("settled")
        .expect("edited");
    let (command, needs) = decode_command_with_level(&settled).expect("decode");
    assert!(matches!(
        command,
        MetaCommand::RecordRaftCopyId {
            keep_seed: true,
            ..
        }
    ));
    assert_eq!(needs, level);
    // Below 47 nothing adds it, and an entry that carries it is refused.
    let below = settle_for_level(&raw, level - 1)
        .expect("settled")
        .expect("edited");
    let (command, needs) = decode_command_with_level(&below).expect("decode");
    assert!(matches!(
        command,
        MetaCommand::RecordRaftCopyId {
            keep_seed: false,
            ..
        }
    ));
    assert!(needs < level);
    assert!(settle_for_level(&settled, level - 1).is_err());
    // One this build cannot decode has the flag taken off.
    let sent = format!(r#"{{"v":1,"op":"{COPY_ID_OP}","keep_seed":true}}"#);
    let stripped = settle_for_level(sent.as_bytes(), level - 1)
        .expect("settles")
        .expect("edited");
    let json: serde_json::Value = serde_json::from_slice(&stripped).expect("json");
    assert_eq!(json["keep_seed"], serde_json::Value::Null);
}

/// Epoch 0 is what a report without an epoch means, so it is written the
/// same as no epoch and does not lift the entry to level 47.
#[test]
fn a_copy_id_report_at_epoch_zero_is_the_old_wire_form() {
    let report = |group_epoch| {
        encode_command(&MetaCommand::RecordRaftCopyId {
            key: crate::model::ShardKey {
                tenant_id: "t".to_string(),
                namespace: "n".to_string(),
                stream: "s".to_string(),
                shard: 0,
                kind: crate::model::ShardKind::Stream,
            },
            node_id: "b".to_string(),
            node_registered_at_millis: 1,
            node_incarnation: 1,
            copy_id: 1,
            keep_ordered: false,
            shard_incarnation: Some(1),
            hold_target: false,
            group_epoch,
            keep_seed: false,
        })
    };
    assert_eq!(report(Some(0)), report(None));
    let (_, needs) = decode_command_with_level(&report(Some(0))).expect("decode");
    assert!(needs < super::super::REFORM_RECORD_LEVEL);
}

/// An audit record is refused below level 49: an older member would refuse
/// the entry as unknown and diverge from the members that applied it.
#[test]
fn an_audit_record_is_refused_below_level_49() {
    let record = encode_command(&MetaCommand::RecordAudit {
        entries: vec![crate::audit::AuditEntry {
            at_ms: 1,
            member: "1".to_string(),
            listener: "api".to_string(),
            peer: None,
            method: "POST".to_string(),
            route: "/v1/x".to_string(),
            path: "/v1/x".to_string(),
            tenant: None,
            subject: None,
            actor: None,
            cert_sha256: None,
            credential: "none".to_string(),
            status: 204,
            refusal: None,
            suppressed: 0,
        }],
    });
    let level = super::super::AUDIT_LEVEL;
    assert!(METADATA_VERSION >= level);
    assert_eq!(op_of(&record).as_deref(), Some(RECORD_AUDIT_OP));
    assert!(depends_on_level(&record));
    let refused = settle_for_level(&record, level - 1).expect_err("below the level");
    assert!(
        matches!(
            decode_result(&refused),
            Ok(Err(MetaError::Conflict(message))) if message.contains("audit record")
        ),
        "{refused:?}"
    );
    assert_eq!(settle_for_level(&record, level), Ok(None));
}

/// A group of one's self re-form is refused below level 55 for the same
/// reason: an older member would keep the old epoch where the others start
/// the next one.
#[test]
fn a_self_reform_epoch_start_is_refused_below_level_55() {
    let record = encode_command(&MetaCommand::StartAloneEpoch {
        key: crate::model::ShardKey {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        node_id: "a".to_string(),
        start: crate::model::ReformEpochStart {
            incarnation: 1,
            expected_epoch: 0,
            planned_floor: None,
            base: crate::model::RaftEpochBase {
                base_last: None,
                first_index: 1,
            },
            voters: [("a".to_string(), 7)].into(),
            dead: Default::default(),
            starts: Default::default(),
        },
    });
    assert_eq!(op_of(&record).as_deref(), Some(REFORM_ALONE_OP));
    assert!(depends_on_level(&record));
    let refused = settle_for_level(&record, 54).expect_err("below the level");
    assert!(
        matches!(
            decode_result(&refused),
            Ok(Err(MetaError::Conflict(message))) if message.contains("self re-form")
        ),
        "{refused:?}"
    );
    assert_eq!(settle_for_level(&record, 55), Ok(None));
}

/// Finalizing `raft_alone_reform` raises the group's floor to the level the
/// command it enables needs, so no member that cannot apply it stays.
#[test]
fn finalizing_the_self_reform_needs_level_55() {
    let finalize = MetaCommand::FinalizeFleetFeature {
        feature: felix_common::fleet::RAFT_ALONE_REFORM.name().to_string(),
        hold_unformed: false,
        hold_formed: false,
        keep_counter: false,
    };
    assert_eq!(finalize.version(), 55);
}

/// An abandon's epoch write is refused below level 56: an older member would
/// refuse the entry as unknown and keep the lost epoch while the others start
/// the next.
#[test]
fn an_abandon_epoch_start_is_refused_below_level_56() {
    let record = encode_command(&MetaCommand::AbandonRaftEpoch {
        key: crate::model::ShardKey {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        start: crate::model::AbandonEpochStart {
            incarnation: 1,
            expected_epoch: 0,
            expected_generation: 1,
            leader: "b".to_string(),
            planned_at_millis: 1,
            offset_gap: 1 << 40,
        },
    });
    assert_eq!(op_of(&record).as_deref(), Some(ABANDON_EPOCH_OP));
    assert!(depends_on_level(&record));
    let refused = settle_for_level(&record, 55).expect_err("below the level");
    assert!(
        matches!(
            decode_result(&refused),
            Ok(Err(MetaError::Conflict(message))) if message.contains("abandon")
        ),
        "{refused:?}"
    );
    assert_eq!(settle_for_level(&record, 56), Ok(None));
}

/// A re-form's epoch write is refused below level 51: an older member would
/// refuse the entry as unknown and keep the old epoch while the others start
/// the next one, and the brokers would then run two different groups.
#[test]
fn a_reform_epoch_start_is_refused_below_level_51() {
    let record = encode_command(&MetaCommand::StartReformedEpoch {
        key: crate::model::ShardKey {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            shard: 0,
            kind: crate::model::ShardKind::Stream,
        },
        start: crate::model::ReformEpochStart {
            incarnation: 1,
            expected_epoch: 0,
            planned_floor: None,
            base: crate::model::RaftEpochBase {
                base_last: None,
                first_index: 1,
            },
            voters: [("a".to_string(), 7)].into(),
            dead: Default::default(),
            starts: Default::default(),
        },
    });
    let level = super::super::REFORM_EPOCH_LEVEL;
    assert!(METADATA_VERSION >= level);
    assert_eq!(op_of(&record).as_deref(), Some(REFORM_EPOCH_OP));
    assert!(depends_on_level(&record));
    let refused = settle_for_level(&record, level - 1).expect_err("below the level");
    assert!(
        matches!(
            decode_result(&refused),
            Ok(Err(MetaError::Conflict(message))) if message.contains("re-form")
        ),
        "{refused:?}"
    );
    assert_eq!(settle_for_level(&record, level), Ok(None));
}
