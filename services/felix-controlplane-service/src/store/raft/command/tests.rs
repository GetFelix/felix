//! What the leader rewrites before a command enters the log, and what it
//! leaves alone.
use super::*;

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
    };
    let encoded = encode_command(&MetaCommand::RecordReplicaReport {
        report: report.clone(),
        leader: Some("broker-a".to_string()),
    });

    let restamped = restamp(&encoded, 222).expect("a report carries a clock");
    match decode_command(&restamped).expect("decodes") {
        MetaCommand::RecordReplicaReport {
            report: stamped,
            leader,
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
    };
    let encoded = encode_command(&MetaCommand::RecordReplicaReport {
        report,
        leader: None,
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
        tenant_id: "acme".to_string(),
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
        tenant_id: "acme".to_string(),
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
