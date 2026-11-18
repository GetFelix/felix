use std::time::Duration;

use super::*;

fn facts() -> Facts {
    Facts {
        durable: true,
        quorum_consistency: false,
        replication_factor: Some(3),
        clustered: true,
        raft: false,
        commit_ack: false,
        ack_durable_on_commit: true,
        fsync: Some(FsyncMode::Periodic {
            interval: Duration::from_millis(250),
        }),
    }
}

#[test]
fn a_leader_stream_is_one_copy_synced_on_the_timer() {
    let said = describe(&facts());
    assert_eq!(said.ack_at, AckPoint::Write);
    assert_eq!(said.copies, AckCopies::Leader);
    assert_eq!(said.fsync, Some(AckFsync::Periodic));
}

#[test]
fn a_quorum_stream_is_a_majority_but_keeps_the_brokers_fsync_mode() {
    let said = describe(&Facts {
        quorum_consistency: true,
        ..facts()
    });
    assert_eq!(said.copies, AckCopies::Quorum);
    assert_eq!(said.fsync, Some(AckFsync::Periodic));
}

#[test]
fn a_raft_durable_stream_is_a_majority() {
    let said = describe(&Facts {
        raft: true,
        ack_durable_on_commit: false,
        ..facts()
    });
    assert_eq!(said.ack_at, AckPoint::Write);
    assert_eq!(said.copies, AckCopies::Quorum);
}

#[test]
fn a_replica_set_of_one_is_not_called_a_quorum() {
    let said = describe(&Facts {
        quorum_consistency: true,
        replication_factor: Some(1),
        ..facts()
    });
    assert_eq!(said.copies, AckCopies::Leader);
    assert_eq!(said.ack_at, AckPoint::Write);
}

#[test]
fn acks_on_enqueue_say_so() {
    let said = describe(&Facts {
        ack_durable_on_commit: false,
        ..facts()
    });
    assert_eq!(said.ack_at, AckPoint::Enqueue);
}

#[test]
fn a_client_that_asked_for_commit_acks_gets_write_on_any_stream() {
    let said = describe(&Facts {
        durable: false,
        commit_ack: true,
        ack_durable_on_commit: false,
        fsync: Some(FsyncMode::OnCommit),
        ..facts()
    });
    assert_eq!(said.ack_at, AckPoint::Write);
    assert_eq!(said.fsync, None, "an in-memory stream has no log to sync");
}

#[test]
fn on_commit_and_none_map_across() {
    let on_commit = describe(&Facts {
        fsync: Some(FsyncMode::OnCommit),
        ..facts()
    });
    assert_eq!(on_commit.fsync, Some(AckFsync::OnCommit));
    let none = describe(&Facts {
        fsync: Some(FsyncMode::None),
        ..facts()
    });
    assert_eq!(none.fsync, Some(AckFsync::None));
}

#[test]
fn a_majority_is_claimed_only_when_replication_is_known() {
    let quorum = Facts {
        quorum_consistency: true,
        ..facts()
    };
    let standalone = describe(&Facts {
        clustered: false,
        ..quorum
    });
    assert_eq!(standalone.copies, AckCopies::Leader);
    let unstated = describe(&Facts {
        replication_factor: None,
        ..quorum
    });
    assert_eq!(unstated.copies, AckCopies::Leader);
    let in_memory = describe(&Facts {
        durable: false,
        ..quorum
    });
    assert_eq!(in_memory.copies, AckCopies::Leader);
}
