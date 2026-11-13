//! `FELIX_SHARD_ENGINE` and what a Raft broker needs before it starts.

use crate::config::env::{RaftPrerequisites, ShardEngine, parse_shard_engine};

const ALL: RaftPrerequisites = RaftPrerequisites {
    compiled: true,
    membership: true,
    peer_transport: true,
    durable_storage: true,
};

#[test]
fn the_engine_is_hybrid_unless_raft_is_asked_for() {
    for unset in [None, Some(""), Some("hybrid"), Some(" Hybrid ")] {
        assert_eq!(
            parse_shard_engine(unset).expect("hybrid"),
            ShardEngine::Hybrid
        );
    }
    assert_eq!(
        parse_shard_engine(Some("RAFT")).expect("raft"),
        ShardEngine::Raft
    );
}

/// A typo must not read as the default: the broker would run the hybrid
/// engine beside peers that run Raft.
#[test]
fn an_unknown_engine_is_refused() {
    let err = parse_shard_engine(Some("rafts")).expect_err("unknown");
    assert!(format!("{err}").contains("hybrid or raft"), "{err}");
}

#[test]
fn a_raft_broker_missing_a_prerequisite_is_refused() {
    assert_eq!(ALL.missing(), None);
    let cases = [
        (
            RaftPrerequisites {
                compiled: false,
                ..ALL
            },
            "cargo feature",
        ),
        (
            RaftPrerequisites {
                membership: false,
                ..ALL
            },
            "FELIX_NODE_ID",
        ),
        (
            RaftPrerequisites {
                peer_transport: false,
                ..ALL
            },
            "FELIX_INTERNAL_BIND",
        ),
        (
            RaftPrerequisites {
                durable_storage: false,
                ..ALL
            },
            "FELIX_DURABLE_STORAGE_DIR",
        ),
    ];
    for (given, names) in cases {
        let why = given.missing().expect("refused");
        assert!(why.contains(names), "{why}");
    }
}
