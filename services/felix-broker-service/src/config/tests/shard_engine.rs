//! `FELIX_SHARD_ENGINE` and what a Raft broker needs before it starts.

use std::time::Duration;

use crate::config::env::{
    RaftPrerequisites, ShardEngine, parse_shard_engine, parse_shard_raft_lease,
};

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

#[test]
fn the_raft_lease_is_the_default_unless_set() {
    for unset in [None, Some(""), Some("  ")] {
        assert_eq!(parse_shard_raft_lease(unset).expect("unset"), None);
    }
    let off = parse_shard_raft_lease(Some("0")).expect("zero");
    assert_eq!(off, Some(Duration::ZERO));
    let set = parse_shard_raft_lease(Some(" 750 ")).expect("set");
    assert_eq!(set, Some(Duration::from_millis(750)));
}

/// Read as the default, a mistyped `0` would leave the lease on where the
/// operator turned it off.
#[test]
fn a_raft_lease_that_is_not_a_number_is_refused() {
    for bad in ["off", "1.5s", "-1"] {
        let err = parse_shard_raft_lease(Some(bad)).expect_err("not a number");
        assert!(
            format!("{err}").contains("FELIX_SHARD_RAFT_LEASE_MS"),
            "{err}"
        );
    }
}
