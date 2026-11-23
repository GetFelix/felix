//! `FELIX_SHARD_ENGINE` and what a Raft broker needs before it starts.

use std::time::Duration;

use crate::config::env::{
    RaftPrerequisites, ShardEngine, parse_shard_engine, parse_shard_raft_elections_at_once,
    parse_shard_raft_lease,
};

const ALL: RaftPrerequisites = RaftPrerequisites {
    compiled: true,
    membership: true,
    peer_transport: true,
    durable_storage: true,
};

#[test]
fn the_setting_names_an_engine_or_none() {
    for unset in [None, Some(""), Some("  ")] {
        assert_eq!(parse_shard_engine(unset).expect("unset"), None);
    }
    for hybrid in [Some("hybrid"), Some(" Hybrid ")] {
        assert_eq!(
            parse_shard_engine(hybrid).expect("hybrid"),
            Some(ShardEngine::Hybrid)
        );
    }
    assert_eq!(
        parse_shard_engine(Some("RAFT")).expect("raft"),
        Some(ShardEngine::Raft)
    );
}

/// A clustered, durable broker with no engine setting runs Raft.
#[test]
fn a_broker_with_no_engine_setting_runs_raft() {
    assert_eq!(ALL.resolve(None).expect("resolve"), ShardEngine::Raft);
    let hybrid = Some(ShardEngine::Hybrid);
    assert_eq!(ALL.resolve(hybrid).expect("asked"), ShardEngine::Hybrid);
}

/// A broker that cannot run Raft falls back to the hybrid path when it was
/// not asked for an engine, and is refused when it was asked for Raft.
#[test]
fn a_broker_that_cannot_run_raft_runs_hybrid_unless_raft_was_asked_for() {
    let standalone = RaftPrerequisites {
        membership: false,
        peer_transport: false,
        ..ALL
    };
    let in_memory = RaftPrerequisites {
        durable_storage: false,
        ..ALL
    };
    for broker in [standalone, in_memory] {
        assert_eq!(broker.resolve(None).expect("resolve"), ShardEngine::Hybrid);
        let err = broker
            .resolve(Some(ShardEngine::Raft))
            .expect_err("refused");
        assert!(
            err.to_string()
                .contains("FELIX_SHARD_ENGINE=raft is refused"),
            "{err}"
        );
    }
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

/// Unset keeps the runtime's bound; `0` takes it off; anything that is not
/// a whole number is refused rather than read as the default.
#[test]
fn the_raft_election_bound_is_the_default_unless_set() {
    for unset in [None, Some(""), Some("  ")] {
        let parsed = parse_shard_raft_elections_at_once(unset).expect("unset");
        assert_eq!(parsed, None);
    }
    let off = parse_shard_raft_elections_at_once(Some("0")).expect("zero");
    assert_eq!(off, Some(0));
    let set = parse_shard_raft_elections_at_once(Some(" 16 ")).expect("set");
    assert_eq!(set, Some(16));
    for bad in ["off", "1.5", "-1"] {
        let err = parse_shard_raft_elections_at_once(Some(bad)).expect_err("not a number");
        let err = format!("{err}");
        assert!(err.contains("FELIX_SHARD_RAFT_ELECTIONS_AT_ONCE"), "{err}");
    }
}
