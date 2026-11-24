use std::collections::BTreeSet;

use super::{EVERY_RUN, check, elected, parse, parse_stood, scan_dir};
use crate::history::checker::Rule;
use crate::history::model::Opened;

const COLOURED: &str = "\u{1b}[2m2026-11-02T10:00:00.000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
    \u{1b}[2mfelix_replication::driver\u{1b}[0m\u{1b}[2m:\u{1b}[0m a majority took the fence; \
    opening the shard for writes \u{1b}[3mstream\u{1b}[0m\u{1b}[2m=\u{1b}[0mhistory-1 \
    \u{1b}[3mshard\u{1b}[0m\u{1b}[2m=\u{1b}[0m0 \u{1b}[3mgeneration\u{1b}[0m\u{1b}[2m=\u{1b}[0m7 \
    \u{1b}[3mcaught_up_from\u{1b}[0m\u{1b}[2m=\u{1b}[0mNone";

fn opened(name: &str, generation: u64, node: &str) -> Opened {
    Opened {
        name: name.to_string(),
        shard: 0,
        generation,
        node: node.to_string(),
    }
}

#[test]
fn an_open_is_read_from_a_coloured_log_line() {
    assert_eq!(
        parse(COLOURED, "broker-2"),
        Some(opened("history-1", 7, "broker-2"))
    );
}

#[test]
fn other_lines_are_not_opens() {
    let line = "INFO felix_replication::driver: standing for election stream=history-1 shard=0 \
                generation=7";
    assert_eq!(parse(line, "broker-2"), None);
}

#[test]
fn two_brokers_opening_one_generation_is_a_violation() {
    let opens: BTreeSet<Opened> = [
        opened("history-0", 4, "broker-1"),
        opened("history-0", 5, "broker-2"),
        opened("history-0", 5, "broker-3"),
        // Another shard at the same generation is no conflict.
        opened("history-cache", 5, "broker-1"),
    ]
    .into_iter()
    .collect();
    let mut out = Vec::new();
    check(&opens, &mut out);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0].rule, Rule::OneLeaderPerGeneration);
    assert_eq!(out[0].list, "history-0");
    assert!(out[0].explanation.contains("generation 5"), "{out:?}");
}

#[test]
fn one_broker_opening_several_generations_is_not() {
    let opens: BTreeSet<Opened> = [
        opened("history-0", 5, "broker-2"),
        opened("history-0", 6, "broker-2"),
    ]
    .into_iter()
    .collect();
    let mut out = Vec::new();
    check(&opens, &mut out);
    assert!(out.is_empty(), "{out:?}");
}

/// A Raft member's open, as `felix_replication::raft::lead` logs it, with the
/// term as the generation.
const RAFT_OPENED: &str = "\u{1b}[2m2026-11-23T10:00:00.000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
    \u{1b}[2mfelix_replication::raft::lead\u{1b}[0m\u{1b}[2m:\u{1b}[0m a majority took the fence; \
    opening the shard for writes \u{1b}[3mstream\u{1b}[0m\u{1b}[2m=\u{1b}[0mhistory-1 \
    \u{1b}[3mshard\u{1b}[0m\u{1b}[2m=\u{1b}[0m0 \u{1b}[3mgeneration\u{1b}[0m\u{1b}[2m=\u{1b}[0m9";

#[test]
fn a_raft_open_is_read_from_its_log_line() {
    assert_eq!(
        parse(RAFT_OPENED, "broker-1"),
        Some(opened("history-1", 9, "broker-1"))
    );
}

#[test]
fn two_raft_members_logging_an_open_in_one_term_is_a_violation() {
    let one = tempfile::tempdir().unwrap();
    let two = tempfile::tempdir().unwrap();
    std::fs::write(one.path().join("broker.log"), RAFT_OPENED).unwrap();
    std::fs::write(two.path().join("broker.log.earlier"), RAFT_OPENED).unwrap();
    let mut opens = BTreeSet::new();
    scan_dir(one.path(), "broker-1", &EVERY_RUN, parse, &mut opens);
    scan_dir(two.path(), "broker-2", &EVERY_RUN, parse, &mut opens);
    let mut out = Vec::new();
    check(&opens, &mut out);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0].rule, Rule::OneLeaderPerGeneration);
    assert!(
        out[0]
            .explanation
            .contains("generation 9 by broker-1 and broker-2"),
        "{out:?}"
    );
}

const STOOD_LINE: &str = "INFO felix_replication::election::candidate: standing for election: the \
    leader is unreachable or stuck and a majority agreed stream=history-1 shard=0 generation=8 \
    leader=broker-0 broker_suspected=true";

#[test]
fn a_candidacy_is_read_from_its_log_line() {
    assert_eq!(
        parse_stood(STOOD_LINE, "broker-2"),
        Some(opened("history-1", 8, "broker-2"))
    );
    let held_back = "INFO felix_replication::election::candidate: not standing for election \
                     stream=history-1 shard=0 why=quorum answers=1";
    assert_eq!(parse_stood(held_back, "broker-2"), None);
    assert_eq!(parse_stood(COLOURED, "broker-2"), None);
}

#[test]
fn an_open_nobody_stood_for_is_not_an_election() {
    // broker-1 opened at 9 after a control-plane move; broker-2 won an
    // election at 8.
    let opens: BTreeSet<Opened> = [
        opened("history-1", 8, "broker-2"),
        opened("history-1", 9, "broker-1"),
    ]
    .into_iter()
    .collect();
    let stood: BTreeSet<Opened> = [opened("history-1", 8, "broker-2")].into_iter().collect();
    assert_eq!(
        elected(&opens, &stood, "history-1", 0, 7, "broker-0"),
        Some(&opened("history-1", 8, "broker-2"))
    );
    assert_eq!(
        elected(&opens, &BTreeSet::new(), "history-1", 0, 7, "broker-0"),
        None
    );
}

#[test]
fn every_run_of_a_broker_is_read() {
    // A heal that restarts a broker twice pushes the first run past
    // `broker.log.previous`, into `broker.log.earlier` only.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("broker.log.earlier"), COLOURED).unwrap();
    std::fs::write(dir.path().join("broker.log.previous"), "nothing opened\n").unwrap();
    std::fs::write(dir.path().join("broker.log"), "nothing opened\n").unwrap();
    let mut opens = BTreeSet::new();
    scan_dir(dir.path(), "broker-2", &EVERY_RUN, parse, &mut opens);
    assert_eq!(
        opens.into_iter().collect::<Vec<_>>(),
        vec![opened("history-1", 7, "broker-2")]
    );
}
