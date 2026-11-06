use super::*;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn sets(members: &[&str]) -> ReplicaSets {
    let set: BTreeSet<String> = members.iter().map(|m| m.to_string()).collect();
    BTreeMap::from([("stream/0".to_string(), set)])
}

fn held(log: &KillLog) -> Vec<String> {
    log.held().into_iter().collect()
}

#[test]
fn overlapping_kills_of_two_members_hold_the_shard() {
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2", "b3"]));
    log.kill("b1", secs(10));
    log.kill("b2", secs(15));
    log.restart("b1", secs(20));
    log.restart("b2", secs(30));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn disjoint_windows_do_not() {
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2", "b3"]));
    log.kill("b1", secs(10));
    log.restart("b1", secs(20));
    log.kill("b2", secs(20));
    log.restart("b2", secs(30));
    assert!(held(&log).is_empty());
}

#[test]
fn a_kill_still_open_overlaps_a_later_one() {
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2", "b3"]));
    log.kill("b1", secs(10));
    log.kill("b3", secs(50));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn a_kill_of_a_broker_outside_the_set_does_not() {
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2"]));
    log.kill("b1", secs(10));
    log.kill("b3", secs(12));
    assert!(held(&log).is_empty());
}

#[test]
fn a_member_that_joined_during_the_overlap_counts() {
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2"]));
    log.kill("b1", secs(10));
    log.kill("b3", secs(12));
    log.sample(secs(15), sets(&["b1", "b3"]));
    log.restart("b1", secs(20));
    log.restart("b3", secs(25));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn one_kill_alone_holds_nothing() {
    // A clean stop never reaches the log, so a kill beside one is alone.
    let mut log = KillLog::default();
    log.sample(secs(0), sets(&["b1", "b2", "b3"]));
    log.kill("b1", secs(10));
    log.restart("b1", secs(30));
    assert!(held(&log).is_empty());
}
