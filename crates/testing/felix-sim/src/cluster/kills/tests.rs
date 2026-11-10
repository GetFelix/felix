use super::*;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// One shard led by `leader`, with these members.
fn view(leader: &str, members: &[&str]) -> Views {
    BTreeMap::from([(
        "stream/0".to_string(),
        ShardView {
            leader: leader.to_string(),
            generation: 1,
            members: members.iter().map(|m| m.to_string()).collect(),
        },
    )])
}

/// `stream/0`'s copies on these nodes, up and trusted or not.
fn trust(copies: &[(&str, bool)]) -> Trust {
    copies
        .iter()
        .map(|(node, trusted)| {
            let copy = if *trusted {
                Copy::Trusted
            } else {
                Copy::Untrusted
            };
            (("stream/0".to_string(), node.to_string()), copy)
        })
        .collect()
}

fn held(log: &KillLog, views: &Views, trust: &Trust) -> Vec<String> {
    log.held(views, trust).into_iter().collect()
}

const THREE: [&str; 3] = ["b1", "b2", "b3"];

#[test]
fn a_leader_and_a_follower_killed_later_in_its_term_hold_the_shard() {
    // b1 led and was killed, then b3: both come back untrusted, whichever
    // died first.
    let log = KillLog::default();
    let copies = trust(&[("b1", false), ("b2", true), ("b3", false)]);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn a_learner_awaiting_readmission_counts_as_untrusted() {
    // b3 restarted unclean and is still a learner when its leader b2 is
    // killed: two of three copies cannot cover.
    let log = KillLog::default();
    let copies = trust(&[("b1", true), ("b2", false), ("b3", false)]);
    assert_eq!(held(&log, &view("b2", &THREE), &copies), ["stream/0"]);
}

#[test]
fn an_unserved_shard_with_trusted_copies_is_still_a_stall() {
    // An elected leader that cannot open (an empty storage id in its set, a
    // joint fence short of the old set) leaves the copies trusted.
    let log = KillLog::default();
    let copies = trust(&[("b1", true), ("b2", true), ("b3", false)]);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    assert!(held(&log, &view("b1", &THREE), &Trust::new()).is_empty());
}

#[test]
fn the_leader_counts_in_the_set_when_the_view_leaves_it_out() {
    // The control plane's replicas never name the leader.
    let log = KillLog::default();
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true)]);
    assert_eq!(
        held(&log, &view("b1", &["b2", "b3"]), &copies),
        ["stream/0"]
    );
}

#[test]
fn a_seat_replacement_in_flight_needs_a_majority_of_the_wider_set() {
    // b4 is joining: four copies, so two untrusted are not enough.
    let log = KillLog::default();
    let four = ["b2", "b3", "b4"];
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true), ("b4", true)]);
    assert!(held(&log, &view("b1", &four), &copies).is_empty());
    let copies = trust(&[("b1", false), ("b2", false), ("b3", false), ("b4", true)]);
    assert_eq!(held(&log, &view("b1", &four), &copies), ["stream/0"]);
}

#[test]
fn a_broker_outside_the_set_does_not_count() {
    let log = KillLog::default();
    let copies = trust(&[("b1", false), ("b4", false), ("b2", true), ("b3", true)]);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
}

#[test]
fn abandoning_the_log_ends_the_hold_until_the_next_unclean_stop() {
    let mut log = KillLog::default();
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true)]);
    log.kill("b2", secs(10));
    log.kill("b1", secs(15));
    log.abandon("stream/0", secs(20));
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    log.kill("b4", secs(30));
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    log.kill("b2", secs(35));
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn copies_untrusted_beside_a_leader_that_never_vouched_them_hold_the_shard() {
    // b2 and b3 came back from a power loss, and b1 led beside them but was
    // cut off or never readmitted b3 before it was killed. Two of three
    // copies are untrusted, so no attestation majority exists.
    let log = KillLog::default();
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::DownAfterKill);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn one_untrusted_copy_of_three_does_not_hold_the_shard() {
    let log = KillLog::default();
    let mut copies = trust(&[("b2", false), ("b3", true)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::Trusted);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
}

#[test]
fn a_copy_down_after_a_kill_counts_untrusted() {
    // b2 and the leader b1 were killed. b2's next start distrusts the copy
    // whatever a leader shipped it before.
    let mut log = KillLog::default();
    log.kill("b2", secs(25));
    log.kill("b1", secs(30));
    let copies = Trust::from([
        (("stream/0".into(), "b1".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b2".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b3".into()), Copy::Trusted),
    ]);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}
