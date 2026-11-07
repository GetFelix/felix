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
fn a_copy_up_while_a_leader_served_and_still_untrusted_is_a_stall() {
    // b2 and b3 restarted unclean, then b1 served beside them before it was
    // killed: a leader had the chance to vouch both, so their doubt is the
    // broker's to clear, not a reason to hold the shard.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.started_unclean("b3", secs(12));
    log.served("stream/0", "b2", secs(20));
    log.served("stream/0", "b3", secs(20));
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::DownAfterKill);
    let views = view("b1", &THREE);
    assert!(held(&log, &views, &copies).is_empty());
    assert_eq!(
        log.vouchable_untrusted(&views, &copies)
            .into_iter()
            .collect::<Vec<_>>(),
        ["stream/0"]
    );
    // Served only before b3's latest unclean start: b3 had no leader since.
    log.started_unclean("b3", secs(30));
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
}

#[test]
fn a_copy_down_after_a_kill_counts_untrusted_though_a_leader_served_it_before() {
    // b2 restarted unclean, a leader served beside it, then b2 and the
    // leader b1 were killed. b2's next start distrusts the copy anyway.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.served("stream/0", "b1", secs(20));
    log.served("stream/0", "b2", secs(20));
    log.kill("b2", secs(25));
    log.kill("b1", secs(30));
    let copies = Trust::from([
        (("stream/0".into(), "b1".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b2".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b3".into()), Copy::Trusted),
    ]);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}
