use felix_broker_service::sim::ShardStand;

use super::*;

/// A broker that serves `shard` at `generation`, with everything agreeing.
fn serving(node: usize, generation: u64) -> Seen {
    Seen {
        node,
        name: format!("b{}", node + 1),
        stand: ShardStand {
            serving: true,
            generation: Some(generation),
            routed_leader: Some((format!("b{}", node + 1), generation)),
            opened_generation: Some(generation),
        },
    }
}

/// A broker with the shard open at `generation` that does not serve it.
fn open_not_serving(node: usize, generation: u64) -> Seen {
    let mut seen = serving(node, generation);
    seen.stand.serving = false;
    seen
}

/// A broker that knows nothing of the shard.
fn idle(node: usize) -> Seen {
    Seen {
        node,
        name: format!("b{}", node + 1),
        stand: ShardStand {
            serving: false,
            generation: None,
            routed_leader: None,
            opened_generation: None,
        },
    }
}

#[test]
fn two_brokers_opening_one_generation_are_two_leaders() {
    let mut check = OneLeaderPerGeneration::default();
    assert!(
        check
            .observe("stream/3", &[serving(0, 4), idle(1)])
            .is_empty()
    );
    // The first leader is gone by the time the second opens: the record of the
    // run still holds it.
    let found = check.observe("stream/3", &[idle(0), open_not_serving(1, 4)]);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].1.contains("b1 and b2"), "{found:?}");
    assert!(found[0].1.contains("generation 4"), "{found:?}");
}

#[test]
fn one_broker_reopening_a_generation_or_leaders_of_different_ones_are_not_two() {
    let mut check = OneLeaderPerGeneration::default();
    assert!(check.observe("stream/3", &[serving(0, 4)]).is_empty());
    assert!(check.observe("stream/3", &[serving(0, 4)]).is_empty());
    assert!(check.observe("stream/3", &[serving(1, 5)]).is_empty());
    // Another shard may be led by anyone at the same generation.
    assert!(check.observe("stream/4", &[serving(1, 4)]).is_empty());
}

#[test]
fn a_violation_is_reported_once_and_the_rest_are_counted() {
    let mut digest = Digest::new(2);
    assert!(digest.admit("a"));
    assert!(!digest.admit("a"));
    assert!(digest.admit("b"));
    assert!(!digest.admit("c"));
    assert!(!digest.admit("d"));
    let rest = digest.rest("example").unwrap();
    assert!(rest.summary.starts_with("2 more"), "{rest}");
    assert!(digest.rest("example").is_none());
}

#[test]
fn two_serving_brokers_violate_at_most_one_serving() {
    let mut check = AtMostOneServing::default();
    let found = check.observe("stream/1", &[serving(0, 4), serving(2, 5), idle(1)]);
    assert_eq!(found.len(), 1);
    assert!(
        found[0]
            .1
            .contains("b1 at generation 4 and b3 at generation 5"),
        "{found:?}"
    );
}

#[test]
fn one_or_no_serving_broker_satisfies_at_most_one_serving() {
    let mut check = AtMostOneServing::default();
    assert!(
        check
            .observe("stream/1", &[serving(0, 4), idle(1)])
            .is_empty()
    );
    assert!(check.observe("stream/1", &[idle(0), idle(1)]).is_empty());
    // Open but not serving (lease lapsed, say) does not count.
    assert!(
        check
            .observe("stream/1", &[serving(0, 4), open_not_serving(1, 4)])
            .is_empty()
    );
}

#[test]
fn serving_a_generation_that_was_not_opened_violates_adopted_matches_opened() {
    let mut check = AdoptedMatchesOpened::default();

    let mut never_opened = serving(0, 4);
    never_opened.stand.opened_generation = None;
    assert_eq!(check.observe("stream/1", &[never_opened]).len(), 1);

    let mut opened_earlier = serving(0, 4);
    opened_earlier.stand.opened_generation = Some(3);
    assert_eq!(check.observe("stream/1", &[opened_earlier]).len(), 1);

    let mut routed_elsewhere = serving(0, 4);
    routed_elsewhere.stand.routed_leader = Some(("b2".to_string(), 4));
    assert_eq!(check.observe("stream/1", &[routed_elsewhere]).len(), 1);

    let mut routed_at_another_generation = serving(0, 4);
    routed_at_another_generation.stand.routed_leader = Some(("b1".to_string(), 5));
    assert_eq!(
        check
            .observe("stream/1", &[routed_at_another_generation])
            .len(),
        1
    );
}

#[test]
fn a_broker_that_opened_what_it_serves_satisfies_adopted_matches_opened() {
    let mut check = AdoptedMatchesOpened::default();
    assert!(
        check
            .observe("stream/1", &[serving(0, 4), idle(1)])
            .is_empty()
    );
    // Not serving: nothing to compare.
    let mut behind = open_not_serving(1, 3);
    behind.stand.routed_leader = Some(("b1".to_string(), 4));
    assert!(check.observe("stream/1", &[behind]).is_empty());
}
