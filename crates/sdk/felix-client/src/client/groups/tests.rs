use super::*;

/// **A named poll is refused by a broker that predates member names**, since
/// it would drop the name and a reclaim would quietly do nothing. An unnamed
/// poll is unaffected.
#[test]
fn a_named_poll_needs_the_broker_to_record_members() {
    let member = GroupMember {
        consumer: "snapshotter".to_string(),
        reclaim: true,
    };
    let without = felix_wire::KNOWN_FEATURES & !felix_wire::FEATURE_GROUP_CONSUMER;

    let refused = require_member_support(without, Some(&member)).expect_err("refused");
    assert!(refused.to_string().contains("does not record which member"));
    require_member_support(without, None).expect("an unnamed poll is fine");
    require_member_support(felix_wire::KNOWN_FEATURES, Some(&member)).expect("supported");
}

/// A group with no cursor starts at offset 0, so everything up to the tail is
/// still ahead of it.
#[test]
fn lag_counts_from_the_cursor_or_the_start() {
    let info = GroupInfo {
        committed: Some(4),
        tail: 10,
        in_flight: 0,
        owed: 0,
        dead_letters: 0,
    };
    assert_eq!(info.lag(), 6);
    assert_eq!(
        GroupInfo {
            committed: None,
            ..info
        }
        .lag(),
        10
    );
}

/// **A delayed nack or a chosen visibility is refused by a broker that
/// predates them.** It would ignore the field and nack at once, or claim for
/// its own timeout, without saying so.
#[test]
fn claim_control_needs_the_broker_to_advertise_it() {
    let without = felix_wire::KNOWN_FEATURES & !felix_wire::FEATURE_GROUP_CLAIM_CONTROL;

    let refused = require_claim_control(without).expect_err("refused");
    assert!(refused.to_string().contains("cannot extend a claim"));
    require_claim_control(felix_wire::KNOWN_FEATURES).expect("supported");
}
