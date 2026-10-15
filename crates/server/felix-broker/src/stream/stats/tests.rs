use super::{SubscriberOwner, SubscriberStats};

#[test]
fn a_new_subscriber_has_no_position_and_no_drops() {
    let stats = SubscriberStats::default();
    assert_eq!(stats.position(), None);
    assert_eq!(stats.dropped_records(), 0);
    assert!(stats.owner().is_none());
}

#[test]
fn drops_add_up_and_the_position_follows_the_latest_batch() {
    let stats = SubscriberStats::default();
    stats.dropped(3);
    stats.dropped(5);
    stats.taken_below(10);
    stats.taken_below(0);
    assert_eq!(stats.dropped_records(), 8);
    assert_eq!(stats.position(), Some(0));
}

#[test]
fn the_first_owner_sticks() {
    let stats = SubscriberStats::default();
    let owner = |connection_id| SubscriberOwner {
        subscription_id: 1,
        connection_id,
        peer: "127.0.0.1:1".to_string(),
        principal: None,
    };
    stats.set_owner(owner(7));
    stats.set_owner(owner(8));
    assert_eq!(stats.owner().map(|owner| owner.connection_id), Some(7));
}
