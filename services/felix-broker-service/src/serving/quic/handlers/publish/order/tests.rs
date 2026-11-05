use std::sync::Arc;

use felix_wire::Message;
use tokio::sync::Semaphore;

use super::AckOrder;
use crate::serving::quic::handlers::publish::Outgoing;

fn ok(request_id: u64) -> Outgoing {
    Outgoing::Message(Message::PublishOk {
        request_id,
        offset: None,
    })
}

fn binary_ok(request_id: u64) -> Outgoing {
    Outgoing::PublishAck {
        request_id,
        error: None,
        code: None,
        detail: None,
        forwarded_to: None,
        offset: None,
    }
}

fn ids(ready: &[Outgoing]) -> Vec<u64> {
    ready
        .iter()
        .map(|outgoing| match outgoing {
            Outgoing::PublishAck { request_id, .. } => *request_id,
            Outgoing::Message(Message::PublishOk { request_id, .. }) => *request_id,
            other => panic!("not a publish answer: {other:?}"),
        })
        .collect()
}

#[test]
fn a_later_answer_waits_for_the_one_read_before_it() {
    let order = AckOrder::new();
    order.enable();
    for id in [1, 2, 3] {
        order.register(id, false, None);
    }
    let mut ready = Vec::new();
    order.release(binary_ok(3), &mut ready);
    order.release(ok(2), &mut ready);
    assert!(ready.is_empty(), "answered ahead of request 1");
    assert!(order.blocked_since().is_some());
    order.release(binary_ok(1), &mut ready);
    assert_eq!(ids(&ready), vec![1, 2, 3]);
    assert!(order.blocked_since().is_none());
}

#[test]
fn a_stream_that_did_not_ask_is_answered_in_completion_order() {
    let order = AckOrder::new();
    order.register(1, false, None);
    order.register(2, false, None);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(ids(&ready), vec![2]);
}

#[test]
fn anything_but_a_registered_publish_answer_goes_straight_through() {
    let order = AckOrder::new();
    order.enable();
    order.register(1, false, None);
    let mut ready = Vec::new();
    order.release(Outgoing::Message(Message::Ok), &mut ready);
    order.release(ok(9), &mut ready);
    assert_eq!(ready.len(), 2);
}

#[test]
fn a_reused_request_id_is_matched_to_its_oldest_open_registration() {
    let order = AckOrder::new();
    order.enable();
    order.register(5, false, None);
    order.register(6, false, None);
    order.register(5, false, None);
    let mut ready = Vec::new();
    order.release(ok(5), &mut ready);
    assert_eq!(ids(&ready), vec![5]);
    order.release(ok(5), &mut ready);
    assert_eq!(ids(&ready), vec![5], "second 5 is still behind 6");
    order.release(ok(6), &mut ready);
    assert_eq!(ids(&ready), vec![5, 6, 5]);
}

#[test]
fn a_window_permit_is_held_until_the_answer_is_released() {
    let window = Arc::new(Semaphore::new(2));
    let order = AckOrder::new();
    order.enable();
    order.register(
        1,
        false,
        Some(Arc::clone(&window).try_acquire_owned().unwrap()),
    );
    order.register(
        2,
        false,
        Some(Arc::clone(&window).try_acquire_owned().unwrap()),
    );
    assert_eq!(window.available_permits(), 0);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(window.available_permits(), 0, "2 is parked, not written");
    order.release(ok(1), &mut ready);
    assert_eq!(window.available_permits(), 2);
}

fn on_completion() -> AckOrder {
    let order = AckOrder::new();
    order.enable();
    order.answer_on_completion();
    order
}

#[test]
fn on_completion_a_plain_answer_does_not_wait_for_an_earlier_one() {
    let order = on_completion();
    for id in [1, 2, 3] {
        order.register(id, false, None);
    }
    let mut ready = Vec::new();
    order.release(binary_ok(3), &mut ready);
    order.release(ok(2), &mut ready);
    assert_eq!(ids(&ready), vec![3, 2]);
    assert!(order.blocked_since().is_some(), "1 is still unanswered");
    order.release(binary_ok(1), &mut ready);
    assert_eq!(ids(&ready), vec![3, 2, 1]);
    assert!(order.blocked_since().is_none());
}

#[test]
fn on_completion_idempotent_answers_keep_request_order_among_themselves() {
    let order = on_completion();
    order.register(1, true, None);
    order.register(2, false, None);
    order.register(3, true, None);
    let mut ready = Vec::new();
    order.release(ok(3), &mut ready);
    assert!(
        ready.is_empty(),
        "idempotent 3 answered ahead of idempotent 1"
    );
    assert!(order.blocked_since().is_some());
    order.release(ok(2), &mut ready);
    assert_eq!(
        ids(&ready),
        vec![2],
        "a plain answer waits for no idempotent one"
    );
    order.release(ok(1), &mut ready);
    assert_eq!(ids(&ready), vec![2, 1, 3]);
    assert!(order.blocked_since().is_none());
}

#[test]
fn on_completion_an_idempotent_answer_does_not_wait_for_a_plain_one() {
    let order = on_completion();
    order.register(1, false, None);
    order.register(2, true, None);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(ids(&ready), vec![2]);
    // Nothing is held, but the plain 1 still has a slot and is still timed.
    assert!(order.blocked_since().is_some());
}

/// A lost plain answer holds nothing back but keeps its window slot, so it
/// is timed like a held one.
#[test]
fn on_completion_an_unanswered_plain_publish_is_timed_alone() {
    let order = on_completion();
    assert!(order.blocked_since().is_none());
    order.register(1, false, None);
    assert!(order.blocked_since().is_some());
    let mut ready = Vec::new();
    order.release(ok(1), &mut ready);
    assert!(order.blocked_since().is_none());
}

#[test]
fn on_completion_a_window_permit_is_held_until_the_answer_is_written() {
    let window = Arc::new(Semaphore::new(2));
    let order = on_completion();
    order.register(
        1,
        false,
        Some(Arc::clone(&window).try_acquire_owned().unwrap()),
    );
    order.register(
        2,
        false,
        Some(Arc::clone(&window).try_acquire_owned().unwrap()),
    );
    assert_eq!(window.available_permits(), 0);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(
        window.available_permits(),
        1,
        "2 is written, 1 still holds a slot"
    );
    order.release(ok(1), &mut ready);
    assert_eq!(window.available_permits(), 2);
}
