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
        order.register(id, None);
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
    order.register(1, None);
    order.register(2, None);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(ids(&ready), vec![2]);
}

#[test]
fn anything_but_a_registered_publish_answer_goes_straight_through() {
    let order = AckOrder::new();
    order.enable();
    order.register(1, None);
    let mut ready = Vec::new();
    order.release(Outgoing::Message(Message::Ok), &mut ready);
    order.release(ok(9), &mut ready);
    assert_eq!(ready.len(), 2);
}

#[test]
fn a_reused_request_id_is_matched_to_its_oldest_open_registration() {
    let order = AckOrder::new();
    order.enable();
    order.register(5, None);
    order.register(6, None);
    order.register(5, None);
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
    order.register(1, Some(Arc::clone(&window).try_acquire_owned().unwrap()));
    order.register(2, Some(Arc::clone(&window).try_acquire_owned().unwrap()));
    assert_eq!(window.available_permits(), 0);
    let mut ready = Vec::new();
    order.release(ok(2), &mut ready);
    assert_eq!(window.available_permits(), 0, "2 is parked, not written");
    order.release(ok(1), &mut ready);
    assert_eq!(window.available_permits(), 2);
}
