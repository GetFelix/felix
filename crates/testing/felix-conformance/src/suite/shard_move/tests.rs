use felix_wire::Message;

use super::*;

fn moved() -> Message {
    Message::ShardMoved {
        subscription_id: 1,
        resume_from: None,
        node_id: Some("next".to_string()),
        addr: None,
        generation: 2,
    }
}

fn event() -> Received {
    Received::Event(b"before-move".to_vec())
}

#[test]
fn ensure_moved_tail_wants_the_event_then_the_frame() {
    ensure_moved_tail(&[event(), Received::Message(moved())], &moved()).expect("ok");
}

#[test]
fn ensure_moved_tail_rejects_a_missing_or_lost_frame() {
    // Not sent.
    assert!(ensure_moved_tail(&[event()], &moved()).is_err());
    // The last event lost on the way out.
    assert!(ensure_moved_tail(&[Received::Message(moved())], &moved()).is_err());
}
