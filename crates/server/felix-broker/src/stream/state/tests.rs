//! Who a subscriber id refers to, and for how long.
//!
//! Both unregister paths work from an id captured earlier and neither holds a
//! lock across the gap: the publish fanout reaps senders it found closed after
//! releasing the log lock, and a subscription guard unregisters when it drops.
//! So the question these ask is whether an id can come to mean a different
//! subscriber than the one the caller meant — because if it can, the victim is
//! a subscriber that did nothing wrong and cannot tell it was unsubscribed.
use super::*;
use crate::stream::SubQueuePolicy;

fn stream() -> StreamState {
    StreamState::new(
        1,
        16,
        SubQueuePolicy::DropNew,
        None,
        crate::broker::ConsistencyLevel::Leader,
    )
}

fn registered_ids(state: &StreamState) -> Vec<u64> {
    state
        .subscriber_snapshot()
        .iter()
        .map(|entry| entry.id)
        .collect()
}

#[test]
fn a_departed_subscribers_id_is_not_handed_to_the_next_one() {
    let state = stream();

    let (first, _first_rx) = state.register_subscriber();
    state.remove_subscriber(first);
    let (second, _second_rx) = state.register_subscriber();

    assert_ne!(
        second, first,
        "the new subscriber inherited the departed one's id, so any reap still \
         holding that id would remove the wrong subscriber"
    );
}

#[test]
fn reaping_a_departed_subscriber_does_not_remove_its_successor() {
    // The interleaving in full: a publish captures a snapshot containing A,
    // A goes away, B registers, and only then does the publish reap what it
    // found closed. B is live and did nothing wrong.
    let state = stream();

    let (a, a_rx) = state.register_subscriber();
    let snapshot = state.subscriber_snapshot();
    assert_eq!(registered_ids(&state), vec![a]);

    // A departs: its receiver closes the channel, its guard unregisters.
    drop(a_rx);
    state.remove_subscriber(a);

    let (b, _b_rx) = state.register_subscriber();

    // The publish finishes and reaps the sender it found closed — A's, taken
    // from the snapshot it captured before any of this happened.
    let closed: Vec<u64> = snapshot
        .iter()
        .filter(|entry| entry.sender.is_closed())
        .map(|entry| entry.id)
        .collect();
    assert_eq!(closed, vec![a], "the snapshot should have found A closed");
    state.remove_subscribers(&closed);

    assert_eq!(
        registered_ids(&state),
        vec![b],
        "reaping the departed subscriber unregistered the live one; it still \
         holds a subscription that will never deliver again, and nothing tells \
         it so"
    );
}

#[test]
fn a_guard_dropping_late_does_not_unregister_its_successor() {
    // The same hazard from the other direction. A subscription's receiver is
    // dropped before its guard — they are separate fields, and `into_parts`
    // hands them out separately — so a reap can free the registration in
    // between, and the guard's own unregister then arrives after someone else
    // has registered.
    let state = stream();

    let (a, a_rx) = state.register_subscriber();
    drop(a_rx);
    // The publish path reaps A on the next fanout, before A's guard runs.
    state.remove_subscribers(&[a]);

    let (b, _b_rx) = state.register_subscriber();

    // Now A's guard finally drops.
    state.remove_subscriber(a);

    assert_eq!(
        registered_ids(&state),
        vec![b],
        "a late guard drop unregistered a subscriber that registered after it"
    );
}

#[test]
fn the_fanout_snapshot_keeps_a_stable_order() {
    // Not a correctness guarantee — delivery order across subscribers never
    // was one — but an order that reshuffles between publishes turns a fanout
    // timing difference into a hunt through whatever changed last.
    let state = stream();
    let mut keep = Vec::new();
    for _ in 0..8 {
        let (id, rx) = state.register_subscriber();
        keep.push((id, rx));
    }

    let ids = registered_ids(&state);
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);

    // And it survives a removal from the middle.
    let victim = keep.remove(3).0;
    state.remove_subscriber(victim);
    let ids = registered_ids(&state);
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);
    assert!(!ids.contains(&victim));
}

#[test]
fn append_batch_keeps_monotonic_sequences_and_trims_once() {
    let stream = StreamState::new(1, 8, SubQueuePolicy::DropNew, None, Default::default());
    let first = vec![
        Bytes::from_static(b"a"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"c"),
        Bytes::from_static(b"d"),
        Bytes::from_static(b"e"),
    ];
    stream.append_batch(&first, 3);
    let second = vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")];
    stream.append_batch(&second, 3);

    let state = stream.log_state.lock();
    let seqs = state.log.iter().map(|entry| entry.seq).collect::<Vec<_>>();
    let payloads = state
        .log
        .iter()
        .map(|entry| entry.payload.clone())
        .collect::<Vec<_>>();

    assert_eq!(state.next_seq, 7);
    assert_eq!(seqs, vec![4, 5, 6]);
    assert_eq!(
        payloads,
        vec![
            Bytes::from_static(b"e"),
            Bytes::from_static(b"f"),
            Bytes::from_static(b"g")
        ]
    );
}

/// Ending subscribers closes every channel, including the clones the fanout
/// snapshot holds, after what was already queued.
#[tokio::test]
async fn ending_subscribers_drains_then_closes_each_one() {
    let state = stream();
    let (_first, mut first_rx) = state.register_subscriber();
    let (_second, mut second_rx) = state.register_subscriber();
    let (snapshot, _) = state
        .append_batch_at(
            &[Bytes::from_static(b"queued")],
            None,
            None,
            16,
            None,
            None,
            None,
        )
        .expect("no turn to supersede");
    for entry in snapshot.iter() {
        let envelope = crate::stream::delivery::DeliveryEnvelope::with_base_offset(
            &[Bytes::from_static(b"queued")],
            None,
        );
        entry
            .sender
            .try_send(crate::stream::delivery::QueuedDelivery::new(
                envelope,
                std::sync::Arc::clone(&state.queued_items),
            ))
            .expect("room in the queue");
    }
    drop(snapshot);

    assert_eq!(state.end_subscribers(None), 2);

    for rx in [&mut first_rx, &mut second_rx] {
        assert!(rx.recv().await.is_some(), "queued delivery was dropped");
        assert!(rx.recv().await.is_none(), "the subscription should end");
    }
    assert_eq!(state.subscriber_count(), 0);
    assert!(registered_ids(&state).is_empty());
}
