use super::*;

fn outcome(first: u64) -> PublishOutcome {
    PublishOutcome {
        subscribers: 0,
        offsets: Some((first, first)),
    }
}

fn digest(sequence: u64) -> PayloadDigest {
    PayloadDigest::of([sequence.to_be_bytes()])
}

#[test]
fn a_new_producer_starts_at_zero_and_counts_up() {
    let table = ProducerTable::default();
    table.turn(7, 0).expect("first batch");
    assert_eq!(table.classify(7, 0).expect("classify"), Sequenced::Append);
    table.remember(7, 0, outcome(10), digest(0));
    assert_eq!(table.classify(7, 1).expect("classify"), Sequenced::Append);
}

#[test]
fn a_re_sent_batch_is_answered_from_memory() {
    let table = ProducerTable::default();
    table.turn(7, 0).expect("first batch");
    table.remember(7, 0, outcome(10), digest(0));
    table.remember(7, 1, outcome(11), digest(1));
    assert_eq!(
        table.classify(7, 0).expect("classify"),
        Sequenced::Duplicate(outcome(10), digest(0))
    );
    assert_eq!(
        table.classify(7, 1).expect("classify"),
        Sequenced::Duplicate(outcome(11), digest(1))
    );
}

#[test]
fn a_gap_names_what_was_expected() {
    let table = ProducerTable::default();
    table.turn(7, 0).expect("first batch");
    table.remember(7, 0, outcome(10), digest(0));
    match table.classify(7, 5) {
        Err(BrokerError::SequenceGap { expected }) => assert_eq!(expected, 1),
        other => panic!("expected a gap, got {other:?}"),
    }
}

#[test]
fn an_unknown_producer_may_only_begin_at_zero() {
    let table = ProducerTable::default();
    match table.turn(9, 3) {
        Err(BrokerError::UnknownProducer { producer_id }) => assert_eq!(producer_id, 9),
        other => panic!("expected unknown producer, got {other:?}"),
    }
    assert_eq!(table.len(), 0, "a refused producer was remembered");
}

#[test]
fn a_sequence_older_than_the_window_is_expired() {
    let table = ProducerTable::default();
    table.turn(7, 0).expect("first batch");
    for sequence in 0..(WINDOW as u64 + 1) {
        table.remember(7, sequence, outcome(sequence), digest(sequence));
    }
    assert!(matches!(
        table.classify(7, 0),
        Err(BrokerError::SequenceExpired { sequence: 0 })
    ));
    assert_eq!(
        table.classify(7, 1).expect("classify"),
        Sequenced::Duplicate(outcome(1), digest(1))
    );
}

#[test]
fn the_coldest_producer_makes_room() {
    let table = ProducerTable::default();
    for producer in 0..MAX_PRODUCERS as u64 {
        table.turn(producer, 0).expect("turn");
    }
    // Touch producer 0 so it is no longer the coldest.
    table.turn(0, 0).expect("turn");
    table.turn(u64::MAX, 0).expect("one more");
    assert_eq!(table.len(), MAX_PRODUCERS);
    assert!(
        table.classify(0, 0).is_ok(),
        "the warm producer was evicted"
    );
    assert!(
        matches!(
            table.classify(1, 0),
            Err(BrokerError::UnknownProducer { .. })
        ),
        "the coldest producer survived"
    );
}

/// The turn is per producer: one producer's batches wait on each other,
/// two producers' do not.
#[tokio::test]
async fn turns_are_per_producer() {
    let table = ProducerTable::default();
    let a = table.turn(1, 0).expect("turn");
    let a_again = table.turn(1, 1).expect("turn");
    let b = table.turn(2, 0).expect("turn");
    let held = a.lock().await;
    assert!(
        a_again.try_lock().is_err(),
        "the same producer got a second turn"
    );
    assert!(b.try_lock().is_ok(), "another producer waited");
    drop(held);
}
