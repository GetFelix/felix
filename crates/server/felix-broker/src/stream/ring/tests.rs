use super::*;

fn entry(seq: u64, len: usize) -> LogEntry {
    LogEntry {
        seq,
        payload: Bytes::from(vec![0u8; len]),
        publisher: None,
        timestamp_micros: None,
    }
}

fn push(ring: &mut Ring, seq: u64, len: usize, capacity: usize) -> bool {
    if !ring.admit(len) {
        return false;
    }
    ring.push_back(entry(seq, len));
    ring.trim(capacity);
    true
}

fn seqs(ring: &Ring) -> Vec<u64> {
    ring.iter().map(|entry| entry.seq).collect()
}

#[test]
fn the_per_ring_bound_drops_the_oldest_first() {
    let mut ring = Ring::new(&RingBytes::new(100, 0));
    for seq in 0..4 {
        assert!(push(&mut ring, seq, 40, 1024));
    }
    assert_eq!(seqs(&ring), [2, 3]);
    assert_eq!(ring.bytes(), 80);
}

#[test]
fn an_entry_over_the_per_ring_bound_is_kept_alone() {
    let mut ring = Ring::new(&RingBytes::new(100, 0));
    assert!(push(&mut ring, 0, 10, 1024));
    assert!(push(&mut ring, 1, 500, 1024));
    assert_eq!(seqs(&ring), [1]);
    assert!(push(&mut ring, 2, 10, 1024));
    assert_eq!(seqs(&ring), [2]);
}

#[test]
fn the_entry_count_still_applies() {
    let mut ring = Ring::new(&RingBytes::new(0, 0));
    for seq in 0..5 {
        assert!(push(&mut ring, seq, 1 << 20, 3));
    }
    assert_eq!(seqs(&ring), [2, 3, 4]);
}

#[test]
fn the_broker_budget_is_shared_and_a_ring_evicts_its_own_entries_for_room() {
    let bytes = RingBytes::new(0, 100);
    let budget = bytes.broker.clone().expect("bounded");
    let mut a = Ring::new(&bytes);
    let mut b = Ring::new(&bytes);
    assert!(push(&mut a, 0, 60, 1024));
    assert!(push(&mut b, 0, 30, 1024));
    // Ten bytes left: `b` makes room from its own entries, never from `a`'s.
    assert!(push(&mut b, 1, 30, 1024));
    assert_eq!(seqs(&a), [0]);
    assert_eq!(seqs(&b), [1]);
    assert_eq!(budget.available(), 10);
    drop(a);
    assert_eq!(budget.available(), 70);
    b.clear();
    assert_eq!(budget.available(), 100);
}

#[test]
fn a_ring_keeps_its_newest_batch_when_other_rings_fill_the_budget() {
    let bytes = RingBytes::new(0, 100);
    let budget = bytes.broker.clone().expect("bounded");
    let mut hot = Ring::new(&bytes);
    let mut quiet = Ring::new(&bytes);
    for seq in 0..10 {
        assert!(push(&mut hot, seq, 50, 1024));
    }
    assert_eq!(budget.available(), 0);
    assert!(push(&mut quiet, 0, 40, 1024));
    assert_eq!(seqs(&quiet), [0]);
    // The next batch replaces the overdrawn one rather than adding to it.
    assert!(push(&mut quiet, 1, 40, 1024));
    assert_eq!(seqs(&quiet), [1]);
    assert_eq!(seqs(&hot), [8, 9]);
    // The shortfall was never taken from the budget, so giving the batch
    // back returns nothing the other ring holds.
    quiet.clear();
    assert_eq!(budget.available(), 0);
    drop(hot);
    assert_eq!(budget.available(), 100);
}

#[test]
fn an_overdrawn_ring_takes_the_room_it_can_and_pays_back_the_rest_first() {
    let bytes = RingBytes::new(0, 100);
    let budget = bytes.broker.clone().expect("bounded");
    let mut other = Ring::new(&bytes);
    let mut ring = Ring::new(&bytes);
    assert!(push(&mut other, 0, 70, 1024));
    // 30 bytes free: the ring takes them and overdraws by 30.
    assert!(push(&mut ring, 0, 60, 1024));
    assert_eq!(budget.available(), 0);
    drop(other);
    assert_eq!(budget.available(), 70);
    // The next batch is admitted under the budget, with the old one evicted.
    assert!(push(&mut ring, 1, 60, 1024));
    assert_eq!(seqs(&ring), [1]);
    assert_eq!(budget.available(), 40);
    drop(ring);
    assert_eq!(budget.available(), 100);
}

#[test]
fn a_batch_larger_than_the_broker_budget_leaves_the_ring_empty() {
    let bytes = RingBytes::new(0, 100);
    let budget = bytes.broker.clone().expect("bounded");
    let mut other = Ring::new(&bytes);
    let mut ring = Ring::new(&bytes);
    assert!(push(&mut other, 0, 50, 1024));
    assert!(push(&mut ring, 0, 20, 1024));
    // No ring keeps a batch larger than the whole budget, so the ring is
    // left empty rather than keeping an entry below a missing one.
    assert!(!push(&mut ring, 1, 101, 1024));
    assert!(ring.is_empty());
    assert_eq!(budget.available(), 50);
    assert!(push(&mut ring, 2, 10, 1024));
    assert_eq!(seqs(&ring), [2]);
}
