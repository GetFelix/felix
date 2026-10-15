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
fn a_batch_the_broker_budget_cannot_hold_leaves_the_ring_empty() {
    let bytes = RingBytes::new(0, 100);
    let budget = bytes.broker.clone().expect("bounded");
    let mut other = Ring::new(&bytes);
    let mut ring = Ring::new(&bytes);
    assert!(push(&mut other, 0, 50, 1024));
    assert!(push(&mut ring, 0, 20, 1024));
    // Even with its own entry gone this ring cannot find 60 bytes, so it is
    // left empty rather than keeping an entry below a missing one.
    assert!(!push(&mut ring, 1, 60, 1024));
    assert!(ring.is_empty());
    assert_eq!(budget.available(), 50);
    assert!(push(&mut ring, 2, 10, 1024));
    assert_eq!(seqs(&ring), [2]);
}
