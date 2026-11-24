use super::*;

fn change(offset: u64, key: &str) -> Change {
    Change {
        offset,
        key: key.into(),
        value: Some(Bytes::from_static(b"v")),
        expires_at: 0,
    }
}

fn offsets(changes: Option<Vec<Change>>) -> Option<Vec<u64>> {
    changes.map(|changes| changes.into_iter().map(|change| change.offset).collect())
}

fn all(_: &str) -> bool {
    true
}

/// **A replay starts at the change at the resume offset**, the first one
/// the client has not seen, and skips keys the filter does not take.
#[test]
fn a_replay_starts_at_the_resume_offset() {
    let mut recent = Recent::default();
    for (offset, key) in [(2, "a"), (3, "b"), (5, "a"), (8, "a")] {
        recent.push(change(offset, key));
    }
    assert_eq!(offsets(recent.since(5, &all)), Some(vec![5, 8]));
    assert_eq!(offsets(recent.since(4, &all)), Some(vec![5, 8]));
    assert_eq!(offsets(recent.since(0, &all)), Some(vec![2, 3, 5, 8]));
    assert_eq!(offsets(recent.since(9, &all)), Some(vec![]));
    assert_eq!(
        offsets(recent.since(3, &|key| key == "a")),
        Some(vec![5, 8])
    );
}

/// **Below the floor there is no replay**: the change just past the last
/// one dropped is the lowest a replay can start from, and it starts there
/// exactly. Dropping by count and by bytes move the floor alike.
#[test]
fn a_replay_from_below_the_floor_is_refused() {
    let mut recent = Recent::bounded(3, usize::MAX);
    for offset in 1..=5 {
        recent.push(change(offset * 10, "k"));
    }
    // 10 and 20 dropped: the floor is 21.
    assert_eq!(offsets(recent.since(20, &all)), None);
    assert_eq!(offsets(recent.since(21, &all)), Some(vec![30, 40, 50]));

    let one = change(1, "k").cost();
    let mut recent = Recent::bounded(usize::MAX, 2 * one);
    for offset in 1..=4 {
        recent.push(change(offset, "k"));
    }
    assert_eq!(offsets(recent.since(2, &all)), None);
    assert_eq!(offsets(recent.since(3, &all)), Some(vec![3, 4]));
}

/// **A value bigger than the byte bound is not kept**, and the floor moves
/// past it, so a resume from it resnapshots rather than misses it.
#[test]
fn an_oversized_change_moves_the_floor_past_it() {
    let mut recent = Recent::bounded(usize::MAX, 100);
    recent.push(change(1, "k"));
    recent.push(Change {
        value: Some(Bytes::from(vec![0; 200])),
        ..change(2, "k")
    });
    assert_eq!(offsets(recent.since(2, &all)), None);
    assert_eq!(offsets(recent.since(3, &all)), Some(vec![]));
}

/// **A restart forgets everything at or below its index**: the state was
/// replaced, so a replay from there would miss what the snapshot folded.
#[test]
fn a_restart_forgets_every_change() {
    let mut recent = Recent::default();
    recent.push(change(4, "k"));
    recent.restart(9);
    assert_eq!(offsets(recent.since(4, &all)), None);
    assert_eq!(offsets(recent.since(9, &all)), None);
    assert_eq!(offsets(recent.since(10, &all)), Some(vec![]));
    recent.restart(0);
    assert_eq!(offsets(recent.since(0, &all)), Some(vec![]));
}

/// **Recording that starts late starts its floor there**: a resume from
/// before it was never kept and resnapshots.
#[test]
fn recording_started_late_replays_only_from_its_start() {
    let mut recent = Recent::starting_at(7);
    recent.push(change(7, "k"));
    assert_eq!(offsets(recent.since(6, &all)), None);
    assert_eq!(offsets(recent.since(7, &all)), Some(vec![7]));
}

/// **The ring never holds more than its count bound**, so its slots stay
/// allocated for at most that many changes.
#[test]
fn the_ring_holds_at_most_its_count_bound() {
    let mut recent = Recent::bounded(4, usize::MAX);
    for offset in 1..=100 {
        recent.push(change(offset, "k"));
        assert!(recent.changes.len() <= 4);
    }
    assert!(
        recent.changes.capacity() < 8,
        "{}",
        recent.changes.capacity()
    );
}
