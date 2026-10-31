use super::*;

fn entry(offset: Offset, floor: Offset, headers: &'static str) -> HeadEntry {
    HeadEntry {
        offset,
        floor,
        headers: Bytes::from_static(headers.as_bytes()),
    }
}

fn head(entries: &[(Offset, Offset, &'static str)]) -> Head {
    Head::new(
        entries
            .iter()
            .map(|(offset, floor, headers)| entry(*offset, *floor, headers))
            .collect(),
    )
}

/// Only entries below the bound count, the largest floor wins, and among
/// entries with that floor the first one set it.
#[test]
fn the_floor_is_the_largest_applied_and_its_first_setter() {
    let mut head = head(&[
        (4, 3, "a"),
        (6, 5, "b"),
        (8, 2, "c"),
        (9, 5, "d"),
        (12, 7, "e"),
    ]);
    assert_eq!(head.floor(), 0);
    assert!(head.has_pending());

    head.apply(6);
    assert_eq!(head.floor(), 3);
    head.apply(10);
    assert_eq!(head.floor(), 5);
    assert_eq!(head.setter().map(|e| e.offset), Some(6), "the first with 5");
    assert!(head.has_pending());

    head.apply(13);
    assert_eq!(head.floor(), 7);
    assert!(!head.has_pending());
}

/// A truncation forgets the entries it cuts, which can lower the floor.
#[test]
fn truncation_forgets_the_entries_it_cuts() {
    let mut head = head(&[(4, 3, "a"), (6, 5, "b")]);
    head.apply(10);
    head.truncate_from(6);
    assert_eq!(head.floor(), 3);
    assert_eq!(head.entries().len(), 1);
    head.truncate_from(0);
    assert_eq!(head.floor(), 0);
    assert!(!head.has_pending());
}

/// Deleted records take their entries with them, and an entry the bound has
/// not passed stays pending.
#[test]
fn pruning_forgets_deleted_entries() {
    let mut head = head(&[(4, 3, "a"), (6, 5, "b"), (9, 8, "c")]);
    head.apply(7);
    head.prune_below(5);
    assert_eq!(head.floor(), 5);
    assert_eq!(head.entries().len(), 2);
    assert!(head.has_pending());
    head.apply(10);
    assert_eq!(head.floor(), 8);
}

/// Deletion stops at the floor, and at the record that set it when that
/// record sits below the floor, so the floor can always be read back.
#[test]
fn deletion_stops_at_the_floor_and_its_record() {
    let mut below = head(&[(4, 3, "a")]);
    assert_eq!(below.delete_bound(), None, "nothing applies yet");
    below.apply(5);
    assert_eq!(below.delete_bound(), Some(3));

    let mut past = head(&[(4, 9, "a")]);
    past.apply(5);
    assert_eq!(past.floor(), 9);
    assert_eq!(past.delete_bound(), Some(4));
}

#[test]
fn the_file_round_trips() {
    let entries = vec![entry(4, 3, "a"), entry(6, 5, ""), entry(9, 8, "headers")];
    assert_eq!(decode(&encode(17, &entries)), Some((17, entries)));
    assert_eq!(decode(&encode(0, &[])), Some((0, Vec::new())));
}

/// A file that is short, damaged anywhere, or longer than it says is not
/// used.
#[test]
fn a_damaged_file_is_not_used() {
    let bytes = encode(17, &[entry(4, 3, "a"), entry(6, 5, "headers")]);
    for len in 0..bytes.len() {
        assert_eq!(decode(&bytes[..len]), None, "cut to {len}");
    }
    for at in 0..bytes.len() {
        let mut flipped = bytes.clone();
        flipped[at] ^= 0x40;
        assert_eq!(decode(&flipped), None, "flipped at {at}");
    }
    let mut longer = bytes[..bytes.len() - HEAD_CRC_LEN].to_vec();
    longer.push(0);
    let crc = crate::segment::format::crc32(&[&longer]);
    longer.extend_from_slice(&crc.to_be_bytes());
    assert_eq!(decode(&longer), None, "a trailing byte");
}
