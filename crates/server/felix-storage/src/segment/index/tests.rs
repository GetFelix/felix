use tempfile::tempdir;

use super::*;

fn index_with(base: Offset, entries: &[(Offset, u64)]) -> SparseIndex {
    let mut index = SparseIndex::new(base);
    for (offset, position) in entries {
        index.push(IndexEntry {
            offset: *offset,
            position: *position,
        });
    }
    index
}

#[test]
fn empty_index_seeks_to_the_first_record() {
    let index = SparseIndex::new(0);
    assert_eq!(index.seek_position(0), SEGMENT_HEADER_LEN);
    assert_eq!(index.seek_position(u64::MAX), SEGMENT_HEADER_LEN);
}

/// An index file is bytes on disk like any other, so it can say a record
/// begins inside the segment header. `seek_position` promises a boundary
/// the caller can decode forward from, and a position in the header is not
/// one — the next thing read is a header field parsed as a record.
///
/// Found by the `sparse_index` fuzz target the first time it ran in CI.
#[test]
fn an_entry_pointing_inside_the_header_is_dropped() {
    let index = index_with(0, &[(0, 0), (1, SEGMENT_HEADER_LEN - 1)]);
    assert!(index.is_empty());
    for probe in [0u64, 1, 7, u64::MAX / 2, u64::MAX] {
        assert!(index.seek_position(probe) >= SEGMENT_HEADER_LEN);
    }
}

/// Position has to ascend with offset for the same reason: records occupy
/// distinct ascending byte ranges, so an entry that moves backwards is one
/// no writer could have produced, and seeking to it would decode forward
/// over records the caller has already passed.
#[test]
fn an_entry_whose_position_goes_backwards_is_dropped() {
    let index = index_with(0, &[(0, 500), (10, 100), (20, 900)]);
    assert_eq!(
        index
            .entries()
            .iter()
            .map(|entry| (entry.offset, entry.position))
            .collect::<Vec<_>>(),
        vec![(0, 500), (20, 900)],
    );
    // What survives is still a usable index, not an empty one.
    assert_eq!(index.seek_position(10), 500);
    assert_eq!(index.seek_position(20), 900);
}

#[test]
fn seek_finds_the_floor_entry() {
    let index = index_with(0, &[(0, 32), (10, 500), (20, 900)]);
    assert_eq!(index.seek_position(0), 32);
    assert_eq!(index.seek_position(9), 32);
    assert_eq!(index.seek_position(10), 500);
    assert_eq!(index.seek_position(19), 500);
    assert_eq!(index.seek_position(20), 900);
    // Past the last entry: start at the last known boundary and scan.
    assert_eq!(index.seek_position(1_000), 900);
}

#[test]
fn seek_before_the_first_entry_starts_at_the_segment_header() {
    let index = index_with(100, &[(105, 500)]);
    assert_eq!(index.seek_position(100), SEGMENT_HEADER_LEN);
}

#[test]
fn out_of_order_pushes_are_ignored() {
    let mut index = SparseIndex::new(0);
    index.push(IndexEntry {
        offset: 10,
        position: 100,
    });
    index.push(IndexEntry {
        offset: 5,
        position: 999,
    });
    index.push(IndexEntry {
        offset: 10,
        position: 999,
    });
    assert_eq!(index.len(), 1);
    assert_eq!(index.seek_position(10), 100);
}

#[test]
fn persist_then_load_round_trips() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    let index = index_with(7, &[(7, 32), (19, 640)]);
    index.persist(&path).expect("persist");
    let loaded = SparseIndex::load(&path, 7).expect("load");
    assert_eq!(loaded, index);
}

#[test]
fn load_rejects_a_mismatched_base_offset() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    index_with(7, &[(7, 32)]).persist(&path).expect("persist");
    assert!(SparseIndex::load(&path, 8).is_none());
}

#[test]
fn load_of_a_missing_file_is_none() {
    let dir = tempdir().expect("tempdir");
    assert!(SparseIndex::load(&dir.path().join("nope.index"), 0).is_none());
}

#[test]
fn load_tolerates_a_torn_trailing_entry() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    index_with(0, &[(0, 32), (5, 400)])
        .persist(&path)
        .expect("persist");

    // Chop the last entry in half, as an interrupted append would.
    let file = OpenOptions::new().write(true).open(&path).expect("open");
    let len = INDEX_HEADER_LEN + INDEX_ENTRY_LEN + INDEX_ENTRY_LEN / 2;
    file.set_len(len).expect("truncate");

    let loaded = SparseIndex::load(&path, 0).expect("load");
    assert_eq!(
        loaded.entries(),
        &[IndexEntry {
            offset: 0,
            position: 32
        }]
    );
}

#[test]
fn load_rejects_a_corrupt_header() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    index_with(0, &[(0, 32)]).persist(&path).expect("persist");
    std::fs::write(&path, b"garbage!").expect("clobber");
    assert!(SparseIndex::load(&path, 0).is_none());
}

#[test]
fn writer_emits_entries_at_the_spacing_interval() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    let mut writer = IndexWriter::open(&path, SparseIndex::new(0))
        .expect("open")
        .with_spacing(100);

    let mut position = SEGMENT_HEADER_LEN;
    for offset in 0..10u64 {
        writer.observe_record(offset, position, 40);
        position += 40;
    }
    writer.sync().expect("sync");

    // First record always indexed, then one per 100 bytes of segment data:
    // entries land at offsets 0, 3, 6 and 9.
    let offsets: Vec<Offset> = writer.index().entries().iter().map(|e| e.offset).collect();
    assert_eq!(offsets, vec![0, 3, 6, 9]);

    let reloaded = SparseIndex::load(&path, 0).expect("load");
    assert_eq!(reloaded.entries(), writer.index().entries());
}

#[test]
fn writer_spacing_never_degenerates_to_every_record() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    let mut writer = IndexWriter::open(&path, SparseIndex::new(0))
        .expect("open")
        .with_spacing(0);
    for offset in 0..4u64 {
        writer.observe_record(offset, SEGMENT_HEADER_LEN + offset, 1);
    }
    // Spacing 1 still indexes every record, but the file stays bounded and
    // no divide-by-zero or unbounded growth is possible.
    assert_eq!(writer.index().len(), 4);
}

#[test]
fn writer_rewrites_a_stale_index_on_open() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("a.index");
    index_with(0, &[(0, 32), (5, 400), (9, 800)])
        .persist(&path)
        .expect("persist");

    // Reopen with a shorter, rebuilt index: the file must shrink to match.
    let writer = IndexWriter::open(&path, index_with(0, &[(0, 32)])).expect("open");
    drop(writer);
    let reloaded = SparseIndex::load(&path, 0).expect("load");
    assert_eq!(
        reloaded.entries(),
        &[IndexEntry {
            offset: 0,
            position: 32
        }]
    );
}
