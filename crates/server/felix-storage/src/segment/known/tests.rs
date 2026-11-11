use tempfile::tempdir;

use super::*;
use crate::segment::GENERATION_VERSION;
use crate::segment::writer::SegmentWriter;

/// A segment of records stamped `stamps`, and its length.
fn segment(dir: &Path, stamps: &[u64]) -> u64 {
    let mut writer =
        SegmentWriter::create_at_version(dir, 0, 0, 1, 0, 64, GENERATION_VERSION).expect("create");
    for stamp in stamps {
        let record = crate::log::AppendRecord {
            payload: bytes::Bytes::from_static(b"x"),
            timestamp_micros: 1,
            mark: Default::default(),
            publisher: None,
            headers: None,
        };
        writer.append_at(&[record], Some(*stamp)).expect("append");
    }
    writer.sync().expect("sync");
    writer.size_bytes()
}

#[test]
fn a_written_file_loads_only_while_it_matches_its_segment() {
    let dir = tempdir().expect("dir");
    let len = segment(dir.path(), &[0, 7, 9, 0]);
    let path = dir.path().join(known_file_name(0));
    let load =
        |len| KnownGenerations::load_verified(dir.path(), 0, "t", GENERATION_VERSION, 0, len);
    assert_eq!(load(len), None, "missing");

    let mut known = KnownGenerations::default();
    let mut position = crate::segment::SEGMENT_HEADER_LEN;
    let record_len = (len - position) / 4;
    for stamp in [0, 7, 9, 0] {
        known.observe(position, Some(stamp));
        position += record_len;
    }
    assert_eq!(known.first_generation(), Some(7));
    assert_eq!(known.last_generation(), Some(9));
    known.persist(&path, 0, len).expect("persist");
    assert_eq!(load(len), Some(known));
    assert_eq!(load(len + 1), None, "a different length");

    // Pointing at a record that carries another generation.
    let wrong = KnownGenerations {
        last: known.first.map(|(at, _)| (at, 9)),
        ..known
    };
    wrong.persist(&path, 0, len).expect("persist");
    assert_eq!(load(len), None);

    known.persist(&path, 0, len).expect("persist");
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[20] ^= 1;
    std::fs::write(&path, bytes).expect("write");
    assert_eq!(load(len), None, "damaged");
}
