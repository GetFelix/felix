use tempfile::tempdir;

use super::*;

fn entry(segment_id: SegmentId, base_offset: Offset, last_offset: Offset) -> ManifestEntry {
    ManifestEntry {
        segment_id,
        base_offset,
        last_offset,
        size_bytes: 100 + segment_id,
        checksum: 0xdead_beef ^ segment_id as u32,
        oldest_timestamp_micros: 10 * base_offset,
        newest_timestamp_micros: 10 * last_offset,
        key: format!("shard/{base_offset:020}-{segment_id:020}.segment"),
    }
}

fn manifest(entries: &[ManifestEntry]) -> Manifest {
    let mut manifest = Manifest::default();
    for entry in entries {
        manifest.insert(entry.clone()).expect("insert");
    }
    manifest
}

#[test]
fn round_trips_through_its_encoding() {
    let original = manifest(&[entry(2, 10, 19), entry(0, 0, 4), entry(1, 5, 9)]);
    let decoded = Manifest::decode(&original.encode()).expect("decode");
    assert_eq!(decoded, original);
    let bases: Vec<_> = decoded.entries().iter().map(|e| e.base_offset).collect();
    assert_eq!(bases, [0, 5, 10], "kept in offset order");
}

#[test]
fn an_empty_manifest_round_trips() {
    let decoded = Manifest::decode(&Manifest::default().encode()).expect("decode");
    assert!(decoded.entries().is_empty());
}

#[test]
fn any_flipped_byte_fails_to_decode() {
    let bytes = manifest(&[entry(0, 0, 4), entry(1, 5, 9)]).encode();
    for at in 0..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[at] ^= 0x01;
        assert!(Manifest::decode(&damaged).is_none(), "byte {at}");
    }
    assert!(Manifest::decode(&bytes[..bytes.len() - 1]).is_none());
}

#[test]
fn refuses_an_entry_overlapping_another() {
    let mut manifest = manifest(&[entry(0, 0, 4)]);
    assert!(manifest.insert(entry(1, 4, 9)).is_err());
    assert!(manifest.insert(entry(1, 5, 9)).is_ok());
}

#[test]
fn covers_only_ranges_with_no_gap() {
    let manifest = manifest(&[entry(0, 0, 4), entry(1, 5, 9), entry(3, 20, 29)]);
    assert!(manifest.covers(0, 10));
    assert!(manifest.covers(3, 7));
    assert!(manifest.covers(10, 10), "an empty range is covered");
    assert!(!manifest.covers(0, 11));
    assert!(!manifest.covers(8, 21), "10..20 was never copied");
    assert!(manifest.covers(20, 30));
}

#[test]
fn forget_from_drops_every_entry_holding_the_offset_or_later() {
    let mut manifest = manifest(&[entry(0, 0, 4), entry(1, 5, 9), entry(2, 10, 14)]);
    assert!(manifest.forget_from(7));
    let lasts: Vec<_> = manifest.entries().iter().map(|e| e.last_offset).collect();
    assert_eq!(lasts, [4], "the entry cut in the middle goes too");
    assert!(!manifest.forget_from(5));
}

#[test]
fn an_absent_manifest_loads_empty_and_a_damaged_one_fails() {
    let dir = tempdir().expect("dir");
    assert!(load(dir.path()).expect("absent").entries().is_empty());

    let written = manifest(&[entry(0, 0, 4)]);
    store(dir.path(), &written).expect("store");
    assert_eq!(load(dir.path()).expect("load"), written);

    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[HEADER_LEN] ^= 0xff;
    std::fs::write(&path, bytes).expect("damage");
    assert!(load(dir.path()).is_err());
}
