//! The id survives a restart and nothing else: a missing or damaged file
//! gets a new one.
use super::*;

fn counter(start: u8) -> impl FnMut() -> [u8; 16] {
    let mut next = start;
    move || {
        next += 1;
        [next; 16]
    }
}

#[test]
fn the_id_persists_across_reopens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let (first, origin) = load_or_create(dir.path(), &mut fresh).expect("create");
    assert_eq!(origin, Origin::Created);
    let (again, origin) = load_or_create(dir.path(), &mut fresh).expect("load");
    assert_eq!(origin, Origin::Loaded);
    assert_eq!(again, first);
}

#[test]
fn deleting_the_file_gives_a_new_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let (first, _) = load_or_create(dir.path(), &mut fresh).expect("create");
    std::fs::remove_file(dir.path().join(FILE_NAME)).expect("remove");
    let (second, origin) = load_or_create(dir.path(), &mut fresh).expect("create");
    assert_eq!(origin, Origin::Created);
    assert_ne!(second, first);
}

#[test]
fn a_corrupt_file_is_replaced_with_a_new_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let (first, _) = load_or_create(dir.path(), &mut fresh).expect("create");
    let path = dir.path().join(FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    let mut flipped = good.clone();
    flipped[10] ^= 0xff;
    let mut bad_magic = good.clone();
    bad_magic[0] = b'X';
    for damaged in [flipped, bad_magic, good[..20].to_vec(), Vec::new()] {
        std::fs::write(&path, &damaged).expect("damage");
        let (replaced, origin) = load_or_create(dir.path(), &mut fresh).expect("replace");
        assert_eq!(origin, Origin::ReplacedCorrupt);
        assert_ne!(replaced, first);
        assert_eq!(
            load_or_create(dir.path(), &mut fresh).expect("load"),
            (replaced, Origin::Loaded)
        );
    }
}

#[test]
fn a_directory_with_shards_but_no_file_gets_a_new_id() {
    // An upgrade from a build without storage ids looks like this; there is
    // nothing to say the shards were not copied in.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("t_ns_orders_0-0123456789abcdef")).expect("mkdir");
    std::fs::create_dir(dir.path().join("caches")).expect("mkdir");
    let (id, origin) = load_or_create(dir.path(), || [7; 16]).expect("create");
    assert_eq!(origin, Origin::Created);
    assert_eq!(id, StorageId::from_bytes([7; 16]));
}

#[test]
fn replace_writes_a_new_id_over_the_old_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (first, _) = load_or_create(dir.path(), || [1; 16]).expect("create");
    let rotated = replace(dir.path(), [2; 16]).expect("replace");
    assert_ne!(rotated, first);
    assert_eq!(
        load_or_create(dir.path(), || [3; 16]).expect("load"),
        (rotated, Origin::Loaded)
    );
}

#[test]
fn the_id_renders_as_32_lowercase_hex_digits() {
    let mut bytes = [0u8; 16];
    bytes[0] = 0xab;
    bytes[15] = 0x01;
    assert_eq!(
        StorageId::from_bytes(bytes).to_hex(),
        "ab000000000000000000000000000001"
    );
}

#[test]
fn an_encoded_id_round_trips_and_trailing_bytes_are_refused() {
    let id = StorageId::from_bytes([9; 16]);
    let encoded = id.encode();
    assert_eq!(StorageId::decode(&encoded), Some(id));
    let mut longer = encoded.to_vec();
    longer.push(0);
    assert_eq!(StorageId::decode(&longer), None);
}
