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

/// Reading the id creates none, so a start that only asks what the directory
/// holds leaves it as it was.
#[test]
fn loading_reads_the_id_and_creates_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(load(dir.path()).expect("load"), None);
    assert!(!dir.path().join(FILE_NAME).exists());
    let (id, _) = load_or_create(dir.path(), counter(0)).expect("create");
    assert_eq!(load(dir.path()).expect("load"), Some(id));
    std::fs::write(dir.path().join(FILE_NAME), b"not an id").expect("damage");
    assert_eq!(load(dir.path()).expect("load"), None);
}

/// `load` must read through the filesystem seam, or a simulated data
/// directory always looks like it has no id.
#[cfg(feature = "sim")]
#[test]
fn load_reads_a_directory_the_simulator_holds_in_memory() {
    let root = std::env::temp_dir().join(format!("felix-storage-id-load-{}", std::process::id()));
    crate::sim_fs::register_root(&root);
    let (id, _) = load_or_create(&root, || [7; 16]).expect("create");
    let loaded = load(&root).expect("load");
    crate::sim_fs::unregister_root(&root);
    assert_eq!(loaded, Some(id));
}

#[cfg(feature = "sim")]
#[test]
fn the_replaced_marker_bytes_are_read_from_a_simulated_directory() {
    let root = std::env::temp_dir().join(format!("felix-storage-replaced-{}", std::process::id()));
    crate::sim_fs::register_root(&root);
    assert_eq!(
        crate::run_state::read_replaced_bytes(&root).expect("read"),
        None
    );
    crate::fs::write(root.join(crate::run_state::REPLACED_FILE_NAME), b"x").expect("write");
    let bytes = crate::run_state::read_replaced_bytes(&root).expect("read");
    crate::sim_fs::unregister_root(&root);
    assert_eq!(bytes, Some(b"x".to_vec()));
}
