use std::io::Write;

use tempfile::tempdir;

use super::*;

#[test]
fn read_at_does_not_move_the_cursor() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    std::fs::write(&path, b"0123456789").expect("write");
    let file = File::open(&path).expect("open");

    let mut buf = [0u8; 4];
    assert_eq!(read_at(&file, &mut buf, 2).expect("read"), 4);
    assert_eq!(&buf, b"2345");
    // A second read at the same offset returns the same bytes, which it
    // could not if the first had advanced a shared cursor.
    assert_eq!(read_at(&file, &mut buf, 2).expect("read"), 4);
    assert_eq!(&buf, b"2345");
}

#[test]
fn read_at_is_short_at_end_of_file() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    std::fs::write(&path, b"abc").expect("write");
    let file = File::open(&path).expect("open");

    let mut buf = [0u8; 8];
    assert_eq!(read_at(&file, &mut buf, 1).expect("read"), 2);
    assert_eq!(&buf[..2], b"bc");
    assert_eq!(read_at(&file, &mut buf, 99).expect("read"), 0);
}

#[test]
fn preallocate_leaves_the_logical_length_alone() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    let mut file = File::create(&path).expect("create");
    file.write_all(b"hi").expect("write");

    preallocate(&file, 0, 1024 * 1024).expect("preallocate");
    // Reserving blocks must not make the file look longer, or recovery would
    // read reserved space as a torn record tail.
    assert_eq!(file.metadata().expect("meta").len(), 2);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn extending_a_reservation_adds_blocks_but_not_length() {
    use std::os::unix::fs::MetadataExt;

    const MIB: u64 = 1024 * 1024;
    let dir = tempdir().expect("dir");
    let mut file = File::create(dir.path().join("f")).expect("create");
    file.write_all(b"hi").expect("write");
    let allocated = |file: &File| file.metadata().expect("meta").blocks() * 512;

    preallocate(&file, 0, MIB).expect("preallocate");
    let first = allocated(&file);
    preallocate(&file, MIB, 3 * MIB).expect("extend");
    let second = allocated(&file);

    assert_eq!(file.metadata().expect("meta").len(), 2);
    assert!(first >= MIB, "the first reservation took {first} bytes");
    assert!(
        second >= 4 * MIB,
        "the extension took {second} bytes in all"
    );
}

#[test]
fn preallocate_of_zero_is_a_no_op() {
    let dir = tempdir().expect("dir");
    let file = File::create(dir.path().join("f")).expect("create");
    preallocate(&file, 0, 0).expect("preallocate");
}

#[test]
fn sync_data_and_sync_dir_succeed() {
    let dir = tempdir().expect("dir");
    let path = dir.path().join("f");
    let mut file = File::create(&path).expect("create");
    file.write_all(b"data").expect("write");
    sync_data(&file).expect("sync_data");
    sync_dir(dir.path()).expect("sync_dir");
}

#[test]
fn create_dir_all_durable_creates_every_missing_component() {
    let dir = tempdir().expect("dir");
    let nested = dir.path().join("a").join("b").join("c");
    create_dir_all_durable(&nested).expect("create");
    assert!(nested.is_dir());
    // Opening an existing root goes through the same call.
    create_dir_all_durable(&nested).expect("existing");
}

#[test]
fn create_dir_all_durable_refuses_a_file_in_the_way() {
    let dir = tempdir().expect("dir");
    let file = dir.path().join("f");
    File::create(&file).expect("create");
    assert!(create_dir_all_durable(&file.join("sub")).is_err());
}

/// A new store root is lost to a power loss unless its parent is flushed.
#[cfg(target_os = "linux")]
#[test]
fn a_created_directory_survives_a_power_loss() {
    use super::power_loss::{PowerLoss, Writeback};

    let dir = tempdir().expect("dir");
    let observer = PowerLoss::install(dir.path()).expect("install");
    create_dir_all_durable(&dir.path().join("storage").join("caches")).expect("create");
    for seed in 0..32 {
        let image = tempdir().expect("image");
        observer
            .crash(seed, Writeback::AnySubset, image.path())
            .expect("crash");
        assert!(
            image.path().join("storage").join("caches").is_dir(),
            "seed {seed} lost the new directory"
        );
    }
}

/// A directory a crashed earlier run created without flushing is made durable
/// when it is opened again.
#[cfg(target_os = "linux")]
#[test]
fn an_unflushed_directory_is_made_durable_when_opened() {
    use super::power_loss::{PowerLoss, Writeback};

    let dir = tempdir().expect("dir");
    let observer = PowerLoss::install(dir.path()).expect("install");
    let root = dir.path().join("counters");
    std::fs::create_dir(&root).expect("unflushed create");
    create_dir_all_durable(&root).expect("open");
    for seed in 0..32 {
        let image = tempdir().expect("image");
        observer
            .crash(seed, Writeback::AnySubset, image.path())
            .expect("crash");
        assert!(
            image.path().join("counters").is_dir(),
            "seed {seed} lost the directory"
        );
    }
}
