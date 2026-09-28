//! The injector, which is only useful if it is also inert when unused.
use super::*;

fn injector(path: &std::path::Path) -> PartitionInjector {
    PartitionInjector::new(path.to_path_buf())
}

/// A missing file means nothing is severed. This is the state a test leaves
/// behind when it heals a partition by deleting the file, so it must not be an
/// error and must not be sticky.
#[test]
fn a_missing_file_blocks_nobody() {
    let dir = tempfile::tempdir().expect("tempdir");
    let injector = injector(&dir.path().join("absent"));
    assert!(!injector.blocks("broker-1"));
}

#[test]
fn a_listed_node_is_blocked_and_others_are_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("partition");
    std::fs::write(&path, "broker-1\nbroker-2\n").expect("write");

    let injector = injector(&path);
    assert!(injector.blocks("broker-1"));
    assert!(injector.blocks("broker-2"));
    assert!(!injector.blocks("broker-3"));
}

/// Blank lines and stray whitespace come from tests writing the file with a
/// shell, and must not produce a node id nobody has.
#[test]
fn blank_lines_and_whitespace_are_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("partition");
    std::fs::write(&path, "\n  broker-1  \n\n").expect("write");

    let injector = injector(&path);
    assert!(injector.blocks("broker-1"));
    assert!(!injector.blocks(""));
}

/// **A partition heals.** The reading is cached, so this also pins that the
/// cache expires rather than freezing the first answer forever.
#[test]
fn deleting_the_file_heals_the_partition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("partition");
    std::fs::write(&path, "broker-1\n").expect("write");

    let injector = injector(&path);
    assert!(injector.blocks("broker-1"));

    std::fs::remove_file(&path).expect("remove");
    std::thread::sleep(REREAD_AFTER + Duration::from_millis(50));
    assert!(
        !injector.blocks("broker-1"),
        "the partition did not heal; the cached reading never expired",
    );
}
