use super::*;

#[test]
fn the_epoch_rises_at_each_call_and_survives_reopening() {
    let root = tempfile::tempdir().expect("tempdir");
    assert_eq!(next_epoch(root.path()).expect("first"), 1);
    assert_eq!(next_epoch(root.path()).expect("second"), 2);
    assert_eq!(
        std::fs::read(root.path().join(EPOCH_FILE)).expect("read"),
        2u64.to_le_bytes()
    );
}

#[test]
fn a_damaged_epoch_file_fails_the_start() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::write(root.path().join(EPOCH_FILE), b"abc").expect("write");
    assert!(next_epoch(root.path()).is_err());
}

#[test]
fn a_beat_rewrites_the_same_eight_bytes() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut probe = Probe::open(root.path()).expect("open");
    probe.beat().expect("first");
    probe.beat().expect("second");
    assert_eq!(
        std::fs::read(root.path().join(PROBE_FILE)).expect("read"),
        2u64.to_le_bytes()
    );
}
