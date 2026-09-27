//! The file's own promises: it round-trips, it is absent as zero, and a copy
//! that does not decode is an error rather than a zero.
use super::*;

#[test]
fn state_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = ReplicaState {
        accepted_generation: 7,
        commit_offset: 1234,
    };
    store(dir.path(), &state).expect("store");
    assert_eq!(load(dir.path()).expect("load"), state);
}

#[test]
fn an_absent_file_reads_as_nothing_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(load(dir.path()).expect("load"), ReplicaState::default());
}

#[test]
fn a_damaged_file_fails_rather_than_reading_as_zero() {
    // Zero would accept any leader, which is what this file exists to stop.
    let dir = tempfile::tempdir().expect("tempdir");
    store(
        dir.path(),
        &ReplicaState {
            accepted_generation: 3,
            commit_offset: 10,
        },
    )
    .expect("store");
    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[9] ^= 0xFF;
    std::fs::write(&path, bytes).expect("write");
    assert!(load(dir.path()).is_err());
}

#[test]
fn copy_into_carries_the_state_to_a_replacement_directory() {
    let from = tempfile::tempdir().expect("tempdir");
    let to = tempfile::tempdir().expect("tempdir");
    let state = ReplicaState {
        accepted_generation: 4,
        commit_offset: 99,
    };
    store(from.path(), &state).expect("store");
    copy_into(from.path(), to.path()).expect("copy");
    assert_eq!(load(to.path()).expect("load"), state);
}
