//! The file's own promises: it round-trips, it is absent as zero, and a copy
//! that does not decode is an error rather than a zero.
use super::*;

#[test]
fn state_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = ReplicaState {
        accepted_generation: 7,
        commit_offset: 1234,
        hold_at_commit: true,
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
            hold_at_commit: false,
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
fn a_file_with_no_flags_reads_as_not_held() {
    // What a build that predates the hold wrote: the flags were a reserved zero.
    let state = ReplicaState {
        accepted_generation: 2,
        commit_offset: 5,
        hold_at_commit: false,
    };
    let bytes = state.encode();
    assert_eq!(&bytes[6..8], &[0, 0]);
    assert_eq!(ReplicaState::decode(&bytes), Some(state));
}
