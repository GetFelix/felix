//! The file's own promises: it round-trips, it is absent as no ballot, and a
//! copy that does not decode is an error rather than no ballot.
use super::*;

fn ballot(generation: u64, leader: &str) -> Ballot {
    Ballot {
        generation,
        leader: leader.to_string(),
    }
}

#[test]
fn a_ballot_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &ballot(7, "broker-b")).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(ballot(7, "broker-b")));
}

#[test]
fn an_absent_file_reads_as_no_ballot() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(load(dir.path()).expect("load"), None);
}

#[test]
fn a_damaged_ballot_fails_rather_than_reading_as_none() {
    // None would let any leader at the accepted generation be adopted.
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &ballot(3, "broker-a")).expect("store");
    let path = dir.path().join(FILE_NAME);
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[HEADER_LEN] ^= 0xFF;
    std::fs::write(&path, bytes).expect("write");
    assert!(load(dir.path()).is_err());
}

#[test]
fn a_length_past_the_bytes_does_not_decode() {
    let mut bytes = ballot(3, "broker-a").encode();
    bytes[6..8].copy_from_slice(&(MAX_LEADER_LEN as u16).to_be_bytes());
    assert_eq!(Ballot::decode(&bytes), None);
}
