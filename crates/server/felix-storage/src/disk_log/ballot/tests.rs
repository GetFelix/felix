//! The file's own promises: it round-trips in both formats, it is absent as
//! no ballot, a copy that does not decode is an error rather than no ballot,
//! and a format this build does not read is refused, never misread.
use super::*;
use crate::io::writeback::SplitMix64;

fn ballot(generation: u64, leader: &str) -> Ballot {
    Ballot {
        generation,
        leader: leader.to_string(),
        committed: false,
    }
}

fn committed(generation: u64, leader: &str) -> Ballot {
    Ballot {
        committed: true,
        ..ballot(generation, leader)
    }
}

fn version_of(bytes: &[u8]) -> u16 {
    u16::from_be_bytes([bytes[4], bytes[5]])
}

/// `bytes` with its closing crc made to fit again.
fn with_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    let end = bytes.len() - 4;
    let crc = crate::segment::format::crc32(&[&bytes[..end]]);
    bytes[end..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

#[test]
fn a_ballot_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &ballot(7, "broker-b")).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(ballot(7, "broker-b")));
}

#[test]
fn a_committed_ballot_round_trips_as_v2() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &committed(7, "broker-b")).expect("store");
    let bytes = std::fs::read(dir.path().join(FILE_NAME)).expect("read");
    assert_eq!(version_of(&bytes), 2);
    assert_eq!(
        load(dir.path()).expect("load"),
        Some(committed(7, "broker-b"))
    );
}

#[test]
fn a_ballot_without_the_mark_is_written_as_v1() {
    // So the previous release still opens every shard this build writes
    // until a committed vote is saved.
    for ballot in [ballot(7, "broker-b"), ballot(3, "")] {
        let bytes = ballot.encode();
        assert_eq!(version_of(&bytes), UNCOMMITTED_VERSION);
        assert_eq!(decode_up_to(&bytes, 1), Ok(ballot));
    }
}

#[test]
fn a_v1_reader_refuses_a_committed_ballot() {
    // The previous release's check: it reads v1 and nothing newer.
    let bytes = committed(7, "broker-b").encode();
    assert_eq!(decode_up_to(&bytes, 1), Err(Undecodable::Newer(2)));
}

#[test]
fn a_newer_format_fails_the_open_and_says_so() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut bytes = committed(7, "broker-b").encode();
    bytes[4..6].copy_from_slice(&3u16.to_be_bytes());
    std::fs::write(dir.path().join(FILE_NAME), with_crc(bytes)).expect("write");
    let err = load(dir.path()).expect_err("a v3 ballot must not be read");
    assert!(err.to_string().contains("newer build"), "{err}");
}

#[test]
fn a_damaged_version_is_damage_not_a_newer_format() {
    let mut bytes = committed(7, "broker-b").encode();
    bytes[4..6].copy_from_slice(&3u16.to_be_bytes());
    assert_eq!(Ballot::decode(&bytes), Err(Undecodable::Damaged));
}

#[test]
fn an_unknown_flag_does_not_decode() {
    let mut bytes = committed(7, "broker-b").encode();
    bytes[HEADER_LEN] = FLAG_COMMITTED | 2;
    assert_eq!(Ballot::decode(&with_crc(bytes)), Err(Undecodable::Damaged));
}

#[test]
fn a_committed_vote_for_no_leader_does_not_decode() {
    let mut bytes = committed(7, "x").encode();
    bytes[6..8].copy_from_slice(&0u16.to_be_bytes());
    bytes.remove(COMMITTED_HEADER_LEN);
    assert_eq!(Ballot::decode(&with_crc(bytes)), Err(Undecodable::Damaged));
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
    assert_eq!(Ballot::decode(&bytes), Err(Undecodable::Damaged));
}

/// Seeded fuzzing of the decoder: every ballot round-trips, no flipped bit
/// or cut decodes, and bytes with a fitting crc decode only to something
/// that round-trips.
#[test]
fn the_decoder_holds_up_to_damage() {
    let mut rng = SplitMix64::new(0xB411_0700);
    for _ in 0..2_000 {
        let len = rng.below(24) as usize;
        let leader: String = (0..len)
            .map(|_| (b'a' + rng.below(26) as u8) as char)
            .collect();
        let original = Ballot {
            generation: rng.next_u64(),
            committed: len > 0 && rng.coin(),
            leader,
        };
        let bytes = original.encode();
        assert_eq!(Ballot::decode(&bytes), Ok(original.clone()));

        let bit = rng.below(bytes.len() as u64 * 8) as usize;
        let mut flipped = bytes.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        assert_eq!(
            Ballot::decode(&flipped),
            Err(Undecodable::Damaged),
            "bit {bit}"
        );

        let cut = rng.below(bytes.len() as u64) as usize;
        assert!(Ballot::decode(&bytes[..cut]).is_err(), "cut at {cut}");

        let mut noise = bytes.clone();
        for _ in 0..1 + rng.below(4) {
            let at = rng.below(noise.len() as u64 - 4) as usize;
            noise[at] = rng.next_u64() as u8;
        }
        if let Ok(decoded) = Ballot::decode(&with_crc(noise)) {
            assert!(!decoded.committed || !decoded.leader.is_empty());
            assert_eq!(Ballot::decode(&decoded.encode()), Ok(decoded));
        }
    }
}
