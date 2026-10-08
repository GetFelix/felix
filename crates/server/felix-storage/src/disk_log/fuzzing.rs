//! The small per-shard state files, for the `sidecar_state` fuzz target.
//!
//! Each file ends its header with a CRC, and mutation almost never lands a
//! matching one, so every decoder also sees the input with its checksum
//! rewritten to fit. That is what gets the bytes past the CRC and into the
//! parsing behind it.

use super::ballot::Ballot;
use super::durable_mark::{DurableMark, MARK_LEN};
use super::epochs::{EPOCH_ENTRY_LEN, EPOCH_HEADER_LEN, EpochMap};
use super::producers::{self, SNAPSHOT_HEADER_LEN};
use super::replica_state::{ENCODED_LEN, ReplicaState};

/// Decode `data` as each state file, raw and with its CRC fixed up. Anything
/// that decodes must re-encode to something that decodes back to itself.
pub(crate) fn sidecars(data: &[u8]) {
    let mark_crc = MARK_LEN - 8;
    for bytes in [data.to_vec(), with_crc(data, mark_crc, 0..mark_crc)] {
        if let Some(mark) = DurableMark::decode(&bytes) {
            assert_eq!(DurableMark::decode(&mark.encode()), Some(mark));
        }
    }

    let replica_crc = ENCODED_LEN - 4;
    for bytes in [data.to_vec(), with_crc(data, replica_crc, 0..replica_crc)] {
        if let Some(state) = ReplicaState::decode(&bytes) {
            assert_eq!(ReplicaState::decode(&state.encode()), Some(state));
        }
    }

    // The ballot's CRC closes the file, wherever its length puts the end.
    let ballot_crc = data.len().saturating_sub(4);
    for bytes in [data.to_vec(), with_crc(data, ballot_crc, 0..ballot_crc)] {
        if let Some(ballot) = Ballot::decode(&bytes) {
            assert_eq!(Ballot::decode(&ballot.encode()), Some(ballot));
        }
    }

    let count = data
        .get(6..8)
        .map_or(0, |raw| u16::from_be_bytes([raw[0], raw[1]]) as usize);
    let body = EPOCH_HEADER_LEN..EPOCH_HEADER_LEN + count * EPOCH_ENTRY_LEN;
    for bytes in [data.to_vec(), with_crc(data, EPOCH_HEADER_LEN - 4, body)] {
        if let Some(map) = EpochMap::decode(&bytes) {
            assert!(map.entries().len() * EPOCH_ENTRY_LEN <= bytes.len());
            assert_eq!(EpochMap::decode(&map.encode()), Some(map));
        }
    }

    let body = SNAPSHOT_HEADER_LEN..data.len().max(SNAPSHOT_HEADER_LEN);
    for bytes in [data.to_vec(), with_crc(data, SNAPSHOT_HEADER_LEN - 4, body)] {
        if let Some((state, as_of)) = producers::decode(&bytes) {
            let again = producers::decode(&producers::encode(&state, as_of));
            assert_eq!(again, Some((state, as_of)));
        }
    }
}

/// `data` with the CRC at `at` rewritten to cover `region`, or unchanged when
/// it is too short to hold either.
fn with_crc(data: &[u8], at: usize, region: std::ops::Range<usize>) -> Vec<u8> {
    let mut bytes = data.to_vec();
    if bytes.len() >= region.end.max(at + 4) {
        let crc = crc32fast::hash(&bytes[region]);
        bytes[at..at + 4].copy_from_slice(&crc.to_be_bytes());
    }
    bytes
}
