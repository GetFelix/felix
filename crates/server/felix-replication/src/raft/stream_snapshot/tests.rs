use felix_broker::{CreatedBy, CreationToken, GroupEntry, GroupOp};

use super::*;

/// Cut batches as `producers.forgotten` lays them out: producer 7 refused
/// up to sequence 3.
fn cuts() -> ForgottenCuts {
    let mut body = Vec::new();
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(&7u64.to_be_bytes());
    body.extend_from_slice(&3u64.to_be_bytes());
    let mut bytes = 0x464C_5046u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    bytes.extend_from_slice(&body);
    ForgottenCuts::decode(&bytes).expect("cut batches")
}

fn groups() -> GroupTable {
    let mut table = GroupTable::default();
    let cre = CreationToken {
        term: 2,
        index: 0,
        by: CreatedBy::Hydrate,
    };
    let entry = GroupEntry {
        group: "billing".to_owned(),
        op: GroupOp::Advance { cre, to: 4 },
    };
    table.apply(2, 5, &entry);
    table
}

/// A purge snapshot's floor: the carried trim at offset 9 naming floor 6.
fn floors() -> CarriedFloor {
    CarriedFloor {
        offset: 9,
        floor: 6,
        headers: Bytes::from_static(b"trimmed"),
    }
}

/// A member that predates purges reads a purge snapshot as cut batches and
/// refuses it, rather than loading it without its floors.
#[test]
fn an_older_reader_refuses_a_purge_snapshot() {
    let bytes = StreamSnapshot {
        purge: Some(floors()),
        ..StreamSnapshot::default()
    }
    .encode();
    assert!(ForgottenCuts::decode(&bytes).is_none());
}

/// Every combination of carried state loads back as it was built.
#[test]
fn a_stream_snapshot_round_trips() {
    for cuts in [ForgottenCuts::default(), cuts()] {
        for groups in [GroupTable::default(), groups()] {
            for purge in [None, Some(floors())] {
                let snapshot = StreamSnapshot {
                    cuts: cuts.clone(),
                    groups: groups.clone(),
                    purge,
                };
                let decoded = StreamSnapshot::decode(&snapshot.encode()).expect("decoded");
                assert_eq!(decoded, snapshot);
            }
        }
    }
}

/// Without group writes the data is the older layout, byte for byte, so a
/// member that predates group tables reads it.
#[test]
fn a_snapshot_without_groups_keeps_the_older_layout() {
    let only_cuts = StreamSnapshot {
        cuts: cuts(),
        groups: GroupTable::default(),
        purge: None,
    };
    assert_eq!(only_cuts.encode().as_ref(), cuts().encode().as_slice());
    assert!(StreamSnapshot::default().encode().is_empty());
}

/// A damaged byte anywhere, or data cut short, is refused rather than
/// loaded as a smaller table.
#[test]
fn a_damaged_stream_snapshot_is_refused() {
    for purge in [None, Some(floors())] {
        refused_when_damaged(purge);
    }
}

fn refused_when_damaged(purge: Option<CarriedFloor>) {
    let bytes = StreamSnapshot {
        cuts: cuts(),
        groups: groups(),
        purge,
    }
    .encode();
    for at in 0..bytes.len() {
        let mut damaged = bytes.to_vec();
        damaged[at] ^= 1;
        assert!(StreamSnapshot::decode(&damaged).is_err(), "at {at}");
    }
    for len in 1..bytes.len() {
        assert!(StreamSnapshot::decode(&bytes[..len]).is_err(), "at {len}");
    }
}

/// The envelope never carries an empty table: that state is the older
/// layout, so the envelope with one is refused.
#[test]
fn an_envelope_with_an_empty_table_is_refused() {
    let table = GroupTable::default().encode();
    let mut body = 0u32.to_be_bytes().to_vec();
    body.extend_from_slice(&table);
    let mut bytes = MAGIC.to_be_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    bytes.extend_from_slice(&body);
    assert!(StreamSnapshot::decode(&bytes).is_err());
}
