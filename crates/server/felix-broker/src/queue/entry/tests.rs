use super::*;

fn token(by: CreatedBy) -> CreationToken {
    CreationToken {
        term: 3,
        index: 41,
        by,
    }
}

fn every_op() -> Vec<GroupOp> {
    let mut ops = Vec::new();
    for by in [CreatedBy::Hydrate, CreatedBy::Seek] {
        let cre = token(by);
        ops.extend([
            GroupOp::Advance { cre, to: 10 },
            GroupOp::DeadLetter { cre, offset: 7 },
            GroupOp::Redrive { cre, offset: 7 },
            GroupOp::FinishRedrive { cre, offset: 7 },
            GroupOp::Discard { cre, offset: 7 },
        ]);
    }
    ops.extend([
        GroupOp::Seek {
            to: 5,
            if_new: false,
        },
        GroupOp::Seek {
            to: 0,
            if_new: true,
        },
        GroupOp::Delete,
    ]);
    ops
}

#[test]
fn every_op_round_trips() {
    for op in every_op() {
        for group in ["g", "billing", "ünïcode"] {
            let entry = GroupEntry {
                group: group.to_owned(),
                op,
            };
            let bytes = entry.encode();
            assert_eq!(GroupEntry::decode(&bytes).expect("decode"), entry);
        }
    }
}

/// The layout is pinned: a member of another build folds these bytes.
#[test]
fn the_layout_is_the_documented_one() {
    let entry = GroupEntry {
        group: "g".to_owned(),
        op: GroupOp::DeadLetter {
            cre: token(CreatedBy::Seek),
            offset: 7,
        },
    };
    let mut want = vec![1, 1, 0, 0, 0, 1, b'g'];
    want.extend(3u64.to_be_bytes());
    want.extend(41u64.to_be_bytes());
    want.push(1);
    want.extend(7u64.to_be_bytes());
    assert_eq!(entry.encode()[..], want[..]);

    let seek = GroupEntry {
        group: "g".to_owned(),
        op: GroupOp::Seek {
            to: 9,
            if_new: true,
        },
    };
    let mut want = vec![1, 5, 0, 0, 0, 1, b'g'];
    want.extend(9u64.to_be_bytes());
    want.push(1);
    assert_eq!(seek.encode()[..], want[..]);

    let delete = GroupEntry {
        group: "g".to_owned(),
        op: GroupOp::Delete,
    };
    assert_eq!(delete.encode()[..], [1, 6, 0, 0, 0, 1, b'g']);
}

/// Every prefix of a valid entry, and every entry with bytes after it, is
/// refused: a member never folds a write it read only part of.
#[test]
fn a_short_or_long_payload_is_refused() {
    for op in every_op() {
        let bytes = GroupEntry {
            group: "g".to_owned(),
            op,
        }
        .encode();
        for len in 0..bytes.len() {
            assert!(
                GroupEntry::decode(&bytes[..len]).is_err(),
                "{op:?} at {len}"
            );
        }
        let mut long = bytes.to_vec();
        long.push(0);
        assert!(GroupEntry::decode(&long).is_err(), "{op:?}");
    }
}

/// An unknown version, operation, creation kind or seek flag is refused
/// rather than read as something this build knows.
#[test]
fn unknown_codes_are_refused() {
    let advance = GroupEntry {
        group: "g".to_owned(),
        op: GroupOp::Advance {
            cre: token(CreatedBy::Hydrate),
            to: 1,
        },
    }
    .encode();
    let seek = GroupEntry {
        group: "g".to_owned(),
        op: GroupOp::Seek {
            to: 1,
            if_new: false,
        },
    }
    .encode();
    let mut forged = Vec::new();
    let mut at = |bytes: &Bytes, index: usize, value: u8| {
        let mut bytes = bytes.to_vec();
        bytes[index] = value;
        forged.push(bytes);
    };
    at(&advance, 0, 2);
    at(&advance, 1, 7);
    at(&advance, 7 + 16, 2);
    at(&seek, seek.len() - 1, 2);
    at(&seek, seek.len() - 1, 0x81);
    for bytes in forged {
        let refused = GroupEntry::decode(&bytes).expect_err("refused");
        assert!(
            matches!(refused, BrokerError::MalformedGroupEntry(_)),
            "{refused}"
        );
    }
    let mut not_utf8 = advance.to_vec();
    not_utf8[6] = 0xFF;
    assert!(GroupEntry::decode(&not_utf8).is_err());
}

#[test]
fn only_tracker_writes_carry_a_token() {
    for op in every_op() {
        let carries = !matches!(op, GroupOp::Seek { .. } | GroupOp::Delete);
        assert_eq!(op.creation().is_some(), carries, "{op:?}");
    }
}

/// An entry naming no group is refused: no group op is valid without one,
/// so such a record can only be damage.
#[test]
fn an_empty_group_name_is_refused() {
    for op in every_op() {
        let bytes = GroupEntry {
            group: String::new(),
            op,
        }
        .encode();
        let refused = GroupEntry::decode(&bytes).expect_err("refused");
        assert!(
            matches!(refused, BrokerError::MalformedGroupEntry(_)),
            "{op:?}: {refused}"
        );
    }
}
