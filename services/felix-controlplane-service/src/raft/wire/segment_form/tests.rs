use super::*;
use crate::raft::wire::Membership;

fn head() -> SnapshotHead {
    SnapshotHead {
        vote: Vote {
            term: 3,
            voted_for: 2,
            committed: true,
        },
        meta: SnapshotMeta {
            last_log_id: Some(LogId { term: 3, index: 41 }),
            last_membership: StoredMembership {
                log_id: Some(LogId { term: 1, index: 0 }),
                membership: Membership {
                    configs: vec![[1, 2, 3].into()],
                    nodes: Default::default(),
                },
            },
        },
    }
}

#[test]
fn a_snapshot_body_carries_its_head_and_its_data_as_they_are() {
    let data = [0u8, 255, b'{', b'"', 7];
    let body = snapshot_body(&head(), &data);
    let (parsed, rest) = parse_snapshot_body(&body).expect("parse");
    assert_eq!(parsed, head());
    assert_eq!(rest, data);
    // The data is not re-encoded: the body ends with it.
    assert!(body.ends_with(&data));

    let body = snapshot_body(&head(), &[]);
    let (_, empty) = parse_snapshot_body(&body).expect("parse");
    assert!(empty.is_empty());
}

#[test]
fn a_body_that_is_not_a_snapshot_request_is_refused() {
    let body = snapshot_body(&head(), b"data");
    assert!(parse_snapshot_body(&[]).is_none());
    assert!(parse_snapshot_body(&body[..3]).is_none());
    // Cut inside the head.
    assert!(parse_snapshot_body(&body[..10]).is_none());
    // A length that points past the end.
    let mut long = body.clone();
    long[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(parse_snapshot_body(&long).is_none());
    // The chunk request a member on redb sends.
    assert!(
        parse_snapshot_body(br#"{"vote":{},"meta":{},"offset":0,"data":[],"done":true}"#).is_none()
    );
}

/// A log id is its term and its index, with no node id to lose.
#[test]
fn ids_keep_their_term_and_index() {
    let id = LogId { term: 9, index: 4 };
    assert_eq!(LogId::from(&types::LogId::from(id)), id);
    let vote = Vote {
        term: 9,
        voted_for: 3,
        committed: false,
    };
    assert_eq!(Vote::from(&types::Vote::from(vote)), vote);
    assert_eq!(
        serde_json::to_value(id).expect("json"),
        serde_json::json!({"term": 9, "index": 4})
    );
}
