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
fn a_snapshot_head_round_trips() {
    let bytes = head().encode();
    assert_eq!(SnapshotHead::decode(&bytes), Some(head()));
    assert!(SnapshotHead::decode(&bytes[..bytes.len() - 1]).is_none());
    // The chunk request a member on redb sends.
    assert!(
        SnapshotHead::decode(br#"{"vote":{},"meta":{},"offset":0,"data":[],"done":true}"#)
            .is_none()
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
