use super::*;

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
    // The chunk request an older release sends.
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

/// The bytes members exchange and the membership and snapshot meta the store
/// keeps. A member of the same release line reads them from a peer, and its
/// own store across a restart, so a renamed field or variant must fail here
/// rather than in a mixed group.
#[test]
fn the_peer_and_stored_forms_are_pinned() {
    let vote = Vote {
        term: 5,
        voted_for: 2,
        committed: true,
    };
    let id = |term, index| Some(LogId { term, index });
    let membership = Membership {
        configs: vec![[1, 2].into()],
        nodes: [(
            1,
            Node {
                addr: "10.0.0.1:7000".into(),
            },
        )]
        .into(),
    };
    let entries = vec![
        Entry {
            log_id: LogId { term: 5, index: 7 },
            payload: Payload::Blank,
        },
        Entry {
            log_id: LogId { term: 5, index: 8 },
            payload: Payload::Normal(vec![1, 2]),
        },
        Entry {
            log_id: LogId { term: 5, index: 9 },
            payload: Payload::Membership(membership.clone()),
        },
    ];
    let cases: Vec<(serde_json::Value, &str)> = vec![
        (
            serde_json::to_value(VoteRequest {
                vote,
                last_log_id: id(4, 6),
            })
            .expect("json"),
            r#"{"last_log_id":{"index":6,"term":4},"vote":{"committed":true,"term":5,"voted_for":2}}"#,
        ),
        (
            serde_json::to_value(VoteResponse {
                vote,
                vote_granted: false,
                last_log_id: None,
            })
            .expect("json"),
            r#"{"last_log_id":null,"vote":{"committed":true,"term":5,"voted_for":2},"vote_granted":false}"#,
        ),
        (
            serde_json::to_value(AppendEntriesRequest {
                vote,
                prev_log_id: id(4, 6),
                entries,
                leader_commit: id(4, 6),
            })
            .expect("json"),
            r#"{"entries":[{"log_id":{"index":7,"term":5},"payload":"Blank"},{"log_id":{"index":8,"term":5},"payload":{"Normal":[1,2]}},{"log_id":{"index":9,"term":5},"payload":{"Membership":{"configs":[[1,2]],"nodes":{"1":{"addr":"10.0.0.1:7000"}}}}}],"leader_commit":{"index":6,"term":4},"prev_log_id":{"index":6,"term":4},"vote":{"committed":true,"term":5,"voted_for":2}}"#,
        ),
        (
            serde_json::to_value(Reply::<AppendEntriesResponse>::Ok(
                AppendEntriesResponse::PartialSuccess(id(5, 8)),
            ))
            .expect("json"),
            r#"{"Ok":{"PartialSuccess":{"index":8,"term":5}}}"#,
        ),
        (
            serde_json::to_value(Reply::<AppendEntriesResponse>::Ok(
                AppendEntriesResponse::HigherVote(vote),
            ))
            .expect("json"),
            r#"{"Ok":{"HigherVote":{"committed":true,"term":5,"voted_for":2}}}"#,
        ),
        (
            serde_json::to_value(Reply::<InstallSnapshotResponse>::Err(serde_json::json!(
                "stopped"
            )))
            .expect("json"),
            r#"{"Err":"stopped"}"#,
        ),
        (
            serde_json::to_value(SnapshotMeta {
                last_log_id: id(5, 9),
                last_membership: StoredMembership {
                    log_id: id(5, 9),
                    membership,
                },
            })
            .expect("json"),
            r#"{"last_log_id":{"index":9,"term":5},"last_membership":{"log_id":{"index":9,"term":5},"membership":{"configs":[[1,2]],"nodes":{"1":{"addr":"10.0.0.1:7000"}}}}}"#,
        ),
    ];
    for (value, pinned) in cases {
        let pinned: serde_json::Value = serde_json::from_str(pinned).expect("pinned json");
        assert_eq!(value, pinned);
    }
}
