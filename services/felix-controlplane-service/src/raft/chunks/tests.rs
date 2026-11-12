use super::*;

fn meta(index: u64) -> wire::SnapshotMeta {
    let last = wire::LogId {
        leader_id: wire::LeaderId {
            term: 3,
            node_id: 2,
        },
        index,
    };
    wire::SnapshotMeta {
        last_log_id: Some(last),
        last_membership: wire::StoredMembership {
            log_id: None,
            membership: wire::Membership {
                configs: Vec::new(),
                nodes: Default::default(),
            },
        },
        snapshot_id: wire::snapshot_id(Some(&last)),
    }
}

fn vote() -> wire::Vote {
    wire::Vote {
        leader_id: wire::LeaderId {
            term: 3,
            node_id: 2,
        },
        committed: true,
    }
}

fn requests(index: u64, data: &[u8], chunk: usize) -> Vec<wire::InstallSnapshotRequest> {
    cut(vote(), &meta(index), data, chunk).collect()
}

#[test]
fn a_snapshot_is_cut_into_chunks_and_joined_again() {
    let data: Vec<u8> = (0..=250).collect();
    let chunks = requests(18, &data, 100);
    let shape: Vec<_> = chunks
        .iter()
        .map(|chunk| (chunk.offset, chunk.data.len(), chunk.done))
        .collect();
    assert_eq!(
        shape,
        vec![(0, 100, false), (100, 100, false), (200, 51, true)]
    );

    let mut current = None;
    let mut whole = None;
    for chunk in chunks {
        assert!(whole.is_none(), "only the last chunk completes");
        whole = receive(&mut current, chunk).expect("in order");
    }
    let (got_meta, got) = whole.expect("the last chunk completes the snapshot");
    assert_eq!(got, data);
    assert_eq!(got_meta, meta(18));
    assert!(current.is_none(), "nothing is held after the last chunk");
}

#[test]
fn an_empty_snapshot_is_one_chunk() {
    let chunks = requests(18, &[], 100);
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].done);
    let (_, data) = receive(&mut None, chunks[0].clone())
        .expect("accepted")
        .expect("complete");
    assert!(data.is_empty());
}

/// A sender that failed part-way starts again at offset 0 under the same id.
#[test]
fn a_transfer_started_again_replaces_the_bytes_held() {
    let data = vec![7u8; 250];
    let chunks = requests(18, &data, 100);
    let mut current = None;
    for chunk in &chunks[..2] {
        assert!(
            receive(&mut current, chunk.clone())
                .expect("start")
                .is_none()
        );
    }
    let mut whole = None;
    for chunk in chunks {
        whole = receive(&mut current, chunk).expect("restart");
    }
    assert_eq!(whole.expect("complete").1, data);
}

#[test]
fn a_newer_snapshot_drops_the_older_transfer() {
    let mut current = None;
    let old = requests(18, &[1u8; 150], 100);
    assert!(
        receive(&mut current, old[0].clone())
            .expect("old")
            .is_none()
    );
    let new = requests(30, &[2u8; 50], 100);
    let (got_meta, data) = receive(&mut current, new[0].clone())
        .expect("new")
        .expect("complete");
    assert_eq!(got_meta, meta(30));
    assert_eq!(data, vec![2u8; 50]);
}

#[test]
fn a_chunk_that_does_not_continue_the_transfer_is_a_mismatch() {
    let chunks = requests(18, &[1u8; 250], 100);

    // The middle of a transfer this member never saw the start of.
    let err = receive(&mut None, chunks[1].clone()).expect_err("no start");
    assert!(wire::is_snapshot_mismatch(&err), "{err}");

    // A hole: the second chunk was lost.
    let mut current = None;
    assert!(
        receive(&mut current, chunks[0].clone())
            .expect("first")
            .is_none()
    );
    let err = receive(&mut current, chunks[2].clone()).expect_err("a hole");
    assert!(wire::is_snapshot_mismatch(&err), "{err}");
    assert!(current.is_none(), "the broken transfer is dropped");
}

/// The chunk a 0.9 leader sends, byte for byte, is accepted.
#[test]
fn a_chunk_from_0_9_is_accepted() {
    let bytes = crate::raft::wire::tests::fixture("install_snapshot_request");
    let mut chunk: wire::InstallSnapshotRequest = serde_json::from_slice(&bytes).expect("parse");
    chunk.offset = 0;
    let (got_meta, data) = receive(&mut None, chunk)
        .expect("accepted")
        .expect("complete");
    assert_eq!(got_meta.snapshot_id, "18-1762819200000000000");
    assert_eq!(data, vec![7, 8, 9]);
}
