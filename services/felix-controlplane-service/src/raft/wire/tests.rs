//! The fixtures in `fixtures/openraft-0.9` are the JSON openraft 0.9.25
//! wrote for the control plane's type configuration, which is what the 0.7
//! releases store and send. They were printed by that library's own serde,
//! not written by hand. A change that alters one byte here breaks a rolling
//! upgrade or a rollback.
use super::*;

pub(crate) fn fixture(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/src/raft/fixtures/openraft-0.9/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|err| panic!("{path}: {err}"))
}

/// Reads the 0.9 bytes as `T` and writes them back unchanged.
fn same_bytes<T: Serialize + serde::de::DeserializeOwned>(name: &str) -> T {
    let bytes = fixture(name);
    let value: T = serde_json::from_slice(&bytes).unwrap_or_else(|err| panic!("{name}: {err}"));
    let written = serde_json::to_vec(&value).expect("serialize");
    assert_eq!(
        String::from_utf8_lossy(&written),
        String::from_utf8_lossy(&bytes),
        "{name}"
    );
    value
}

#[test]
fn stored_forms_are_the_bytes_0_9_wrote() {
    same_bytes::<Vote>("stored_vote");
    same_bytes::<Vote>("stored_vote_uncommitted");
    same_bytes::<Entry>("stored_entry_blank");
    same_bytes::<Entry>("stored_entry_normal");
    same_bytes::<Entry>("stored_entry_membership");
    same_bytes::<Entry>("stored_entry_joint_membership");
    same_bytes::<SnapshotMeta>("stored_snapshot_meta");
    same_bytes::<SnapshotMeta>("stored_snapshot_meta_empty");
    same_bytes::<Option<LogId>>("stored_committed");
    same_bytes::<LogId>("stored_last_purged");
}

#[test]
fn peer_requests_and_responses_are_the_bytes_0_9_sent() {
    same_bytes::<VoteRequest>("vote_request");
    same_bytes::<VoteRequest>("vote_request_empty_log");
    same_bytes::<Reply<VoteResponse>>("vote_response");
    same_bytes::<AppendEntriesRequest>("append_entries_request");
    same_bytes::<AppendEntriesRequest>("append_entries_heartbeat");
    same_bytes::<Reply<AppendEntriesResponse>>("append_entries_success");
    same_bytes::<Reply<AppendEntriesResponse>>("append_entries_partial");
    same_bytes::<Reply<AppendEntriesResponse>>("append_entries_partial_none");
    same_bytes::<Reply<AppendEntriesResponse>>("append_entries_conflict");
    same_bytes::<Reply<AppendEntriesResponse>>("append_entries_higher_vote");
    same_bytes::<InstallSnapshotRequest>("install_snapshot_request");
    same_bytes::<Reply<InstallSnapshotResponse>>("install_snapshot_response");
}

/// What the library is handed and hands back loses nothing on the way.
#[test]
fn the_library_types_carry_every_field() {
    let vote: Vote = same_bytes("stored_vote");
    assert_eq!(Vote::from(&types::Vote::from(vote)), vote);

    for name in [
        "stored_entry_blank",
        "stored_entry_normal",
        "stored_entry_membership",
        "stored_entry_joint_membership",
    ] {
        let entry: Entry = same_bytes(name);
        assert_eq!(
            Entry::from(&types::Entry::from(entry.clone())),
            entry,
            "{name}"
        );
    }
}

/// The id is the one field the library no longer has. It comes back as the
/// snapshot's position, and everything else is unchanged.
#[test]
fn a_snapshot_meta_keeps_an_id_through_the_library() {
    let stored: SnapshotMeta = same_bytes("stored_snapshot_meta");
    let back = SnapshotMeta::from(&types::SnapshotMeta::from(stored.clone()));
    assert_eq!(back.snapshot_id, "3-2-18");
    assert_eq!(back.last_log_id, stored.last_log_id);
    assert_eq!(back.last_membership, stored.last_membership);

    let empty: SnapshotMeta = same_bytes("stored_snapshot_meta_empty");
    let back = SnapshotMeta::from(&types::SnapshotMeta::from(empty));
    assert_eq!(back.snapshot_id, "empty");
}

#[test]
fn a_0_9_error_is_read_as_text_and_a_mismatch_is_recognized() {
    let stopped: Reply<VoteResponse> =
        serde_json::from_slice(&fixture("fatal_stopped")).expect("parse");
    let Reply::Err(err) = stopped else {
        panic!("an error reply");
    };
    assert!(!is_snapshot_mismatch(&err));

    let bytes = fixture("install_snapshot_mismatch");
    let mismatch: Reply<InstallSnapshotResponse> = serde_json::from_slice(&bytes).expect("parse");
    let Reply::Err(err) = mismatch else {
        panic!("an error reply");
    };
    assert!(is_snapshot_mismatch(&err));

    // And the one this build answers is the one 0.9 answered.
    let id = "18-1762819200000000000";
    let ours = Reply::<InstallSnapshotResponse>::Err(snapshot_mismatch((id, 0), (id, 4)));
    assert_eq!(
        serde_json::to_value(&ours).expect("serialize"),
        serde_json::from_slice::<serde_json::Value>(&bytes).expect("parse"),
    );
}
