//! A follower whose `epochs` file is gone or unreadable still sends a newer
//! leader back over the records it holds past its commit offset (#2757).

use super::*;

enum Damage {
    Absent,
    Short,
    Corrupt,
}

/// Every `epochs` file under `root`, which holds one broker's data.
fn epochs_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name().is_some_and(|name| name == "epochs") {
                found.push(path);
            }
        }
    }
    found
}

fn damage(root: &std::path::Path, how: &Damage) {
    let files = epochs_files(root);
    assert!(!files.is_empty(), "no generation history was written");
    for path in files {
        match how {
            Damage::Absent => std::fs::remove_file(&path).expect("remove"),
            Damage::Short => {
                let bytes = std::fs::read(&path).expect("read");
                std::fs::write(&path, &bytes[..bytes.len() / 2]).expect("write");
            }
            Damage::Corrupt => {
                let mut bytes = std::fs::read(&path).expect("read");
                let last = bytes.len() - 1;
                bytes[last] ^= 0xFF;
                std::fs::write(&path, bytes).expect("write");
            }
        }
    }
}

/// Offset 2 is a dead generation-4 leader's unacknowledged write and the
/// history saying so is lost across a restart. A generation-6 leader starting
/// at this follower's tail must be sent back to the commit offset, so the
/// stale record is compared rather than kept under the new log.
async fn sent_back_to_commit(how: Damage) {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let old = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        let mut first = batch(4, 0, &["a", "b"]);
        first.commit_offset = Some(2);
        old.apply(SENDER, first, felix_broker::LogKind::Stream)
            .await;
        old.apply(
            SENDER,
            batch(4, 2, &["stale"]),
            felix_broker::LogKind::Stream,
        )
        .await;
        broker
            .durable_storage()
            .expect("storage")
            .shutdown()
            .await
            .expect("shutdown");
    }
    damage(dir.path(), &how);

    let broker = broker_on(dir.path());
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let new = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    let answer = new
        .apply(SENDER, batch(6, 3, &["d"]), felix_broker::LogKind::Stream)
        .await;
    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::LogGap);
    assert_eq!(refused.expected_offset, 2, "sent back to the commit offset");

    // The resend reaches the stale record. With no history to say a dead
    // leader wrote it, the follower stops rather than keep it or guess.
    let answer = new
        .apply(
            SENDER,
            batch(6, 2, &["c", "d"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::LogConflict);
    assert_eq!(held(&broker).await, ["a", "b", "stale"]);
}

#[tokio::test]
async fn an_absent_history_sends_a_newer_leader_back_to_commit() {
    sent_back_to_commit(Damage::Absent).await;
}

#[tokio::test]
async fn a_short_history_sends_a_newer_leader_back_to_commit() {
    sent_back_to_commit(Damage::Short).await;
}

#[tokio::test]
async fn a_corrupt_history_sends_a_newer_leader_back_to_commit() {
    sent_back_to_commit(Damage::Corrupt).await;
}

/// Where the records past commit agree with the new leader, the one resend
/// compares them and replication carries on from there.
#[tokio::test]
async fn a_lost_history_costs_one_resend_when_the_records_agree() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = broker_on(dir.path());
        let old = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        let mut first = batch(4, 0, &["a", "b"]);
        first.commit_offset = Some(2);
        old.apply(SENDER, first, felix_broker::LogKind::Stream)
            .await;
        old.apply(SENDER, batch(4, 2, &["c"]), felix_broker::LogKind::Stream)
            .await;
        broker
            .durable_storage()
            .expect("storage")
            .shutdown()
            .await
            .expect("shutdown");
    }
    damage(dir.path(), &Damage::Corrupt);

    let broker = broker_on(dir.path());
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let new = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 6));
    let answer = new
        .apply(SENDER, batch(6, 3, &["d"]), felix_broker::LogKind::Stream)
        .await;
    assert_eq!(refusal(&answer).expected_offset, 2);
    let answer = new
        .apply(
            SENDER,
            batch(6, 2, &["c", "d"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let answer = new
        .apply(SENDER, batch(6, 4, &["e"]), felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    assert_eq!(held(&broker).await, ["a", "b", "c", "d", "e"]);
}
