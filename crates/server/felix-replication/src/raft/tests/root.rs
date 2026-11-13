//! Opening a data root through the restore gate.

use felix_storage::StorageId;
use felix_storage::raft_copy::{ControlPlaneCopy, CopyRoot, VoteMark};
use felix_storage::storage_identity;
use tokio_util::sync::CancellationToken;

use super::super::root::{RootReport, open_root};
use super::*;

const RETRY: Duration = Duration::from_millis(10);

/// A root with a storage id whose marks have been written: one copy opened
/// and a vote saved, so its sequence is above 0.
async fn root_with_marks(dir: &Path) -> (StorageId, u64) {
    storage_identity::load_or_create(dir, || [1; 16]).expect("id");
    let stop = CancellationToken::new();
    let copies = open_root(dir, None, false, || async { None }, RETRY, &stop)
        .await
        .expect("open")
        .expect("a root with no marks opens");
    let opened = copies
        .open_copy("t/ns/s/0/Stream", &dir.join("shard"), "shard", log_config())
        .await
        .expect("open copy");
    opened
        .marks
        .vote(VoteMark {
            generation: 2,
            leader: None,
            committed: false,
        })
        .await
        .expect("vote");
    opened.log.close().await.expect("close");
    let report = RootReport::of(copies.mark_log());
    assert!(report.sequence > 0, "{report:?}");
    (report.storage_id, report.sequence)
}

fn opened_id(copies: &CopyRoot) -> StorageId {
    copies.storage_id()
}

/// No witness and no control-plane copy: the root is held, and opens on the
/// first answer that carries a copy it is not behind.
#[tokio::test]
async fn a_root_nothing_vouches_for_waits_for_the_control_plane() {
    let dir = tempfile::tempdir().expect("dir");
    let (id, sequence) = root_with_marks(dir.path()).await;
    let asked = std::sync::atomic::AtomicU32::new(0);
    let stop = CancellationToken::new();
    let copies = open_root(
        dir.path(),
        None,
        false,
        || {
            let n = asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                (n >= 3).then_some(ControlPlaneCopy {
                    storage_id: id,
                    sequence,
                })
            }
        },
        RETRY,
        &stop,
    )
    .await
    .expect("open")
    .expect("opened");
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(opened_id(&copies), id);
    assert_eq!(copies.mark_log().sequence(), sequence);
}

/// Held, it opens nothing however long it waits; a stop ends the wait.
#[tokio::test]
async fn a_held_root_opens_nothing_before_the_stop() {
    let dir = tempfile::tempdir().expect("dir");
    root_with_marks(dir.path()).await;
    let stop = CancellationToken::new();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        stopper.cancel();
    });
    let opened = open_root(dir.path(), None, false, || async { None }, RETRY, &stop)
        .await
        .expect("open");
    assert!(opened.is_none());
}

/// A root below the control plane's copy was put back from a backup: it
/// gets a new storage id and its mark log starts over, so every copy in it
/// opens as a new member.
#[tokio::test]
async fn a_root_behind_the_control_plane_opens_under_a_new_storage_id() {
    let dir = tempfile::tempdir().expect("dir");
    let (id, sequence) = root_with_marks(dir.path()).await;
    let stop = CancellationToken::new();
    let copies = open_root(
        dir.path(),
        None,
        false,
        || async move {
            Some(ControlPlaneCopy {
                storage_id: id,
                sequence: sequence + 1,
            })
        },
        RETRY,
        &stop,
    )
    .await
    .expect("open")
    .expect("opened");
    assert_ne!(opened_id(&copies), id);
    assert_eq!(copies.mark_log().sequence(), 0);
    let opened = copies
        .open_copy(
            "t/ns/s/0/Stream",
            &dir.path().join("shard"),
            "shard",
            log_config(),
        )
        .await
        .expect("open copy");
    assert!(opened.rotation.is_some(), "the old copy kept its id");
    opened.log.close().await.expect("close");
}
