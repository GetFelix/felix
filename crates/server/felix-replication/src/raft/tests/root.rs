//! Opening a data root through the restore gate.

use felix_storage::StorageId;
use felix_storage::raft_copy::{ControlPlaneCopy, CopyRoot, VoteMark};
use felix_storage::storage_identity;
use tokio_util::sync::CancellationToken;

use super::super::root::{RootOpened, RootReport, open_root};
use super::*;

const RETRY: Duration = Duration::from_millis(10);

fn opened(opened: RootOpened) -> CopyRoot {
    match opened {
        RootOpened::Open(copies) => copies,
        other => panic!("the root did not open: {other:?}"),
    }
}

/// A root with a storage id whose marks have been written: one copy opened
/// and a vote saved, so its sequence is above 0.
async fn root_with_marks(dir: &Path) -> (StorageId, u64) {
    let (id, _) = storage_identity::load_or_create(dir, || [1; 16]).expect("id");
    let stop = CancellationToken::new();
    let copies = opened(
        open_root(dir, None, false, id, || async { None }, RETRY, &stop)
            .await
            .expect("open"),
    );
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
    let copies = opened(
        open_root(
            dir.path(),
            None,
            false,
            id,
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
        .expect("open"),
    );
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(opened_id(&copies), id);
    assert_eq!(copies.mark_log().sequence(), sequence);
}

/// Held, it opens nothing however long it waits; a stop ends the wait.
#[tokio::test]
async fn a_held_root_opens_nothing_before_the_stop() {
    let dir = tempfile::tempdir().expect("dir");
    let (id, _) = root_with_marks(dir.path()).await;
    let stop = CancellationToken::new();
    let stopper = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        stopper.cancel();
    });
    let held = open_root(dir.path(), None, false, id, || async { None }, RETRY, &stop)
        .await
        .expect("open");
    assert!(matches!(held, RootOpened::Stopped), "{held:?}");
}

/// A root below the control plane's copy was put back from a backup: it
/// gets a new storage id and is not opened, so the broker stops and
/// registers under the new id. On the next open its mark log has started
/// over and every copy in it opens as a new member.
#[tokio::test]
async fn a_root_behind_the_control_plane_rotates_and_is_not_opened() {
    let dir = tempfile::tempdir().expect("dir");
    let (id, sequence) = root_with_marks(dir.path()).await;
    let stop = CancellationToken::new();
    let rotated = open_root(
        dir.path(),
        None,
        false,
        id,
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
    .expect("open");
    let RootOpened::Rotated {
        storage_id,
        sequence: was,
        control_plane,
        ..
    } = rotated
    else {
        panic!("a root behind the control plane was not rotated: {rotated:?}");
    };
    assert_ne!(storage_id, id);
    assert_eq!((was, control_plane), (sequence, Some(sequence + 1)));
    assert_eq!(
        storage_identity::load(dir.path()).expect("id"),
        Some(storage_id)
    );

    // The restart registers under the new id; the control plane has no copy
    // under it yet.
    let copies = opened(
        open_root(
            dir.path(),
            None,
            false,
            storage_id,
            || async { None },
            RETRY,
            &stop,
        )
        .await
        .expect("reopen"),
    );
    assert_eq!(opened_id(&copies), storage_id);
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

/// **A rotation that fails after writing the new id is not retried into an
/// open.** The retry finds the root at sequence 0 under the new id, which
/// passes the gate, but this run registered under the old one: it must stop
/// the broker as the rotation would have, not open members under an id the
/// control plane never heard of.
#[tokio::test]
async fn a_rotation_that_fails_after_the_id_is_written_does_not_open_on_retry() {
    let dir = tempfile::tempdir().expect("dir");
    let (id, sequence) = root_with_marks(dir.path()).await;
    let stop = CancellationToken::new();
    let behind = || async move {
        Some(ControlPlaneCopy {
            storage_id: id,
            sequence: sequence + 1,
        })
    };
    *super::super::root::FAIL_AFTER_ROTATION
        .lock()
        .expect("hook") = Some(dir.path().to_path_buf());
    let failed = open_root(dir.path(), None, false, id, behind, RETRY, &stop).await;
    *super::super::root::FAIL_AFTER_ROTATION
        .lock()
        .expect("hook") = None;
    assert!(failed.is_err(), "{failed:?}");
    let rotated_to = storage_identity::load(dir.path())
        .expect("id")
        .expect("an id");
    assert_ne!(
        rotated_to, id,
        "the injected failure came before the id write"
    );

    // The caller's retry, still under the id it registered.
    let retried = open_root(dir.path(), None, false, id, behind, RETRY, &stop)
        .await
        .expect("retry");
    match retried {
        RootOpened::NotRegistered {
            storage_id,
            registered,
        } => assert_eq!((storage_id, registered), (rotated_to, id)),
        other => panic!("the retry did not stop: {other:?}"),
    }
}
