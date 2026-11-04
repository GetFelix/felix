//! Commits that require a key to hold a version: written only if it still
//! does, with nothing left behind when it does not.

use bytes::Bytes;

use super::conditional::durable_broker;
use crate::error::BrokerError;
use crate::{StateOp, VersionCheck};

fn put(key: &str, value: &'static [u8]) -> Vec<StateOp> {
    vec![StateOp::Put {
        key: key.to_string(),
        value: Bytes::from_static(value),
    }]
}

fn require(key: &str, version: Option<u64>) -> Vec<VersionCheck> {
    vec![VersionCheck {
        key: key.to_string(),
        required: version,
    }]
}

/// A key never written holds no version; a check for none passes once, and
/// then the key holds the commit's offset.
#[tokio::test]
async fn an_absent_key_can_be_created_once() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    let first = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e1"),
            put("k", b"1"),
            None,
            &require("k", None),
            None,
        )
        .await
        .expect("absent key");
    assert_eq!(first.offsets, Some((0, 0)));
    let refused = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e2"),
            put("k", b"2"),
            None,
            &require("k", None),
            None,
        )
        .await
        .expect_err("now present");
    assert!(matches!(
        refused,
        BrokerError::VersionMismatch {
            version: Some(0),
            ..
        }
    ));
}

/// A met precondition writes; an unmet one writes nothing, consumes no
/// offset and leaves the state as it was.
#[tokio::test]
async fn a_stale_version_writes_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .commit_to_handle(&handle, Bytes::from_static(b"e1"), put("k", b"1"), None)
        .await
        .expect("first");
    let second = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e2"),
            put("k", b"2"),
            None,
            &require("k", Some(0)),
            None,
        )
        .await
        .expect("version 0 held");
    assert_eq!(second.offsets, Some((1, 1)));

    let refused = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e3"),
            put("k", b"3"),
            None,
            &require("k", Some(0)),
            None,
        )
        .await
        .expect_err("version is 1 now");
    match refused {
        BrokerError::VersionMismatch { key, version } => {
            assert_eq!((key.as_str(), version), ("k", Some(1)));
        }
        other => panic!("unexpected {other:?}"),
    }
    let read = broker.state_get(&handle, "k").await.expect("read");
    assert_eq!(read.value, Some(Bytes::from_static(b"2")));
    assert_eq!(read.version, Some(1));
    let next = broker
        .commit_to_handle(&handle, Bytes::from_static(b"e4"), put("j", b"x"), None)
        .await
        .expect("next");
    assert_eq!(next.offsets, Some((2, 2)), "the refusal took no offset");
}

/// A delete leaves the key absent, and a check for absent then passes.
#[tokio::test]
async fn a_deleted_key_is_absent_again() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .commit_to_handle(&handle, Bytes::from_static(b"e1"), put("k", b"1"), None)
        .await
        .expect("put");
    broker
        .commit_to_handle(
            &handle,
            Bytes::from_static(b"e2"),
            vec![StateOp::Delete {
                key: "k".to_string(),
            }],
            None,
        )
        .await
        .expect("delete");
    broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e3"),
            put("k", b"2"),
            None,
            &require("k", None),
            None,
        )
        .await
        .expect("absent after delete");
}

/// Every check must hold: the first key to differ is named, and the change
/// that did hold is not applied either.
#[tokio::test]
async fn one_failing_key_refuses_the_whole_commit() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .commit_to_handle(&handle, Bytes::from_static(b"e1"), put("a", b"1"), None)
        .await
        .expect("a");
    let mut checks = require("a", Some(0));
    checks.extend(require("b", Some(7)));
    let refused = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"e2"),
            put("a", b"2"),
            None,
            &checks,
            None,
        )
        .await
        .expect_err("b is absent");
    assert!(matches!(
        refused,
        BrokerError::VersionMismatch { ref key, version: None } if key == "b"
    ));
    let read = broker.state_get(&handle, "a").await.expect("read");
    assert_eq!(read.version, Some(0));
}

/// **Of commits racing on one version, exactly one wins.** A commit takes
/// its offset before it is visible to readers, so a check that read only
/// readable state would let several pass; the claim a commit leaves behind
/// is what the next check sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_racing_on_one_version_have_one_winner() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .commit_to_handle(&handle, Bytes::from_static(b"seed"), put("k", b"0"), None)
        .await
        .expect("seed");
    let mut racers = Vec::new();
    for n in 0..16u8 {
        let broker = broker.clone();
        let handle = handle.clone();
        racers.push(tokio::spawn(async move {
            broker
                .commit_to_handle_checked(
                    &handle,
                    Bytes::from(vec![n]),
                    vec![StateOp::Put {
                        key: "k".to_string(),
                        value: Bytes::from(vec![n]),
                    }],
                    None,
                    &require("k", Some(0)),
                    None,
                )
                .await
        }));
    }
    let mut won = 0;
    for racer in racers {
        match racer.await.expect("task") {
            Ok(_) => won += 1,
            Err(BrokerError::VersionMismatch { .. }) => {}
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(won, 1);
}

/// A commit with no precondition still counts: one that wrote the key ahead
/// of a checked commit changes the version the check sees, so the check
/// never passes against a version that is already gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unconditional_commit_ahead_of_a_check_is_seen_by_it() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, handle) = durable_broker(dir.path()).await;
    broker
        .commit_to_handle(&handle, Bytes::from_static(b"seed"), put("k", b"0"), None)
        .await
        .expect("seed");
    let writer = {
        let (broker, handle) = (broker.clone(), handle.clone());
        tokio::spawn(async move {
            broker
                .commit_to_handle(&handle, Bytes::from_static(b"w"), put("k", b"1"), None)
                .await
        })
    };
    let checked = broker
        .commit_to_handle_checked(
            &handle,
            Bytes::from_static(b"c"),
            put("k", b"2"),
            None,
            &require("k", Some(0)),
            None,
        )
        .await;
    let written = writer.await.expect("task").expect("write");
    match checked {
        // The check took its place first.
        Ok(checked) => assert!(checked.offsets < written.offsets),
        Err(BrokerError::VersionMismatch { version, .. }) => {
            assert_eq!(version, written.offsets.map(|(first, _)| first));
        }
        Err(other) => panic!("unexpected {other:?}"),
    }
}
