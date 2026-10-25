use std::sync::atomic::Ordering;
use std::time::Duration;

use felix_client::{CommitError, CommitOp};

use super::BareBroker;

/// A client refuses commit and state_get against a broker that advertised
/// nothing, and the broker is sent neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_sends_no_commit_to_a_broker_that_advertised_none() {
    let bare = BareBroker::start().expect("start the bare broker");
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        felix_cluster::client::connect(bare.addr, "t1", "token"),
    )
    .await
    .expect("connect timed out")
    .expect("connect");

    let commit = tokio::time::timeout(
        Duration::from_secs(5),
        client.commit("t1", "default", b"k", vec![CommitOp::publish("s", "e")]),
    )
    .await
    .expect("a commit must be refused at the call, not left waiting")
    .expect_err("commit must be refused");
    assert!(
        matches!(
            commit.downcast_ref::<CommitError>(),
            Some(CommitError::Unsupported)
        ),
        "commit gave {commit:?}"
    );

    let state = tokio::time::timeout(
        Duration::from_secs(5),
        client.state_get("t1", "default", "s", b"k", "k"),
    )
    .await
    .expect("a state read must be refused at the call, not left waiting")
    .expect_err("state_get must be refused");
    assert!(
        matches!(
            state.downcast_ref::<CommitError>(),
            Some(CommitError::Unsupported)
        ),
        "state_get gave {state:?}"
    );

    assert_eq!(bare.requests.load(Ordering::SeqCst), 0);
}
