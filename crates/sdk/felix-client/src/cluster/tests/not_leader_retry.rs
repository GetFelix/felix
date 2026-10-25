//! Conditional publishes, commits and state reads answered with a coded
//! `not_leader`, against a stub that refuses once and then serves.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use felix_wire::{ErrorCode, Message};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::CommitOp;
use crate::cluster::ClusterClient;

/// A stub that answers `code` to its first request and serves the rest.
async fn refuses_once(code: ErrorCode) -> Result<(StubBroker, ClusterClient)> {
    let calls = Arc::new(AtomicUsize::new(0));
    let (broker, cert) = StubBroker::start(move |_| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Message::Error {
                message: format!("refused with {code}"),
                retry: Some(code.default_retry()),
                code: Some(code.clone()),
                detail: None,
            }
        } else {
            Message::PublishOk {
                request_id: 0,
                offset: None,
            }
        }
    })?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    Ok((broker, cluster))
}

fn sent(broker: &StubBroker) -> Vec<&'static str> {
    broker.named().into_iter().map(|(kind, ..)| kind).collect()
}

fn ops() -> Vec<CommitOp> {
    vec![CommitOp::enqueue("orders", "tick")]
}

/// **A coded `not_leader` on a first call is asked again**, as a plain publish
/// is. The broker refuses before it applies anything, so the retry cannot
/// write twice.
#[tokio::test]
#[serial_test::serial]
async fn publish_if_is_asked_again_after_not_leader() -> Result<()> {
    let (broker, cluster) = refuses_once(ErrorCode::NotLeader).await?;
    let written = cluster
        .publish_if("t1", "default", "orders", None, vec![b"x".to_vec()], 0)
        .await?;
    assert_eq!(written, crate::ConditionalWrite::Written { offset: 7 });
    assert_eq!(sent(&broker), ["publish_if", "publish_if"]);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn commit_is_asked_again_after_not_leader() -> Result<()> {
    let (broker, cluster) = refuses_once(ErrorCode::NotLeader).await?;
    let receipt = cluster.commit("t1", "default", b"d00", ops()).await?;
    assert_eq!(receipt.offset, 7);
    assert_eq!(sent(&broker), ["commit", "commit"]);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn state_get_is_asked_again_after_not_leader() -> Result<()> {
    let (broker, cluster) = refuses_once(ErrorCode::NotLeader).await?;
    let state = cluster
        .state_get("t1", "default", "orders", b"d00", "k")
        .await?;
    assert_eq!(state.as_of, Some(7));
    assert_eq!(sent(&broker), ["state_get", "state_get"]);
    Ok(())
}

/// **Any other refusal is returned as it came.** `leadership_lost` and
/// `quorum_timeout` can follow an apply, so sending again could write twice.
/// The text names the operation, not a group request.
#[tokio::test]
#[serial_test::serial]
async fn other_refusals_are_not_retried_and_name_the_operation() -> Result<()> {
    for code in [ErrorCode::LeadershipLost, ErrorCode::InvalidRequest] {
        let (broker, cluster) = refuses_once(code.clone()).await?;
        let refused = cluster
            .publish_if("t1", "default", "orders", None, vec![b"x".to_vec()], 0)
            .await
            .expect_err("refused");
        let text = format!("{refused:#}");
        assert!(text.contains("publish_if refused"), "{text}");
        assert!(!text.contains("group request"), "{text}");
        assert_eq!(sent(&broker), ["publish_if"], "{code}");

        let (broker, cluster) = refuses_once(code.clone()).await?;
        let refused = cluster
            .commit("t1", "default", b"d00", ops())
            .await
            .expect_err("refused");
        assert!(format!("{refused:#}").contains("commit refused"));
        assert_eq!(sent(&broker), ["commit"], "{code}");

        let (broker, cluster) = refuses_once(code.clone()).await?;
        let refused = cluster
            .state_get("t1", "default", "orders", b"d00", "k")
            .await
            .expect_err("refused");
        assert!(format!("{refused:#}").contains("state_get refused"));
        assert_eq!(sent(&broker), ["state_get"], "{code}");
    }
    Ok(())
}
