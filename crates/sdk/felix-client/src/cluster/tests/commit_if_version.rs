//! Commits that require a key's version, against a stub that answers as
//! scripted.

use anyhow::Result;
use felix_wire::{Message, PublishRefusalReason};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::Client;
use crate::{CommitError, CommitOp, PublishRefused, StateCondition};

async fn client_of(
    script: impl Fn(u64) -> Message + Send + Sync + 'static,
    honours: bool,
) -> Result<(StubBroker, Client)> {
    let (broker, cert) = StubBroker::start(script)?;
    if honours {
        broker.honour_commit_if_version();
    }
    let cluster =
        Client::connect_with_policy(&[broker.addr], "localhost", default_config(cert)?, policy())
            .await?;
    Ok((broker, cluster))
}

fn commits(broker: &StubBroker) -> usize {
    broker
        .named()
        .into_iter()
        .filter(|(kind, ..)| *kind == "commit")
        .count()
}

fn ops() -> Vec<CommitOp> {
    vec![
        CommitOp::publish("orders", "placed"),
        CommitOp::put_if("orders", "order-1", "placed", StateCondition::Version(3)),
    ]
}

/// **A broker that never advertised the bit is not sent the field.** It
/// would ignore `if_version` and make the change unconditionally.
#[tokio::test]
#[serial_test::serial]
async fn an_unsupporting_broker_is_not_sent_a_precondition() -> Result<()> {
    let (broker, cluster) = client_of(|_| Message::Ok, false).await?;
    let error = cluster
        .commit("t1", "default", b"order-1", ops())
        .await
        .expect_err("unsupported");
    assert_eq!(
        error.downcast_ref::<CommitError>(),
        Some(&CommitError::IfVersionUnsupported)
    );
    assert_eq!(commits(&broker), 0);
    Ok(())
}

/// A commit with no precondition is the request it always was, and reaches a
/// broker that lacks the bit.
#[tokio::test]
#[serial_test::serial]
async fn an_unconditional_commit_still_reaches_an_older_broker() -> Result<()> {
    let (broker, cluster) = client_of(|_| Message::Ok, false).await?;
    let receipt = cluster
        .commit(
            "t1",
            "default",
            b"order-1",
            vec![
                CommitOp::publish("orders", "placed"),
                CommitOp::put("orders", "order-1", "placed"),
            ],
        )
        .await?;
    assert_eq!(receipt.offset, 7);
    assert_eq!(commits(&broker), 1);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn a_version_mismatch_is_a_typed_refusal() -> Result<()> {
    let (_broker, cluster) = client_of(
        |request_id| Message::PublishRefused {
            request_id,
            reason: PublishRefusalReason::VersionMismatch {
                key: "order-1".to_string(),
                version: Some(5),
            },
            message: "stale".to_string(),
        },
        true,
    )
    .await?;
    let error = cluster
        .commit("t1", "default", b"order-1", ops())
        .await
        .expect_err("refused");
    assert_eq!(
        error.downcast_ref::<PublishRefused>().map(|r| &r.reason),
        Some(&PublishRefusalReason::VersionMismatch {
            key: "order-1".to_string(),
            version: Some(5),
        })
    );
    Ok(())
}
