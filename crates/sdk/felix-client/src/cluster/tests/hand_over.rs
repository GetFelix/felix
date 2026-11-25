//! A plain publish into a shard that is between leaders: the broker refuses
//! before it proposes anything, and the client sends the batch again.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_wire::{AckMode, ErrorCode, ErrorDetail, Message};

use super::stub_broker::StubBroker;
use crate::cluster::{Client, ReconnectPolicy};
use crate::test_support::build_client_config_with_overrides;
use crate::{BrokerError, ClientConfig};

const ATTEMPTS: usize = 5;

fn fast_policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: ATTEMPTS,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: None,
    }
}

fn refusal(request_id: u64, code: ErrorCode, reason: Option<&str>) -> Message {
    Message::PublishError {
        request_id,
        message: format!("refused with {code}"),
        retry: Some(code.default_retry()),
        code: Some(code),
        detail: reason.map(|reason| ErrorDetail {
            reason: Some(reason.to_string()),
            ..ErrorDetail::default()
        }),
    }
}

/// A stub that refuses its first `refusals` publishes with `code` and serves
/// the rest, as a shard does while its leadership is handed over.
async fn handing_over(
    refusals: usize,
    code: ErrorCode,
    reason: Option<&'static str>,
) -> Result<(StubBroker, Client)> {
    let seen = Arc::new(AtomicUsize::new(0));
    let (broker, cert) = StubBroker::start(move |id| {
        if seen.fetch_add(1, Ordering::SeqCst) < refusals {
            refusal(id, code.clone(), reason)
        } else {
            Message::PublishOk {
                request_id: id,
                offset: None,
            }
        }
    })?;
    let config: ClientConfig = build_client_config_with_overrides(cert, 0)?;
    let client =
        Client::connect_with_policy(&[broker.addr], "localhost", config, fast_policy()).await?;
    Ok((broker, client))
}

/// **Every plain publish kind rides out a hand-over.** The broker that formed
/// a Raft group hands its first term to the placed leader, and a publish in
/// that window is refused `not_ready` before anything is proposed. Sent
/// again, it lands once.
#[tokio::test]
#[serial_test::serial]
async fn a_publish_refused_during_a_hand_over_is_sent_again() -> Result<()> {
    let not_ready = || handing_over(2, ErrorCode::ShardUnavailable, Some("not_ready"));
    let ack = AckMode::PerMessage;

    let (broker, client) = not_ready().await?;
    client
        .publish("t1", "default", "orders", b"x".to_vec(), ack)
        .await?;
    assert_eq!(broker.publishes(), 3, "publish");

    let (broker, client) = not_ready().await?;
    client
        .publish_keyed("t1", "default", "orders", b"x".to_vec(), "k".into(), ack)
        .await?;
    assert_eq!(broker.publishes(), 3, "publish_keyed");

    let (broker, client) = not_ready().await?;
    client
        .publish_batch("t1", "default", "orders", vec![b"x".to_vec()], ack)
        .await?;
    assert_eq!(broker.publishes(), 3, "publish_batch");

    let (broker, client) = not_ready().await?;
    client
        .publish_batch_keyed(
            "t1",
            "default",
            "orders",
            "k".into(),
            vec![b"x".to_vec()],
            ack,
        )
        .await?;
    assert_eq!(broker.publishes(), 3, "publish_batch_keyed");

    let (broker, client) = not_ready().await?;
    let report = client
        .publish_records(
            "t1",
            "default",
            "orders",
            vec![("k".into(), b"x".to_vec())],
            ack,
        )
        .await;
    report.into_result()?;
    assert_eq!(broker.publishes(), 3, "publish_records");
    Ok(())
}

/// **Each refusal that comes before a proposal is retried**: the shard has no
/// leader yet, is moving, leads elsewhere, or the broker has no room.
#[tokio::test]
#[serial_test::serial]
async fn every_refusal_before_a_proposal_is_retried() -> Result<()> {
    for (code, reason) in [
        (ErrorCode::ShardUnavailable, Some("not_ready")),
        (ErrorCode::ShardUnavailable, Some("moving")),
        (ErrorCode::ShardUnavailable, Some("owner_unavailable")),
        (ErrorCode::NotLeader, None),
        (ErrorCode::Overloaded, None),
        (ErrorCode::NotFound, None),
    ] {
        let (broker, client) = handing_over(1, code.clone(), reason).await?;
        client
            .publish(
                "t1",
                "default",
                "orders",
                b"x".to_vec(),
                AckMode::PerMessage,
            )
            .await
            .map_err(|err| err.context(format!("{code} {reason:?}")))?;
        assert_eq!(broker.publishes(), 2, "{code} {reason:?}");
    }
    Ok(())
}

/// **A publish that may have landed is never sent again.** These answers can
/// follow a write, and a plain publish has no sequence to tell a re-send from
/// a new record.
#[tokio::test]
#[serial_test::serial]
async fn an_outcome_in_doubt_is_returned_after_one_attempt() -> Result<()> {
    for code in [
        ErrorCode::QuorumTimeout,
        ErrorCode::LeadershipLost,
        ErrorCode::Unacknowledged,
        ErrorCode::Internal,
        ErrorCode::Storage,
    ] {
        let (broker, client) = handing_over(1, code.clone(), None).await?;
        let err = client
            .publish_keyed(
                "t1",
                "default",
                "orders",
                b"x".to_vec(),
                "k".into(),
                AckMode::PerMessage,
            )
            .await
            .expect_err("refused");
        let broker_error = err.downcast_ref::<BrokerError>().expect("coded");
        assert_eq!(broker_error.code, code);
        assert_eq!(broker.publishes(), 1, "{code}");
    }
    Ok(())
}

/// **The retries are bounded by the policy.** A shard that never comes back
/// costs the policy's attempts, and the caller gets the broker's refusal.
#[tokio::test]
#[serial_test::serial]
async fn a_shard_that_stays_closed_fails_after_the_policys_attempts() -> Result<()> {
    let (broker, client) =
        handing_over(usize::MAX, ErrorCode::ShardUnavailable, Some("not_ready")).await?;
    let err = client
        .publish(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("never served");
    let broker_error = err.downcast_ref::<BrokerError>().expect("coded");
    assert_eq!(broker_error.code, ErrorCode::ShardUnavailable);
    assert_eq!(broker_error.reason(), Some("not_ready"));
    assert_eq!(broker.publishes(), ATTEMPTS);
    Ok(())
}
