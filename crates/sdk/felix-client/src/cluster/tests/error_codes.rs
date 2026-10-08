//! What a [`ClusterClient`] does with a broker's typed error, end to end
//! against stub brokers.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_wire::{AckMode, ErrorCode, ErrorDetail, Message, RetryClass};

use super::stub_broker::StubBroker;
use crate::cluster::{ClusterClient, Owner, ReconnectPolicy, ShardKey};
use crate::test_support::{build_client_config_with_overrides, build_server_config};
use crate::{BrokerError, ClientConfig};

fn fast_policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 5,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: None,
    }
}

fn publish_error(request_id: u64, code: ErrorCode, reason: Option<&str>) -> Message {
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

fn error(code: ErrorCode, reason: Option<&str>) -> Message {
    Message::Error {
        message: format!("refused with {code}"),
        retry: Some(code.default_retry()),
        code: Some(code),
        detail: reason.map(|reason| ErrorDetail {
            reason: Some(reason.to_string()),
            ..ErrorDetail::default()
        }),
    }
}

async fn cluster(seed: &StubBroker, config: ClientConfig) -> Result<ClusterClient> {
    ClusterClient::connect_with_policy(&[seed.addr], "localhost", config, fast_policy()).await
}

fn shard0() -> ShardKey {
    ("t1".into(), "default".into(), "orders".into(), 0)
}

/// **A cached owner that answers "fenced" is dropped and the publish goes
/// through the entry broker at once.** The owner said it did not apply the
/// batch, so sending it elsewhere cannot duplicate it, and the entry broker
/// routes by the current assignment rather than the one this client cached.
#[tokio::test]
#[serial_test::serial]
async fn a_fenced_owner_is_forgotten_and_the_publish_rerouted() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let entry = StubBroker::start_with(server_config.clone(), |id| Message::PublishOk {
        request_id: id,
        offset: None,
    })?;
    let old_owner = StubBroker::start_with(server_config, |id| {
        publish_error(id, ErrorCode::ShardUnavailable, Some("fenced"))
    })?;
    let cluster = cluster(&entry, build_client_config_with_overrides(cert, 0)?).await?;
    let owner_client = cluster.connect_to(old_owner.addr).await?;
    cluster.owners.write().await.insert(
        shard0(),
        Owner {
            node_id: "broker-old".into(),
            generation: 1,
            client: owner_client,
        },
    );

    cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    assert_eq!(old_owner.publishes(), 1);
    assert_eq!(
        entry.publishes(),
        1,
        "the refused batch goes to the entry broker"
    );
    assert!(cluster.owners.read().await.is_empty());
    Ok(())
}

/// **A fatal code ends the retry loop on the first answer.** Before codes the
/// client only recognised "forbidden" in the text, so a request that can never
/// succeed as sent burned the whole attempt schedule.
#[tokio::test]
#[serial_test::serial]
async fn a_fatal_code_is_not_retried() -> Result<()> {
    let (entry, cert) = StubBroker::start(|id| publish_error(id, ErrorCode::InvalidRequest, None))?;
    let cluster = cluster(&entry, build_client_config_with_overrides(cert, 0)?).await?;

    let err = cluster
        .publish_at_least_once(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("refused");

    let broker = err.downcast_ref::<BrokerError>().expect("typed");
    assert_eq!(broker.code, ErrorCode::InvalidRequest);
    assert_eq!(entry.publishes(), 1);
    Ok(())
}

/// **A publish that may have been applied is reported, not re-sent.** Only
/// `publish_at_least_once` and an idempotent producer send it again.
#[tokio::test]
#[serial_test::serial]
async fn an_outcome_unknown_publish_is_surfaced() -> Result<()> {
    let (entry, cert) = StubBroker::start(|id| publish_error(id, ErrorCode::QuorumTimeout, None))?;
    let cluster = cluster(&entry, build_client_config_with_overrides(cert, 0)?).await?;

    let err = cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("refused");

    let broker = err.downcast_ref::<BrokerError>().expect("typed");
    assert_eq!(broker.retry, RetryClass::OutcomeUnknown);
    assert_eq!(entry.publishes(), 1);
    Ok(())
}

/// `publish_at_least_once` re-sends an ambiguous outcome: that is the
/// duplicate its name warns about, and the reason a caller chose it.
#[tokio::test]
#[serial_test::serial]
async fn at_least_once_resends_an_outcome_unknown() -> Result<()> {
    let (entry, cert) = StubBroker::start(|id| publish_error(id, ErrorCode::QuorumTimeout, None))?;
    let cluster = cluster(&entry, build_client_config_with_overrides(cert, 0)?).await?;

    cluster
        .publish_at_least_once(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("refused every time");

    assert_eq!(entry.publishes(), fast_policy().attempts);
    Ok(())
}

/// A subscribe refusal is a [`BrokerError`] with its code and reason, not
/// the debug text of the message it arrived in.
#[tokio::test]
#[serial_test::serial]
async fn a_subscribe_refusal_is_typed() -> Result<()> {
    let (entry, cert) =
        StubBroker::start(|_| error(ErrorCode::ShardUnavailable, Some("not_ready")))?;
    let cluster = Arc::new(cluster(&entry, build_client_config_with_overrides(cert, 1)?).await?);

    let err = cluster
        .subscribe("t1", "default", "orders")
        .await
        .err()
        .expect("refused");

    let broker = err.downcast_ref::<BrokerError>().expect("typed");
    assert_eq!(broker.code, ErrorCode::ShardUnavailable);
    assert_eq!(broker.reason(), Some("not_ready"));
    assert_eq!(
        entry.subscribes(),
        fast_policy().attempts,
        "a shard that stays unready is asked once per attempt, then given up on"
    );
    Ok(())
}

/// **A shard still opening is waited for, not reported.** A promoted leader
/// fences a majority before it serves, and a subscribe that lands in that
/// window must not fail the whole call.
#[tokio::test]
#[serial_test::serial]
async fn a_subscribe_waits_out_a_shard_that_is_still_opening() -> Result<()> {
    let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (entry, cert) = StubBroker::start(move |_| {
        if asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
            error(ErrorCode::ShardUnavailable, Some("not_ready"))
        } else {
            Message::Subscribed {
                subscription_id: 0,
                start_offset: None,
                live_offset: None,
                queue_capacity: None,
            }
        }
    })?;
    let cluster = Arc::new(cluster(&entry, build_client_config_with_overrides(cert, 1)?).await?);

    cluster
        .subscribe("t1", "default", "orders")
        .await
        .expect("opened once the shard was ready");

    assert_eq!(entry.subscribes(), 3);
    Ok(())
}

/// A fatal subscribe refusal is not retried.
#[tokio::test]
#[serial_test::serial]
async fn a_fatal_subscribe_refusal_is_not_retried() -> Result<()> {
    let (entry, cert) = StubBroker::start(|_| error(ErrorCode::Forbidden, None))?;
    let cluster = Arc::new(cluster(&entry, build_client_config_with_overrides(cert, 1)?).await?);

    cluster
        .subscribe("t1", "default", "orders")
        .await
        .err()
        .expect("refused");

    assert_eq!(entry.subscribes(), 1);
    Ok(())
}

/// **An owner reached by redirect that turns out to be fenced sends the
/// subscribe back to the entry broker**, which answers from the current
/// assignment, instead of failing on the stale hop.
#[tokio::test]
#[serial_test::serial]
async fn a_fenced_redirect_target_sends_the_subscribe_back() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let old_owner = StubBroker::start_with(server_config.clone(), |_| {
        error(ErrorCode::ShardUnavailable, Some("fenced"))
    })?;
    let old_addr = old_owner.addr.to_string();
    let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let entry = StubBroker::start_with(server_config, move |_| {
        // First the stale owner, then an answer that shows the second ask.
        if asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            Message::NotLeader {
                node_id: "broker-old".into(),
                addr: Some(old_addr.clone()),
                generation: 1,
            }
        } else {
            error(ErrorCode::Forbidden, None)
        }
    })?;
    let cluster = Arc::new(cluster(&entry, build_client_config_with_overrides(cert, 1)?).await?);

    let err = cluster
        .subscribe("t1", "default", "orders")
        .await
        .err()
        .expect("refused");

    assert_eq!(old_owner.subscribes(), 1);
    assert_eq!(entry.subscribes(), 2, "the entry broker is asked again");
    let broker = err.downcast_ref::<BrokerError>().expect("typed");
    assert_eq!(broker.code, ErrorCode::Forbidden);
    Ok(())
}

/// **A chosen shard is subscribed to on its owner.** The redirect is for that
/// shard, and the shard number travels on both asks.
#[tokio::test]
#[serial_test::serial]
async fn a_chosen_shard_follows_its_own_redirect() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let owner = StubBroker::start_with(server_config.clone(), |_| Message::Subscribed {
        subscription_id: 0,
        start_offset: None,
        live_offset: None,
        queue_capacity: None,
    })?;
    let owner_addr = owner.addr.to_string();
    let entry = StubBroker::start_with(server_config, move |_| Message::NotLeader {
        node_id: "broker-2".into(),
        addr: Some(owner_addr.clone()),
        generation: 1,
    })?;
    let cluster = Arc::new(cluster(&entry, build_client_config_with_overrides(cert, 1)?).await?);

    let subscription = cluster
        .subscribe_shard("t1", "default", "orders", 3, None)
        .await?;

    assert_eq!(entry.subscribed_shards(), [Some(3)]);
    assert_eq!(owner.subscribed_shards(), [Some(3)]);
    assert_eq!(subscription.client().dialled(), owner.addr);
    Ok(())
}
