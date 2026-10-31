//! Conditional head trims, against a stub that answers as scripted.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use felix_wire::{CursorErrorReason, ErrorCode, Message, PublishRefusalReason, RetryClass};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::Client;
use crate::{BrokerError, ConditionalTrim, SubscribeCursorError, TrimError};

async fn client_of(
    script: impl Fn(u64) -> Message + Send + Sync + 'static,
    head_trim: bool,
) -> Result<(StubBroker, Client)> {
    let (broker, cert) = StubBroker::start(script)?;
    if head_trim {
        broker.honour_head_trim();
    }
    let cluster =
        Client::connect_with_policy(&[broker.addr], "localhost", default_config(cert)?, policy())
            .await?;
    Ok((broker, cluster))
}

fn trims(broker: &StubBroker) -> usize {
    broker
        .named()
        .into_iter()
        .filter(|(kind, ..)| *kind == "trim_before")
        .count()
}

fn refused(code: ErrorCode, reason: Option<&str>) -> Message {
    Message::PublishError {
        request_id: 0,
        message: format!("refused with {code}"),
        retry: Some(code.default_retry()),
        code: Some(code),
        detail: reason.map(|reason| felix_wire::ErrorDetail {
            reason: Some(reason.into()),
            ..felix_wire::ErrorDetail::default()
        }),
    }
}

async fn trim(cluster: &Client) -> Result<ConditionalTrim> {
    cluster
        .trim_before("t1", "default", "orders", 2, 5, 9, Some(b"holder-2"))
        .await
}

/// **A broker that never advertised head trims is not sent the request.** An
/// older broker would drop the unknown frame and cost the connection.
#[tokio::test]
#[serial_test::serial]
async fn an_unsupporting_broker_is_not_sent_a_trim() -> Result<()> {
    let (broker, cluster) = client_of(|_| Message::Ok, false).await?;
    let error = trim(&cluster).await.expect_err("unsupported");
    assert_eq!(
        error.downcast_ref::<TrimError>(),
        Some(&TrimError::Unsupported)
    );
    assert_eq!(trims(&broker), 0);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn a_trim_is_answered_with_where_it_landed_and_the_floor() -> Result<()> {
    let (broker, cluster) = client_of(|_| Message::Ok, true).await?;
    assert_eq!(
        trim(&cluster).await?,
        ConditionalTrim::Trimmed {
            offset: Some(7),
            oldest: 5
        }
    );
    assert_eq!(broker.named()[0].1, Some(2), "the shard is named");
    Ok(())
}

/// **A stale tail is an answer and is never sent again.** The retry would be
/// refused the same way, or by the trim it is retrying.
#[tokio::test]
#[serial_test::serial]
async fn a_stale_tail_is_an_answer_and_not_retried() -> Result<()> {
    let (broker, cluster) = client_of(
        |request_id| Message::PublishRefused {
            request_id,
            reason: PublishRefusalReason::OffsetMismatch { tail: 10 },
            message: "stale".into(),
        },
        true,
    )
    .await?;
    assert_eq!(trim(&cluster).await?, ConditionalTrim::Refused { tail: 10 });
    assert_eq!(trims(&broker), 1);
    Ok(())
}

/// **A timed-out trim may have landed, so it is returned as it came** and
/// not sent again: the caller reads the shard to find out.
#[tokio::test]
#[serial_test::serial]
async fn a_timed_out_trim_is_ambiguous_and_not_retried() -> Result<()> {
    let (broker, cluster) = client_of(
        |_| refused(ErrorCode::Unacknowledged, Some("trim_timed_out")),
        true,
    )
    .await?;
    let error = trim(&cluster).await.expect_err("ambiguous");
    let broker_error = error.downcast_ref::<BrokerError>().expect("typed");
    assert_eq!(broker_error.code, ErrorCode::Unacknowledged);
    assert_eq!(broker_error.retry, RetryClass::OutcomeUnknown);
    assert_eq!(broker_error.reason(), Some("trim_timed_out"));
    assert_eq!(trims(&broker), 1);
    Ok(())
}

/// **Each refusal the caller must act on is typed and surfaced as is.**
#[tokio::test]
#[serial_test::serial]
async fn other_refusals_are_typed_and_not_retried() -> Result<()> {
    let cases = [
        (ErrorCode::FeatureNotEnabled, Some("head_trim")),
        (ErrorCode::StreamHasCommits, None),
        (ErrorCode::InvalidRequest, Some("trim_past_tail")),
        (ErrorCode::InvalidRequest, Some("not_durable")),
        (ErrorCode::Forbidden, None),
    ];
    for (code, reason) in cases {
        let scripted = code.clone();
        let (broker, cluster) = client_of(move |_| refused(scripted.clone(), reason), true).await?;
        let error = trim(&cluster).await.expect_err("refused");
        let typed = error.downcast_ref::<BrokerError>().expect("typed");
        assert_eq!(typed.code, code);
        assert_eq!(typed.reason(), reason);
        assert!(format!("{error:#}").contains("trim_before refused"));
        assert_eq!(trims(&broker), 1, "{code}");
    }
    Ok(())
}

/// **A coded `not_leader` is asked again**: the broker answers it before it
/// writes anything.
#[tokio::test]
#[serial_test::serial]
async fn a_trim_is_asked_again_after_not_leader() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let (broker, cluster) = client_of(
        move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                refused(ErrorCode::NotLeader, None)
            } else {
                Message::Ok
            }
        },
        true,
    )
    .await?;
    assert_eq!(
        trim(&cluster).await?,
        ConditionalTrim::Trimmed {
            offset: Some(7),
            oldest: 5
        }
    );
    assert_eq!(trims(&broker), 2);
    Ok(())
}

/// **A read below the floor names the trim that set it.**
#[tokio::test]
#[serial_test::serial]
async fn a_cursor_error_carries_the_trim_headers() -> Result<()> {
    let (_broker, cluster) = client_of(
        |_| Message::SubscribeCursorError {
            reason: CursorErrorReason::TooOld,
            requested: 1,
            available: 5,
            trim_headers: Some(bytes::Bytes::from_static(b"holder-2")),
        },
        true,
    )
    .await?;
    let cluster = Arc::new(cluster);
    let error = cluster
        .subscribe_from(
            "t1",
            "default",
            "orders",
            Some(felix_wire::StartPosition::Offset(1)),
        )
        .await
        .err()
        .expect("too old");
    let cursor = error.downcast_ref::<SubscribeCursorError>().expect("typed");
    assert_eq!(cursor.reason, CursorErrorReason::TooOld);
    assert_eq!(cursor.available, 5);
    assert_eq!(cursor.trim_headers.as_deref(), Some(&b"holder-2"[..]));
    Ok(())
}
