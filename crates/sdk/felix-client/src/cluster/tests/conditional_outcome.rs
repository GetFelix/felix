//! What a failed `publish_if` says about whether anything was written.

use anyhow::Result;
use felix_wire::{ErrorCode, Message, RetryClass};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::Client;
use crate::{AckLost, outcome_unknown};

async fn cluster_answering(answer: Message) -> Result<(StubBroker, Client)> {
    let (broker, cert) = StubBroker::start(move |_| answer.clone())?;
    let cluster =
        Client::connect_with_policy(&[broker.addr], "localhost", default_config(cert)?, policy())
            .await?;
    Ok((broker, cluster))
}

async fn publish_if(cluster: &Client) -> Result<crate::ConditionalWrite> {
    cluster
        .publish_if("t1", "default", "orders", None, vec![b"x".to_vec()], 0)
        .await
}

fn coded(code: ErrorCode, retry: RetryClass) -> Message {
    Message::Error {
        message: "no".into(),
        retry: Some(retry),
        code: Some(code),
        detail: None,
    }
}

/// **A stream that dies after the send is an unknown outcome**, not a plain
/// transport error: the batch, or a prefix of it, may be in the log.
#[tokio::test]
#[serial_test::serial]
async fn a_stream_lost_after_the_send_is_unknown() -> Result<()> {
    let (_broker, cluster) = cluster_answering(Message::Ok).await?;
    let err = publish_if(&cluster).await.unwrap_err();
    assert!(err.chain().any(|e| e.is::<AckLost>()), "{err:#}");
    assert!(outcome_unknown(&err));
    Ok(())
}

/// **A broker that gave up waiting says the same.** `quorum_timeout` carries
/// `outcome_unknown`.
#[tokio::test]
#[serial_test::serial]
async fn a_quorum_timeout_is_unknown() -> Result<()> {
    let (_broker, cluster) =
        cluster_answering(coded(ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown)).await?;
    let err = publish_if(&cluster).await.unwrap_err();
    assert!(outcome_unknown(&err), "{err:#}");
    Ok(())
}

/// **A refusal before anything was written is not unknown.** A stale offset
/// is an answer, covered with the classifier's unit test.
#[tokio::test]
#[serial_test::serial]
async fn forbidden_wrote_nothing() -> Result<()> {
    let (_broker, cluster) =
        cluster_answering(coded(ErrorCode::Forbidden, RetryClass::Fatal)).await?;
    let err = publish_if(&cluster).await.unwrap_err();
    assert!(!outcome_unknown(&err), "{err:#}");

    Ok(())
}

/// **An answer that says nothing about the write is unknown too.** Neither an
/// error without a code nor a stray message tells the writer it is safe.
#[tokio::test]
#[serial_test::serial]
async fn an_uncoded_error_or_a_stray_answer_is_unknown() -> Result<()> {
    let uncoded = Message::Error {
        message: "boom".into(),
        retry: None,
        code: None,
        detail: None,
    };
    let (_broker, cluster) = cluster_answering(uncoded).await?;
    let err = publish_if(&cluster).await.unwrap_err();
    assert!(outcome_unknown(&err), "{err:#}");

    let (_broker, cluster) = cluster_answering(Message::CacheOk { request_id: 0 }).await?;
    let err = publish_if(&cluster).await.unwrap_err();
    assert!(outcome_unknown(&err), "{err:#}");
    Ok(())
}
