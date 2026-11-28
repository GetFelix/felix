use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::anyhow;
use felix_wire::{ErrorCode, RetryClass};

use super::{is_retryable, retry_refused};

fn refusal(code: ErrorCode, retry: RetryClass) -> anyhow::Error {
    anyhow::Error::new(felix_client::BrokerError::new(code, retry, "refused"))
        .context("group settle refused")
}

#[test]
fn only_refusals_that_wrote_nothing_are_retryable() {
    for retryable in [
        refusal(ErrorCode::ShardUnavailable, RetryClass::Retry),
        refusal(ErrorCode::Overloaded, RetryClass::RetryAfter),
    ] {
        assert!(is_retryable(&retryable), "{retryable:#}");
    }
    for final_ in [
        // Pinned to one broker, a redirect cannot be followed.
        refusal(ErrorCode::NotLeader, RetryClass::Redirect),
        refusal(ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown),
        refusal(ErrorCode::Internal, RetryClass::Fatal),
        anyhow!("connection lost"),
        // The wording of an answer is not its class.
        anyhow!("the shard's engine has not settled a batch; retry"),
    ] {
        assert!(!is_retryable(&final_), "{final_:#}");
    }
}

#[tokio::test]
async fn a_retryable_refusal_is_sent_again_until_it_clears() {
    let calls = AtomicU32::new(0);
    let got = retry_refused(|| async {
        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
            Err(refusal(ErrorCode::ShardUnavailable, RetryClass::Retry))
        } else {
            Ok(7)
        }
    })
    .await;
    assert_eq!(got.expect("settled"), 7);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn any_other_refusal_is_returned_at_once() {
    let calls = AtomicU32::new(0);
    let got: anyhow::Result<()> = retry_refused(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(refusal(ErrorCode::Internal, RetryClass::Fatal))
    })
    .await;
    assert!(got.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
