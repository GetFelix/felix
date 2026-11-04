use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::anyhow;

use super::retry_settling;

#[tokio::test]
async fn a_settling_refusal_is_retried_until_it_clears() {
    let calls = AtomicU32::new(0);
    let got = retry_settling(|| async {
        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
            Err(anyhow!(
                "group poll not served: this broker has no committed mark for the shard yet"
            ))
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
    let got: anyhow::Result<()> = retry_settling(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(anyhow!("not the shard leader"))
    })
    .await;
    assert!(got.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
