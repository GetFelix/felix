use tokio::time::Duration;

use crate::publish::PublishAdmission;

#[tokio::test]
async fn publish_admission_bounds_shared_inflight_bytes() {
    let admission = PublishAdmission::new(4);
    let permit = admission.acquire(4).await.expect("initial permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
            .await
            .is_err()
    );
    drop(permit);
    let _permit = admission.acquire(1).await.expect("released permit");
}

/// A publish bigger than the budget (but under the frame cap) is not refused:
/// it waits for everything in flight and then holds the whole budget.
#[tokio::test]
async fn an_oversized_publish_goes_out_alone() {
    let admission = PublishAdmission::new(4);
    let small = admission.acquire(1).await.expect("small permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(5))
            .await
            .is_err(),
        "the oversized publish did not wait for the one in flight"
    );
    drop(small);
    let big = admission.acquire(5).await.expect("oversized permit");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), admission.acquire(1))
            .await
            .is_err(),
        "something went out alongside the oversized publish"
    );
    drop(big);
    let _permit = admission.acquire(1).await.expect("released permit");
}
