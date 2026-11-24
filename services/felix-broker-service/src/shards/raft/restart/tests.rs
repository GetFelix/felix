use super::*;

fn key() -> ShardKey {
    crate::shards::raft::tests::key("s", 0)
}

#[tokio::test(start_paused = true)]
async fn the_first_stop_reopens_at_once_and_repeated_ones_back_off() {
    let mut restarts = Restarts::default();
    restarts.stopped(&key());
    assert!(restarts.restarting(&key()) && restarts.due(&key()));
    restarts.reopened(&key());
    assert!(!restarts.restarting(&key()));

    restarts.stopped(&key());
    assert!(!restarts.due(&key()));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(restarts.due(&key()));
    restarts.reopened(&key());

    // A member that ran steady starts over.
    tokio::time::advance(STEADY).await;
    restarts.stopped(&key());
    assert!(restarts.due(&key()));
    restarts.reopened(&key());
    tokio::time::advance(STEADY).await;
    restarts.retain(|_| true);
    assert!(!restarts.restarting(&key()) && restarts.0.is_empty());
}

#[test]
fn the_wait_is_bounded() {
    assert_eq!(wait_after(1), Duration::ZERO);
    assert_eq!(wait_after(2), Duration::from_secs(1));
    assert_eq!(wait_after(40), MAX_WAIT);
}
