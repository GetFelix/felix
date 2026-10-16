//! Which copies an unclean start may not lead or vouch for.
use super::*;

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: "s".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

fn log(dir: &tempfile::TempDir) -> felix_broker::StreamLog {
    felix_broker::StreamLog::from_log(
        felix_storage::disk_log::DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            felix_storage::log::LogConfig::default(),
        )
        .expect("open"),
    )
}

#[tokio::test]
async fn a_copy_led_before_the_crash_is_one_until_a_newer_leader_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log(&dir);
    let unclean = UncleanStart::new("broker-a");
    assert!(!unclean.led_before(&key(), &log, 1), "nothing accepted yet");

    log.accept_generation(5, Some("broker-a"))
        .await
        .expect("accept");
    assert!(unclean.led_before(&key(), &log, 5));
    assert!(unclean.led_before(&key(), &log, 6), "at any generation");

    log.give_up_generation().await.expect("give up");
    assert!(unclean.led_before(&key(), &log, 6), "given up is still led");

    log.accept_generation(6, Some("broker-b"))
        .await
        .expect("accept");
    assert!(!unclean.led_before(&key(), &log, 7));
}

#[tokio::test]
async fn a_copy_this_process_leads_is_its_own() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log(&dir);
    let unclean = UncleanStart::new("broker-a");
    unclean.lead(&key(), 7);
    log.accept_generation(7, Some("broker-a"))
        .await
        .expect("accept");
    assert!(unclean.leads_at(&key(), 7));
    assert!(!unclean.led_before(&key(), &log, 7));
}

#[tokio::test]
async fn a_copy_followed_from_another_leader_is_not_one_it_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log(&dir);
    let unclean = UncleanStart::new("broker-a");
    log.accept_generation(5, Some("broker-b"))
        .await
        .expect("accept");
    assert!(!unclean.led_before(&key(), &log, 5));
    assert!(!unclean.led_before(&key(), &log, 6));
}

/// Without a ballot only an assignment at the accepted generation says this
/// broker led it: the control plane names one leader per generation.
#[tokio::test]
async fn without_a_ballot_only_the_accepted_generation_was_led() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = log(&dir);
    let unclean = UncleanStart::new("broker-a");
    log.accept_generation(5, None).await.expect("accept");
    assert!(unclean.led_before(&key(), &log, 5));
    assert!(!unclean.led_before(&key(), &log, 6));
}
