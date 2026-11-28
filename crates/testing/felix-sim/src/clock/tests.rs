use super::*;

const SEC: Duration = Duration::from_secs(1);

/// What `felix_common::clock` reads now, as (lease, wall) from the shared zero.
fn readings() -> (Duration, Duration) {
    let wall = felix_common::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH + felix_common::clock::sim::EPOCH)
        .unwrap();
    (felix_common::clock::boottime(), wall)
}

#[tokio::test(start_paused = true)]
async fn felix_common_reads_virtual_time_from_the_install() {
    install();
    tokio::time::advance(10 * SEC).await;
    assert_eq!(readings(), (10 * SEC, 10 * SEC));
}
