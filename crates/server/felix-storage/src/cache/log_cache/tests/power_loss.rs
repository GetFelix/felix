//! Compaction under a simulated power loss, not a process crash.
//!
//! The log runs with `FsyncMode::None`, the mode where compaction's own
//! flushes are the only thing ordering its copies before the deletes. After
//! each step of a pass, crash images are built and opened: every live value
//! made durable before the pass began must read back from each of them.

use super::*;
use crate::compaction::COPY_BATCH;
use crate::io::power_loss::{PowerLoss, Writeback};

const TRIALS_PER_STEP: u64 = 4;

async fn check_images(
    observer: &PowerLoss,
    expected: &[(String, Bytes)],
    seed: &mut u64,
    step: &str,
) {
    for _ in 0..TRIALS_PER_STEP {
        *seed += 1;
        for writeback in [Writeback::AnySubset, Writeback::InOrder] {
            let image = tempfile::tempdir().expect("image dir");
            observer
                .crash(*seed, writeback, image.path())
                .expect("build the crash image");
            let recovered = LogCache::open(image.path(), config()).expect("open the image");
            let when = format!("after a power loss {step} (seed {seed:#x}, {writeback:?})");
            assert_reads(&recovered, expected, &when).await;
        }
    }
}

#[tokio::test]
async fn a_power_loss_anywhere_in_a_pass_keeps_every_live_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("caches");
    std::fs::create_dir_all(&root).expect("root");
    let observer = PowerLoss::install(&root).expect("install the power-loss observer");
    let cache = LogCache::open(&root, config()).expect("open");
    let expected = overwritten(&cache, 3 * COPY_BATCH).await;
    let shard = cache.shard(T, NS, C, 0).expect("shard");
    let log = shard.current_log().await;
    log.sync().await.expect("make the starting state durable");

    let mut seed = 0xc0_4ac7_0000;
    check_images(&observer, &expected, &mut seed, "before the pass").await;

    let cut = log.roll_now().await.expect("roll");
    check_images(&observer, &expected, &mut seed, "after the cut").await;

    let below = shard.live_below(cut).await.expect("live");
    for (n, batch) in below.chunks(COPY_BATCH).enumerate() {
        assert!(shard.copy_forward(&log, batch).await.expect("copy"));
        check_images(
            &observer,
            &expected,
            &mut seed,
            &format!("after copy batch {n}"),
        )
        .await;
    }

    shard.compact().await.expect("finish the pass");
    assert!(log.base_offset() >= cut, "the pass trimmed nothing");
    check_images(&observer, &expected, &mut seed, "after the trim").await;
    cache.shutdown().await.expect("shutdown");
}
