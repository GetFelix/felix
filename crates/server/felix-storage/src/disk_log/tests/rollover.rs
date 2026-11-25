use super::*;

#[tokio::test]
async fn background_rollover_keeps_the_log_contiguous_and_reopenable() {
    // `rollover_threshold_percent` is 100 by default -- the background roll is
    // measured to hurt tail latency on a device-level flush, so it ships off.
    // This exercises it explicitly so the path stays covered.
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        rollover_threshold_percent: 60,
        max_overshoot_percent: 200,
        ..config(FsyncMode::OnCommit)
    };
    {
        let log = DiskLog::open(dir.path(), "t/ns/s/0", config.clone()).expect("open");
        for i in 0..60 {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append");
        }
        assert!(log.segments().len() > 1, "expected rollovers");
        log.shutdown().await.expect("shutdown");
    }

    // Reopening is the real assertion: a background roll that left an
    // uninstalled segment, a gap, or a duplicated offset shows up here.
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config).expect("reopen");
    assert_eq!(log.tail_offset().await.expect("tail"), 60);
    let values = read_all(&log, 0).await;
    assert_eq!(values.len(), 60);
    assert_eq!(values[0], "value-000");
    assert_eq!(values[59], "value-059");
}

/// A rollover whose seal fails must never let an `OnCommit` append be
/// acknowledged on the strength of a flush that did not cover it.
///
/// The window this guards: the seal fails, `pending_seal` is cleared, and an
/// append already in flight captures only the new active segment, syncs that,
/// and reports a durable bound spanning both. The retired segment's records
/// were never flushed.
#[tokio::test]
async fn a_failed_seal_stops_the_log_instead_of_acknowledging() {
    let dir = tempdir().expect("dir");
    let config = LogConfig {
        rollover_threshold_percent: 60,
        max_overshoot_percent: 200,
        ..config(FsyncMode::OnCommit)
    };
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config).expect("open");

    log.inner
        .fail_seal
        .store(true, std::sync::atomic::Ordering::Release);

    // Append until the background rollover has run and failed. Every append
    // either succeeds durably or fails -- what must never happen is an
    // acknowledgement after the seal failed.
    let mut rejected = None;
    for i in 0..200 {
        if let Err(err) = log.append(&records(&[&format!("value-{i:03}")])).await {
            rejected = Some(err);
            break;
        }
    }
    let rejected = rejected.expect("the failed seal should have stopped the log");
    assert!(
        rejected.to_string().contains("rollover failed"),
        "unexpected error: {rejected}",
    );

    // Terminal, on every path that accepts work or reports durability.
    assert!(log.append(&records(&["after"])).await.is_err());
    assert!(log.sync().await.is_err());
    assert!(log.shutdown().await.is_err());

    // And the retired segment is still owed a flush, so no later flush can
    // silently skip it.
    assert!(
        log.inner.pending_seal.lock().is_some(),
        "a failed seal must keep the retired segment on the flush path",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_across_a_rollover_stay_contiguous() {
    let dir = tempdir().expect("dir");
    // Segments small enough that this run crosses many boundaries, so the
    // off-thread pre-roll and the in-lock fallback both get exercised, and
    // concurrent publishers race the roll itself.
    let log = std::sync::Arc::new(
        DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 200,
                ..config(FsyncMode::None)
            },
        )
        .expect("open"),
    );

    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let log = std::sync::Arc::clone(&log);
        tasks.push(tokio::spawn(async move {
            log.append(&records(&[&format!("value-{i:03}")]))
                .await
                .expect("append")
        }));
    }
    let mut assigned: Vec<Offset> = Vec::new();
    for task in tasks {
        assigned.push(task.await.expect("join").first_offset);
    }
    log.sync().await.expect("sync");

    // Every append got a distinct offset and the log holds exactly them, with
    // no gap or duplicate introduced by a rollover racing an append.
    assigned.sort_unstable();
    assert_eq!(assigned, (0..64).collect::<Vec<_>>());
    assert!(log.segments().len() > 2, "expected several rollovers");

    let stored = read_all(&log, 0).await;
    assert_eq!(stored.len(), 64);
    let mut unique = stored.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        64,
        "duplicate or lost records across a rollover"
    );

    // And it reopens cleanly, which is where a segment left empty or
    // double-rolled would surface.
    log.shutdown().await.expect("shutdown");
    drop(log);
    let reopened = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 200,
            ..config(FsyncMode::None)
        },
    )
    .expect("reopen");
    assert_eq!(reopened.tail_offset().await.expect("tail"), 64);
}

/// An inline rollover must not park every Tokio worker in the runtime.
///
/// The publisher that rolls is on a blocking thread; the ones behind it queue
/// on `segments`, which is synchronous. Once as many are queued as there are
/// workers, nothing else in the runtime can run until the rollover finishes.
/// The longest stall an unrelated task sees is what measures that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_rollover_does_not_park_every_worker() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;

    const ROLL_MILLIS: u64 = 500;
    const WORKERS: usize = 2;

    let dir = tempdir().expect("dir");
    // The 100 default for `rollover_threshold_percent` keeps the background
    // rollover out of this; the inline hard-limit path is under test.
    let log = Arc::new(open(&dir, FsyncMode::None));

    // Fill the segment so the next appends are the ones that must roll.
    for i in 0..8 {
        log.append(&records(&[&format!("fill-{i}")]))
            .await
            .expect("fill");
    }

    log.inner
        .slow_inline_roll_millis
        .store(ROLL_MILLIS, Ordering::Release);

    let stop = Arc::new(AtomicBool::new(false));
    let max_stall_micros = Arc::new(AtomicU64::new(0));

    // Touches nothing the log owns; it only needs a worker to run on.
    let ticker = tokio::spawn({
        let stop = Arc::clone(&stop);
        let max_stall_micros = Arc::clone(&max_stall_micros);
        async move {
            let mut last = Instant::now();
            while !stop.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
                let now = Instant::now();
                max_stall_micros.fetch_max((now - last).as_micros() as u64, Ordering::Release);
                last = now;
            }
        }
    });

    // Several times the worker count: one publisher is inside the rollover on
    // a blocking thread, and it takes only two of the rest arriving together to
    // park both workers. Oversubscribing makes that certain rather than likely.
    let publishers: Vec<_> = (0..WORKERS * 4)
        .map(|p| {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                for i in 0..4 {
                    log.append(&records(&[&format!("p{p}-{i}")]))
                        .await
                        .expect("append");
                }
            })
        })
        .collect();

    for publisher in publishers {
        publisher.await.expect("publisher");
    }
    stop.store(true, Ordering::Release);
    ticker.await.expect("ticker");

    // Half of one rollover. The bug parks every worker for the *whole* rollover,
    // so the signal is ~500ms and the noise is a yielding task losing its slot
    // on a busy box. A fifth was chosen as "far above the scheduling noise even
    // on a loaded box"; CI has since falsified that twice, measuring 137ms on a
    // shared runner with nothing wrong. Half keeps a 2x margin on both sides
    // rather than sitting next to the noise floor.
    let stall = Duration::from_micros(max_stall_micros.load(Ordering::Acquire));
    assert!(
        stall < Duration::from_millis(ROLL_MILLIS / 2),
        "an unrelated task stalled for {stall:?} during a {ROLL_MILLIS}ms rollover: \
         appends parked their workers on the synchronous segment lock",
    );
}

#[tokio::test]
async fn sealing_reports_a_descriptor_and_checksum() {
    let dir = tempdir().expect("dir");
    let log = open(&dir, FsyncMode::None);
    log.append(&records(&["a", "b"])).await.expect("append");

    let sealed = log.seal().await.expect("seal");
    assert_eq!(sealed.descriptor.base_offset, 0);
    assert_eq!(sealed.descriptor.last_offset, 1);
    assert_ne!(sealed.checksum, 0);

    // Sealing rolls, so new appends land in a fresh segment and the sealed one
    // stays immutable.
    log.append(&records(&["c"])).await.expect("append");
    assert!(log.segments().len() > 1);
    assert_eq!(read_all(&log, 0).await, vec!["a", "b", "c"]);
}
