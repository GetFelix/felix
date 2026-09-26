use std::io::Write;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use super::*;

fn temp_file(dir: &std::path::Path, name: &str) -> Arc<File> {
    let mut file = File::options()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(dir.join(name))
        .expect("open");
    file.write_all(b"record").expect("write");
    Arc::new(file)
}

/// A flush must reach the kernel while another operation is still in the
/// ring. One log's slow device sync must not hold back every other log.
#[tokio::test]
async fn a_new_flush_is_not_held_behind_an_outstanding_one() {
    if ring().is_none() {
        eprintln!("io_uring unavailable; skipping");
        return;
    }
    // A poll on an empty pipe stands in for a slow fsync: it stays in the
    // ring until the test writes to the pipe.
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(File::from(OwnedFd::from(reader)));
    let held = tokio::spawn(poll_readable(gate));
    // Let the gate reach the ring and the service thread go back to waiting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dir = tempdir().expect("dir");
    let file = temp_file(dir.path(), "log");
    let flushed = tokio::time::timeout(Duration::from_secs(2), fsync(file)).await;

    writer.write_all(b"x").expect("release gate");
    let released = held.await.expect("join");
    assert!(matches!(released, Some(Ok(()))), "gate: {released:?}");
    let flushed = flushed.expect("flush waited behind an unrelated outstanding operation");
    assert!(matches!(flushed, Some(Ok(()))), "flush: {flushed:?}");
}

/// More operations than the ring has entries, all held in flight at once.
/// The overflow has to wait for room, not come back as an I/O error: a failed
/// flush poisons its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_requests_than_ring_entries_wait_rather_than_fail() {
    if ring().is_none() {
        eprintln!("io_uring unavailable; skipping");
        return;
    }
    let (reader, mut writer) = std::io::pipe().expect("pipe");
    let gate = Arc::new(File::from(OwnedFd::from(reader)));
    let requests = RING_ENTRIES as usize * 4;
    let held: Vec<_> = (0..requests)
        .map(|_| tokio::spawn(poll_readable(Arc::clone(&gate))))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        held.iter().all(|task| !task.is_finished()),
        "an operation finished before the gate opened",
    );

    writer.write_all(b"x").expect("release gate");
    for task in held {
        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("an operation never completed")
            .expect("join");
        assert!(matches!(outcome, Some(Ok(()))), "{outcome:?}");
    }
}

/// N logs flushing concurrently through the ring. Prints the per-flush
/// latency so a change to the service loop can be compared before and after:
/// `cargo test -p felix-storage --lib uring_fsync::tests::concurrent_flush_latency -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement, not a check"]
async fn concurrent_flush_latency() {
    if ring().is_none() {
        eprintln!("io_uring unavailable; skipping");
        return;
    }
    let dir = tempdir().expect("dir");
    for logs in [1usize, 4, 12] {
        let mut tasks = Vec::new();
        for i in 0..logs {
            let file = temp_file(dir.path(), &format!("log-{logs}-{i}"));
            tasks.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(500);
                for _ in 0..500 {
                    (&*file).write_all(&[0u8; 4096]).expect("write");
                    let start = Instant::now();
                    let outcome = fsync(Arc::clone(&file)).await;
                    assert!(matches!(outcome, Some(Ok(()))), "{outcome:?}");
                    samples.push(start.elapsed());
                }
                samples
            }));
        }
        let mut all = Vec::new();
        for task in tasks {
            all.extend(task.await.expect("join"));
        }
        all.sort();
        let mean = all.iter().sum::<Duration>() / all.len() as u32;
        let p = |q: f64| all[((all.len() - 1) as f64 * q) as usize];
        eprintln!(
            "logs={logs:>2} flushes={} mean={mean:?} p50={:?} p99={:?}",
            all.len(),
            p(0.50),
            p(0.99)
        );
    }
}
