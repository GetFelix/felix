use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[tokio::test]
async fn the_result_of_the_work_reaches_the_caller() {
    let flusher = LogThread::new("test-flush");
    flusher.run(|| Ok(())).await.expect("ok");
    let err = flusher
        .run(|| Err::<(), _>(io::Error::other("device gone")))
        .await
        .expect_err("the failure must be reported");
    assert_eq!(err.to_string(), "device gone");
    // A failed flush does not take the thread with it.
    flusher.run(|| Ok(())).await.expect("ok after a failure");
}

#[tokio::test]
async fn work_runs_off_the_calling_thread() {
    let flusher = LogThread::new("test-flush");
    let caller = std::thread::current().id();
    let (tx, rx) = std::sync::mpsc::channel();
    flusher
        .run(move || {
            let _ = tx.send(std::thread::current().id());
            Ok(())
        })
        .await
        .expect("run");
    assert_ne!(rx.recv().expect("thread id"), caller);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_are_served_one_at_a_time() {
    let flusher = Arc::new(LogThread::new("test-flush"));
    let running = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let flusher = Arc::clone(&flusher);
        let running = Arc::clone(&running);
        let done = Arc::clone(&done);
        tasks.push(tokio::spawn(async move {
            flusher
                .run(move || {
                    assert_eq!(
                        running.fetch_add(1, Ordering::SeqCst),
                        0,
                        "overlapping flushes"
                    );
                    std::thread::sleep(Duration::from_micros(50));
                    running.fetch_sub(1, Ordering::SeqCst);
                    done.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        }));
    }
    for task in tasks {
        task.await.expect("join").expect("run");
    }
    assert_eq!(done.load(Ordering::SeqCst), 64);
}

/// A caller that gives up must not stop the work, or a later flush that
/// finds nothing to do would be trusting a sync that never happened.
#[tokio::test]
async fn an_abandoned_call_still_runs_its_work() {
    let flusher = LogThread::new("test-flush");
    let (started_tx, started) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let (ran_tx, ran) = std::sync::mpsc::channel();
    let call = flusher.run(move || {
        let _ = started_tx.send(());
        let _ = held.recv();
        let _ = ran_tx.send(());
        Ok(())
    });
    let abandoned = tokio::time::timeout(Duration::from_millis(20), call).await;
    assert!(abandoned.is_err(), "the call should still be waiting");
    started.recv().expect("the work started");
    release.send(()).expect("release");
    ran.recv_timeout(Duration::from_secs(5))
        .expect("the work finished after its caller left");
    flusher
        .run(|| Ok(()))
        .await
        .expect("the thread still serves");
}

#[tokio::test]
async fn a_panicking_job_is_reported_and_the_thread_keeps_serving() {
    let flusher = LogThread::new("test-flush");
    let err = flusher
        .run(|| -> io::Result<()> { panic!("injected") })
        .await
        .expect_err("a panic is a failed flush");
    assert!(err.to_string().contains("panicked"), "{err}");
    flusher.run(|| Ok(())).await.expect("the next flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_queued_behind_a_panicking_one_still_runs() {
    let flusher = Arc::new(LogThread::new("test-flush"));
    let (started_tx, started) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel::<()>();
    let first = {
        let flusher = Arc::clone(&flusher);
        tokio::spawn(async move {
            flusher
                .run(move || -> io::Result<()> {
                    started_tx.send(()).expect("started");
                    release_rx.recv().expect("release");
                    panic!("injected")
                })
                .await
        })
    };
    started.recv().expect("the first flush started");
    // Queued on the same thread while the first is still running.
    let second = {
        let flusher = Arc::clone(&flusher);
        tokio::spawn(async move { flusher.run(|| Ok(())).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    release.send(()).expect("release");

    assert!(first.await.expect("task").is_err(), "the panic is reported");
    second
        .await
        .expect("task")
        .expect("a healthy flush queued behind a panic is not failed with it");
}

#[test]
fn the_thread_exits_when_the_log_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let flusher = LogThread::new("test-flush");
    let (tx, rx) = std::sync::mpsc::channel();
    runtime
        .block_on(flusher.run(move || {
            // Dropped when the thread's stack unwinds on exit.
            struct Exit(std::sync::mpsc::Sender<()>);
            impl Drop for Exit {
                fn drop(&mut self) {
                    let _ = self.0.send(());
                }
            }
            thread_local!(static EXIT: std::cell::RefCell<Option<Exit>> = const { std::cell::RefCell::new(None) });
            EXIT.with(|exit| *exit.borrow_mut() = Some(Exit(tx)));
            Ok(())
        }))
        .expect("run");
    drop(flusher);
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the flush thread outlived its log");
}

#[tokio::test]
async fn an_idle_thread_exits_and_the_next_flush_starts_another() {
    let flusher = LogThread::with_idle("test-flush", Duration::from_millis(5));
    let first = thread_of(&flusher).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        flusher.queue.lock().is_none(),
        "the idle thread should have exited"
    );
    assert_ne!(thread_of(&flusher).await, first);
}

/// A flush submitted just as the thread decides it is idle must still run.
/// Losing it would fail a durable append for no reason at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_racing_the_idle_exit_is_not_lost() {
    let flusher = LogThread::with_idle("test-flush", Duration::from_micros(200));
    for round in 0..2_000u32 {
        // Land submissions on both sides of the idle deadline.
        std::thread::sleep(Duration::from_micros(150 + u64::from(round % 100)));
        flusher
            .run(|| Ok(()))
            .await
            .unwrap_or_else(|err| panic!("round {round}: {err}"));
    }
}

async fn thread_of(flusher: &LogThread) -> std::thread::ThreadId {
    let (tx, rx) = std::sync::mpsc::channel();
    flusher
        .run(move || {
            let _ = tx.send(std::thread::current().id());
            Ok(())
        })
        .await
        .expect("run");
    rx.recv().expect("thread id")
}

/// The round trip a flush pays before and after the sync itself, against the
/// blocking pool it replaces. The work is a no-op, so this is dispatch cost
/// only. Run with:
/// `cargo test --release -p felix-storage --lib dispatch_overhead -- --ignored --nocapture`
#[test]
#[ignore = "timing measurement, not a correctness check"]
fn dispatch_overhead() {
    const ROUNDS: usize = 20_000;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("runtime");
    for logs in [1usize, 8, 32] {
        for pool in [true, false] {
            let latencies = runtime.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..logs {
                    tasks.push(tokio::spawn(async move {
                        let flusher = LogThread::new("bench-flush");
                        let mut samples = Vec::with_capacity(ROUNDS);
                        for _ in 0..ROUNDS {
                            let started = std::time::Instant::now();
                            if pool {
                                tokio::task::spawn_blocking(|| Ok::<_, io::Error>(()))
                                    .await
                                    .expect("join")
                                    .expect("run");
                            } else {
                                flusher.run(|| Ok(())).await.expect("run");
                            }
                            samples.push(started.elapsed());
                        }
                        samples
                    }));
                }
                let mut all = Vec::new();
                for task in tasks {
                    all.extend(task.await.expect("join"));
                }
                all
            });
            let mut sorted = latencies;
            sorted.sort();
            let mean = sorted.iter().sum::<Duration>() / sorted.len() as u32;
            println!(
                "{:<14} logs={logs:<3} mean={:>8.1}us p50={:>8.1}us p99={:>8.1}us",
                if pool {
                    "spawn_blocking"
                } else {
                    "flush thread"
                },
                mean.as_secs_f64() * 1e6,
                sorted[sorted.len() / 2].as_secs_f64() * 1e6,
                sorted[sorted.len() * 99 / 100].as_secs_f64() * 1e6,
            );
        }
    }
}
