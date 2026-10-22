//! When executors are woken, and that no job is ever left waiting for one.

use std::sync::atomic::AtomicBool;

use futures::FutureExt;

use super::*;

fn lane_job(lane: usize, seq: u64) -> PublishJob {
    let mut job = named_job(&format!("lane{lane}"));
    job.payloads = vec![Bytes::copy_from_slice(&seq.to_be_bytes())];
    job
}

fn lane_of(job: &PublishJob) -> usize {
    match &job.target {
        PublishTarget::Named { tenant_id, .. } => tenant_id[4..].parse().expect("lane"),
        _ => unreachable!("tests queue named jobs"),
    }
}

fn seq_of(job: &PublishJob) -> u64 {
    u64::from_be_bytes(job.payloads[0][..].try_into().expect("seq"))
}

fn push(scheduler: &PublishScheduler, job: PublishJob) {
    let queued = scheduler
        .submit("t", job, std::future::ready(()))
        .now_or_never()
        .expect("does not wait");
    assert!(queued.is_ok(), "queue has room");
}

fn wakes(partition: &Partition) -> usize {
    partition.wakes.load(Ordering::Relaxed)
}

/// Park an executor on `partition` and wait until it is counted as idle.
async fn park(
    partition: &Arc<Partition>,
    idle: usize,
) -> tokio::task::JoinHandle<Option<(PublishJob, LaneGuard)>> {
    let waiting = Arc::clone(partition);
    let task = tokio::spawn(async move { waiting.next().await });
    while partition.idle.load(Ordering::SeqCst) < idle {
        tokio::task::yield_now().await;
    }
    task
}

#[tokio::test]
async fn a_push_with_no_executor_waiting_wakes_nobody() {
    let scheduler = PublishScheduler::new(1, 100, 100);
    let partition = &scheduler.partitions()[0];
    for lane in 0..5 {
        push(&scheduler, lane_job(lane, 0));
    }
    assert_eq!(wakes(partition), 0);
}

#[tokio::test]
async fn only_the_push_that_readies_a_lane_wakes_an_executor() {
    let scheduler = PublishScheduler::new(1, 100, 100);
    let partition = Arc::clone(&scheduler.partitions()[0]);
    let executor = park(&partition, 1).await;
    // Ten jobs on one lane, queued before the executor can run: the first
    // readies the lane, the other nine join its queue.
    for seq in 0..10 {
        push(&scheduler, lane_job(0, seq));
    }
    assert_eq!(wakes(&partition), 1);
    let (job, _guard) = executor.await.expect("joined").expect("a job");
    assert_eq!(seq_of(&job), 0);
}

#[tokio::test]
async fn a_busy_lane_is_woken_for_when_it_completes_not_when_pushed() {
    let scheduler = PublishScheduler::new(1, 100, 100);
    let partition = Arc::clone(&scheduler.partitions()[0]);
    push(&scheduler, lane_job(0, 0));
    let (first, guard) = partition.next().await.expect("a job");
    assert_eq!(seq_of(&first), 0);

    let executor = park(&partition, 1).await;
    push(&scheduler, lane_job(0, 1));
    assert_eq!(wakes(&partition), 0, "the lane is running; nothing to take");

    drop(guard);
    assert_eq!(wakes(&partition), 1, "the lane is ready again");
    let (second, guard) = executor.await.expect("joined").expect("a job");
    assert_eq!(seq_of(&second), 1);

    // Finishing a lane with nothing behind it wakes nobody.
    let executor = park(&partition, 1).await;
    drop(guard);
    assert_eq!(wakes(&partition), 1);
    drop(executor);
}

/// A push that lands after an executor found the queue empty but before it
/// parks must wake it. This is the interleaving the idle count exists for.
#[tokio::test]
async fn a_push_between_the_empty_look_and_parking_is_not_lost() {
    let scheduler = PublishScheduler::new(1, 100, 100);
    let partition = Arc::clone(&scheduler.partitions()[0]);
    let looked = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    *partition.look_hook.lock() = Some((Arc::clone(&looked), Arc::clone(&resume)));

    let waiting = Arc::clone(&partition);
    let executor = tokio::spawn(async move { waiting.next().await.map(|(job, _)| seq_of(&job)) });
    looked.notified().await;
    push(&scheduler, lane_job(0, 7));
    resume.notify_one();

    let ran = tokio::time::timeout(Duration::from_secs(5), executor)
        .await
        .expect("the executor was woken for the job pushed before it parked")
        .expect("joined");
    assert_eq!(ran, Some(7));
}

/// A complete that lands in the same window must wake it too.
#[tokio::test]
async fn a_complete_between_the_empty_look_and_parking_is_not_lost() {
    let scheduler = PublishScheduler::new(1, 100, 100);
    let partition = Arc::clone(&scheduler.partitions()[0]);
    push(&scheduler, lane_job(0, 0));
    push(&scheduler, lane_job(0, 1));
    let (_, guard) = partition.next().await.expect("first job");

    let looked = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    *partition.look_hook.lock() = Some((Arc::clone(&looked), Arc::clone(&resume)));
    let waiting = Arc::clone(&partition);
    let executor = tokio::spawn(async move { waiting.next().await.map(|(job, _)| seq_of(&job)) });
    looked.notified().await;
    drop(guard);
    resume.notify_one();

    let ran = tokio::time::timeout(Duration::from_secs(5), executor)
        .await
        .expect("the executor was woken for the lane that became ready")
        .expect("joined");
    assert_eq!(ran, Some(1));
}

/// Producers and executors race with executors parking between jobs, which
/// is where a skipped wake-up would strand a job. Every job must run, each
/// lane one at a time and in the order its producer sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_job_is_stranded_when_wakes_are_skipped() {
    const PRODUCERS: usize = 8;
    const LANES_PER_PRODUCER: usize = 4;
    const JOBS_PER_PRODUCER: u64 = 4_000;
    const EXECUTORS: usize = 4;
    let lanes = PRODUCERS * LANES_PER_PRODUCER;
    let scheduler = Arc::new(PublishScheduler::new(1, 1_000_000, 1_000_000));
    let partition = Arc::clone(&scheduler.partitions()[0]);

    let running: Arc<Vec<AtomicBool>> =
        Arc::new((0..lanes).map(|_| AtomicBool::new(false)).collect());
    let next_seq: Arc<Vec<AtomicUsize>> =
        Arc::new((0..lanes).map(|_| AtomicUsize::new(0)).collect());
    let ran = Arc::new(AtomicUsize::new(0));

    let mut executors = Vec::new();
    for _ in 0..EXECUTORS {
        let (partition, running, next_seq, ran) = (
            Arc::clone(&partition),
            Arc::clone(&running),
            Arc::clone(&next_seq),
            Arc::clone(&ran),
        );
        executors.push(tokio::spawn(async move {
            while let Some((job, guard)) = partition.next().await {
                let lane = lane_of(&job);
                assert!(
                    !running[lane].swap(true, Ordering::SeqCst),
                    "lane {lane} ran two jobs at once"
                );
                let seq = seq_of(&job) as usize;
                assert_eq!(
                    next_seq[lane].fetch_add(1, Ordering::SeqCst),
                    seq,
                    "lane order"
                );
                if seq.is_multiple_of(3) {
                    tokio::task::yield_now().await;
                }
                running[lane].store(false, Ordering::SeqCst);
                drop(guard);
                ran.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    let mut producers = Vec::new();
    for producer in 0..PRODUCERS {
        let scheduler = Arc::clone(&scheduler);
        producers.push(tokio::spawn(async move {
            let mut seqs = [0u64; LANES_PER_PRODUCER];
            for n in 0..JOBS_PER_PRODUCER {
                let slot = (n as usize) % LANES_PER_PRODUCER;
                let lane = producer * LANES_PER_PRODUCER + slot;
                push(&scheduler, lane_job(lane, seqs[slot]));
                seqs[slot] += 1;
                // Thin, uneven load, so executors drain and park.
                if n.is_multiple_of(4) {
                    tokio::task::yield_now().await;
                }
                if n.is_multiple_of(64) {
                    tokio::time::sleep(Duration::from_micros(50)).await;
                }
            }
        }));
    }
    for producer in producers {
        producer.await.expect("producer");
    }

    let total = PRODUCERS * JOBS_PER_PRODUCER as usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while ran.load(Ordering::SeqCst) < total {
        assert!(
            tokio::time::Instant::now() < deadline,
            "stranded: {} of {total} jobs ran",
            ran.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    scheduler.close();
    for executor in executors {
        tokio::time::timeout(Duration::from_secs(10), executor)
            .await
            .expect("executors exit once the queue is closed and empty")
            .expect("executor");
    }
}

#[test]
fn the_depth_gauge_is_set_on_big_moves_and_on_empty() {
    assert!(!depth_gauge_due(0, 1));
    assert!(!depth_gauge_due(0, DEPTH_STEP - 1));
    assert!(depth_gauge_due(0, DEPTH_STEP));
    assert!(!depth_gauge_due(DEPTH_STEP, DEPTH_STEP + 3));
    assert!(
        depth_gauge_due(DEPTH_STEP, 0),
        "an emptied queue reads zero"
    );
    assert!(depth_gauge_due(40, 0));
    assert!(!depth_gauge_due(0, 0));
}

/// Many threads move a depth up and down and report it; once they stop, the
/// gauge must read zero, whatever order the sets landed in.
#[test]
fn the_depth_gauge_reads_zero_once_the_queue_empties() {
    let gauge = Arc::new(DepthGauge::new());
    let depth = Arc::new(AtomicUsize::new(0));
    let shown = Arc::new(AtomicUsize::new(usize::MAX));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (gauge, depth, shown) =
                (Arc::clone(&gauge), Arc::clone(&depth), Arc::clone(&shown));
            std::thread::spawn(move || {
                let report = || {
                    gauge.report(
                        || depth.load(Ordering::Relaxed),
                        |value| shown.store(value, Ordering::Relaxed),
                    )
                };
                for _ in 0..5_000 {
                    for _ in 0..8 {
                        depth.fetch_add(5, Ordering::Relaxed);
                        report();
                    }
                    for _ in 0..8 {
                        depth.fetch_sub(5, Ordering::Relaxed);
                        report();
                    }
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("thread");
    }
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(shown.load(Ordering::Relaxed), 0);
}
