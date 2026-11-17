//! A debug clock for the stages of one publish, from the frame arriving to
//! its ack being written.
//!
//! Off unless `FELIX_HOP_TRACE=1` is set at startup; then every [`mark`] is
//! one relaxed load. On, the frame handler starts the clock with [`start`]
//! and each stage records the time since then. It keeps one start time for
//! the whole process, so it is exact only with one publish in flight, which
//! is the low-load case it is for. A thread prints each stage's p50 and p99
//! to stderr every five seconds, lines starting with `hop`.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// A point on a publish's way, in the order a single-voter Raft publish
/// passes them. Not every engine passes every one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Stage {
    /// The handler hands the batch to the shard's engine.
    Handed,
    /// The shard's propose task took the batch off its queue.
    Dequeued,
    /// openraft took the write onto its core's queue.
    Proposed,
    /// The log's append began, on openraft's core.
    AppendBegin,
    /// The batch is written to the log (not yet fsynced).
    Written,
    /// The publisher's task saw the batch written.
    WrittenSeen,
    /// openraft applied the batch: it is committed.
    Applied,
    /// The publisher has its answer.
    Answered,
    /// The ack frame was queued for the client.
    AckSent,
}

const STAGES: usize = 9;
const NAMES: [&str; STAGES] = [
    "handed",
    "dequeued",
    "proposed",
    "append_begin",
    "written",
    "written_seen",
    "applied",
    "answered",
    "ack_sent",
];

struct Clock {
    epoch: Instant,
    started: AtomicU64,
    samples: [Mutex<Vec<u32>>; STAGES],
}

fn clock() -> Option<&'static Clock> {
    static CLOCK: OnceLock<Option<Clock>> = OnceLock::new();
    CLOCK
        .get_or_init(|| {
            let on = std::env::var("FELIX_HOP_TRACE").is_ok_and(|v| v == "1");
            on.then(|| {
                std::thread::spawn(report);
                Clock {
                    epoch: Instant::now(),
                    started: AtomicU64::new(0),
                    samples: Default::default(),
                }
            })
        })
        .as_ref()
}

/// Start the clock for a publish just decoded.
pub fn start() {
    if let Some(clock) = clock() {
        let now = clock.epoch.elapsed().as_nanos() as u64;
        clock.started.store(now, Ordering::Relaxed);
    }
}

/// Record that the current publish reached `stage`.
pub fn mark(stage: Stage) {
    if let Some(clock) = clock() {
        let now = clock.epoch.elapsed().as_nanos() as u64;
        let since = now.saturating_sub(clock.started.load(Ordering::Relaxed)) / 1000;
        clock.samples[stage as usize]
            .lock()
            .push(u32::try_from(since).unwrap_or(u32::MAX));
    }
}

fn report() {
    loop {
        std::thread::sleep(Duration::from_secs(5));
        let Some(clock) = clock() else { return };
        for (name, samples) in NAMES.iter().zip(&clock.samples) {
            let mut taken = std::mem::take(&mut *samples.lock());
            if taken.is_empty() {
                continue;
            }
            taken.sort_unstable();
            let at = |q: f64| taken[((taken.len() - 1) as f64 * q) as usize];
            eprintln!(
                "hop {name:<13} n={:<7} p50={:>5}us p99={:>5}us",
                taken.len(),
                at(0.5),
                at(0.99)
            );
        }
    }
}
