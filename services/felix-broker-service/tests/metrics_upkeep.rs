//! The metrics recorder must not hold every histogram sample until a scrape.
//!
//! Its own test binary: it counts live heap bytes through a global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use felix_broker_service::observability::spawn_metrics_upkeep_every;
use metrics_exporter_prometheus::PrometheusBuilder;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards to `System` unchanged; the counter only observes sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn upkeep_folds_histogram_samples_without_a_scrape() {
    const SAMPLES: u32 = 2_000_000;
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("soak_depth"));

    let before = LIVE.load(Ordering::Relaxed);
    for sample in 0..SAMPLES {
        // Queue depths repeat, so the folded summary stays small.
        histogram.record(f64::from(sample % 64));
    }
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(before);
    // Eight bytes per sample is the floor for holding them raw.
    assert!(held > 8_000_000, "samples were not held raw: {held} bytes");

    spawn_metrics_upkeep_every(handle, Duration::from_millis(20));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = LIVE.load(Ordering::Relaxed).saturating_sub(before);
        if now < held / 4 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "upkeep did not fold the samples: {now} of {held} bytes still held"
        );
        // Recording keeps the epoch collector advancing, which is what frees
        // the drained blocks.
        for sample in 0..1_000 {
            histogram.record(f64::from(sample % 64));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
