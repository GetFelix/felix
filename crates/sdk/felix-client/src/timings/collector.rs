//! The timings collector behind the `telemetry` feature: one global,
//! lock-per-series store that sampled operations push into.

mod record;

pub use record::*;

use std::sync::Mutex;
#[cfg(not(test))]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{ClientCacheTimingSamples, ClientTimingSamples};

#[cfg(not(test))]
static COLLECTOR: OnceLock<TimingCollector> = OnceLock::new();
// Tests need to put the collector back to "never enabled", which a `OnceLock`
// cannot do through a shared reference.
#[cfg(test)]
static COLLECTOR: test_slot::Slot = test_slot::Slot::new();

struct TimingCollector {
    publish_enqueue_wait_ns: Mutex<Vec<u64>>,
    encode_ns: Mutex<Vec<u64>>,
    binary_encode_ns: Mutex<Vec<u64>>,
    text_encode_ns: Mutex<Vec<u64>>,
    text_batch_build_ns: Mutex<Vec<u64>>,
    write_ns: Mutex<Vec<u64>>,
    send_await_ns: Mutex<Vec<u64>>,
    sub_read_wait_ns: Mutex<Vec<u64>>,
    sub_read_await_ns: Mutex<Vec<u64>>,
    sub_queue_wait_ns: Mutex<Vec<u64>>,
    sub_decode_ns: Mutex<Vec<u64>>,
    sub_dispatch_ns: Mutex<Vec<u64>>,
    sub_consumer_gap_ns: Mutex<Vec<u64>>,
    sub_poll_gap_ns: Mutex<Vec<u64>>,
    sub_time_in_queue_ns: Mutex<Vec<u64>>,
    sub_runtime_gap_ns: Mutex<Vec<u64>>,
    sub_delivery_chan_wait_ns: Mutex<Vec<u64>>,
    e2e_latency_ns: Mutex<Vec<u64>>,
    ack_read_wait_ns: Mutex<Vec<u64>>,
    ack_decode_ns: Mutex<Vec<u64>>,
    cache_encode_ns: Mutex<Vec<u64>>,
    cache_open_stream_ns: Mutex<Vec<u64>>,
    cache_write_ns: Mutex<Vec<u64>>,
    cache_finish_ns: Mutex<Vec<u64>>,
    cache_read_wait_ns: Mutex<Vec<u64>>,
    cache_read_drain_ns: Mutex<Vec<u64>>,
    cache_decode_ns: Mutex<Vec<u64>>,
    cache_validate_ns: Mutex<Vec<u64>>,
    sample_every: usize,
    enabled: AtomicBool,
    counter: AtomicUsize,
}

/// Start collecting, sampling one operation in every `sample_every`.
///
/// Only the first call takes effect; later ones keep the existing collector.
pub fn enable_collection(sample_every: usize) {
    let sample_every = sample_every.max(1);
    let _ = COLLECTOR.set(TimingCollector {
        publish_enqueue_wait_ns: Mutex::new(Vec::new()),
        encode_ns: Mutex::new(Vec::new()),
        binary_encode_ns: Mutex::new(Vec::new()),
        text_encode_ns: Mutex::new(Vec::new()),
        text_batch_build_ns: Mutex::new(Vec::new()),
        write_ns: Mutex::new(Vec::new()),
        send_await_ns: Mutex::new(Vec::new()),
        sub_read_wait_ns: Mutex::new(Vec::new()),
        sub_read_await_ns: Mutex::new(Vec::new()),
        sub_queue_wait_ns: Mutex::new(Vec::new()),
        sub_decode_ns: Mutex::new(Vec::new()),
        sub_dispatch_ns: Mutex::new(Vec::new()),
        sub_consumer_gap_ns: Mutex::new(Vec::new()),
        sub_poll_gap_ns: Mutex::new(Vec::new()),
        sub_time_in_queue_ns: Mutex::new(Vec::new()),
        sub_runtime_gap_ns: Mutex::new(Vec::new()),
        sub_delivery_chan_wait_ns: Mutex::new(Vec::new()),
        e2e_latency_ns: Mutex::new(Vec::new()),
        ack_read_wait_ns: Mutex::new(Vec::new()),
        ack_decode_ns: Mutex::new(Vec::new()),
        cache_encode_ns: Mutex::new(Vec::new()),
        cache_open_stream_ns: Mutex::new(Vec::new()),
        cache_write_ns: Mutex::new(Vec::new()),
        cache_finish_ns: Mutex::new(Vec::new()),
        cache_read_wait_ns: Mutex::new(Vec::new()),
        cache_read_drain_ns: Mutex::new(Vec::new()),
        cache_decode_ns: Mutex::new(Vec::new()),
        cache_validate_ns: Mutex::new(Vec::new()),
        sample_every,
        enabled: AtomicBool::new(true),
        counter: AtomicUsize::new(0),
    });
}

/// Pause or resume sampling without discarding what was collected.
pub fn set_enabled(enabled: bool) {
    if let Some(collector) = COLLECTOR.get() {
        collector.enabled.store(enabled, Ordering::Relaxed);
    }
}

/// Whether the operation about to run should be timed.
pub fn should_sample() -> bool {
    let Some(collector) = COLLECTOR.get() else {
        return false;
    };
    if !collector.enabled.load(Ordering::Relaxed) {
        return false;
    }
    let idx = collector.counter.fetch_add(1, Ordering::Relaxed);
    idx % collector.sample_every == 0
}

/// Drain the publish and subscribe samples. `None` until collection is
/// enabled.
pub fn take_samples() -> Option<ClientTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut publish_enqueue_wait = collector
        .publish_enqueue_wait_ns
        .lock()
        .expect("publish enqueue wait lock");
    let mut encode = collector.encode_ns.lock().expect("encode lock");
    let mut binary_encode = collector
        .binary_encode_ns
        .lock()
        .expect("binary encode lock");
    let mut text_encode = collector.text_encode_ns.lock().expect("text encode lock");
    let mut text_batch_build = collector
        .text_batch_build_ns
        .lock()
        .expect("text batch build lock");
    let mut write = collector.write_ns.lock().expect("write lock");
    let mut send_await = collector.send_await_ns.lock().expect("send await lock");
    let mut sub_read = collector.sub_read_wait_ns.lock().expect("sub read lock");
    let mut sub_read_await = collector
        .sub_read_await_ns
        .lock()
        .expect("sub read await lock");
    let mut sub_queue_wait = collector
        .sub_queue_wait_ns
        .lock()
        .expect("sub queue wait lock");
    let mut sub_decode = collector.sub_decode_ns.lock().expect("sub decode lock");
    let mut sub_dispatch = collector.sub_dispatch_ns.lock().expect("sub dispatch lock");
    let mut sub_consumer_gap = collector
        .sub_consumer_gap_ns
        .lock()
        .expect("sub consumer gap lock");
    let mut sub_poll_gap = collector.sub_poll_gap_ns.lock().expect("sub poll gap lock");
    let mut sub_time_in_queue = collector
        .sub_time_in_queue_ns
        .lock()
        .expect("sub time in queue lock");
    let mut sub_runtime_gap = collector
        .sub_runtime_gap_ns
        .lock()
        .expect("sub runtime gap lock");
    let mut sub_delivery_chan_wait = collector
        .sub_delivery_chan_wait_ns
        .lock()
        .expect("sub delivery channel wait lock");
    let mut e2e_latency = collector.e2e_latency_ns.lock().expect("e2e latency lock");
    let mut ack_read = collector.ack_read_wait_ns.lock().expect("ack read lock");
    let mut ack_decode = collector.ack_decode_ns.lock().expect("ack decode lock");
    Some((
        std::mem::take(&mut *publish_enqueue_wait),
        std::mem::take(&mut *encode),
        std::mem::take(&mut *binary_encode),
        std::mem::take(&mut *text_encode),
        std::mem::take(&mut *text_batch_build),
        std::mem::take(&mut *write),
        std::mem::take(&mut *send_await),
        std::mem::take(&mut *sub_read),
        std::mem::take(&mut *sub_read_await),
        std::mem::take(&mut *sub_queue_wait),
        std::mem::take(&mut *sub_decode),
        std::mem::take(&mut *sub_dispatch),
        std::mem::take(&mut *sub_consumer_gap),
        std::mem::take(&mut *sub_poll_gap),
        std::mem::take(&mut *sub_time_in_queue),
        std::mem::take(&mut *sub_runtime_gap),
        std::mem::take(&mut *sub_delivery_chan_wait),
        std::mem::take(&mut *e2e_latency),
        std::mem::take(&mut *ack_read),
        std::mem::take(&mut *ack_decode),
    ))
}

/// Drain the cache samples. `None` until collection is enabled.
pub fn take_cache_samples() -> Option<ClientCacheTimingSamples> {
    let collector = COLLECTOR.get()?;
    let mut encode = collector.cache_encode_ns.lock().expect("cache encode lock");
    let mut open_stream = collector
        .cache_open_stream_ns
        .lock()
        .expect("cache open stream lock");
    let mut write = collector.cache_write_ns.lock().expect("cache write lock");
    let mut finish = collector.cache_finish_ns.lock().expect("cache finish lock");
    let mut read_wait = collector
        .cache_read_wait_ns
        .lock()
        .expect("cache read wait lock");
    let mut read_drain = collector
        .cache_read_drain_ns
        .lock()
        .expect("cache read drain lock");
    let mut decode = collector.cache_decode_ns.lock().expect("cache decode lock");
    let mut validate = collector
        .cache_validate_ns
        .lock()
        .expect("cache validate lock");
    Some((
        std::mem::take(&mut *encode),
        std::mem::take(&mut *open_stream),
        std::mem::take(&mut *write),
        std::mem::take(&mut *finish),
        std::mem::take(&mut *read_wait),
        std::mem::take(&mut *read_drain),
        std::mem::take(&mut *decode),
        std::mem::take(&mut *validate),
    ))
}

#[cfg(test)]
pub(crate) fn reset_collector_for_tests() {
    COLLECTOR.clear();
}

/// A `OnceLock` stand-in that tests can clear.
#[cfg(test)]
mod test_slot {
    use std::sync::RwLock;

    use super::TimingCollector;

    // Collectors are leaked rather than dropped on `clear`, so a reference
    // another test took before the clear stays valid.
    pub(super) struct Slot(RwLock<Option<&'static TimingCollector>>);

    impl Slot {
        pub(super) const fn new() -> Self {
            Self(RwLock::new(None))
        }

        pub(super) fn get(&self) -> Option<&'static TimingCollector> {
            *self
                .0
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        pub(super) fn set(&self, collector: TimingCollector) -> Result<(), TimingCollector> {
            let mut slot = self
                .0
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_some() {
                return Err(collector);
            }
            *slot = Some(Box::leak(Box::new(collector)));
            Ok(())
        }

        pub(super) fn clear(&self) {
            *self
                .0
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
    }
}
