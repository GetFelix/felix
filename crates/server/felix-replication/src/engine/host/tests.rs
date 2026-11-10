use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use felix_common::membership::RefusedGeneration;
use parking_lot::Mutex;

use super::{EngineHost, Host, HybridHost};
use crate::ShardKey;
use crate::driver::WriteFence;
use crate::quorum::ShardServing;

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

/// Answers a distinct value per method and counts the refusals.
#[derive(Default)]
struct Serving {
    refusals: AtomicU64,
}

impl ShardServing for Serving {
    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(7)
    }

    fn lease_valid(&self) -> bool {
        false
    }

    fn record_ack_refusal(&self) {
        self.refusals.fetch_add(1, SeqCst);
    }
}

/// Records which call came in, with its arguments, and answers a distinct
/// value per method so a crossed delegation shows.
#[derive(Default)]
struct Fence {
    calls: Mutex<Vec<(&'static str, u64)>>,
}

impl Fence {
    fn saw(&self, call: &'static str, generation: u64) {
        self.calls.lock().push((call, generation));
    }
}

impl WriteFence for Fence {
    fn quiesced(&self, _key: &ShardKey) -> bool {
        self.saw("quiesced", 0);
        false
    }

    fn serve_without_lease(&self, _key: &ShardKey, generation: u64) {
        self.saw("serve_without_lease", generation);
    }

    fn sessions_without_lease(&self, _key: &ShardKey, generation: u64) {
        self.saw("sessions_without_lease", generation);
    }

    fn passed(&self, _key: &ShardKey, generation: u64) {
        self.saw("passed", generation);
    }

    fn deposed(&self, _key: &ShardKey, generation: u64) {
        self.saw("deposed", generation);
    }

    fn withheld(&self, _key: &ShardKey, generation: u64) -> bool {
        self.saw("withheld", generation);
        true
    }

    fn gave_up_at(&self, _key: &ShardKey) -> Option<u64> {
        Some(11)
    }

    fn refused_at(&self, _key: &ShardKey) -> Option<RefusedGeneration> {
        Some(RefusedGeneration {
            generation: 12,
            promised_to: "n2".to_string(),
            ballot: 0,
        })
    }

    fn probed(&self, _key: &ShardKey, generation: u64, reclaimable: bool) {
        self.saw(
            if reclaimable {
                "probed_clear"
            } else {
                "probed_ahead"
            },
            generation,
        );
    }

    fn opened_since(&self, _key: &ShardKey, generation: u64) -> bool {
        self.saw("opened_since", generation);
        false
    }

    fn promoting(&self, _key: &ShardKey, generation: u64) -> bool {
        self.saw("promoting", generation);
        true
    }
}

fn host() -> (Host<Serving, Fence>, Arc<Serving>, Arc<Fence>) {
    let serving = Arc::new(Serving::default());
    let fence = Arc::new(Fence::default());
    (
        Host::new(Arc::clone(&serving), Arc::clone(&fence)),
        serving,
        fence,
    )
}

#[test]
fn the_serving_state_comes_from_the_serving_side() {
    let (host, _, _) = host();

    assert_eq!(EngineHost::generation(&host, &key()), Some(7));
    assert!(EngineHost::replicated(&host, &key()));
    assert!(!HybridHost::lease_valid(&host));
}

#[test]
fn an_ack_refusal_is_counted_on_the_serving_side_through_either_trait() {
    let (host, serving, fence) = host();

    HybridHost::record_ack_refusal(&host);
    assert_eq!(serving.refusals.load(SeqCst), 1);
    ShardServing::record_ack_refusal(&host);
    assert_eq!(serving.refusals.load(SeqCst), 2);
    assert!(fence.calls.lock().is_empty());
}

#[test]
fn quiesced_and_deposed_go_to_the_fence() {
    let (host, _, fence) = host();

    assert!(!EngineHost::quiesced(&host, &key()));
    EngineHost::deposed(&host, &key(), 5);

    assert_eq!(*fence.calls.lock(), vec![("quiesced", 0), ("deposed", 5)]);
}

#[test]
fn each_hybrid_callback_reaches_its_own_fence_method() {
    let (host, _, fence) = host();
    let key = key();

    HybridHost::serve_without_lease(&host, &key, 1);
    HybridHost::sessions_without_lease(&host, &key, 2);
    HybridHost::passed(&host, &key, 3);
    assert!(HybridHost::withheld(&host, &key, 4));
    HybridHost::probed(&host, &key, 5, true);
    HybridHost::probed(&host, &key, 6, false);
    assert!(!HybridHost::opened_since(&host, &key, 8));
    assert!(HybridHost::promoting(&host, &key, 9));

    assert_eq!(HybridHost::gave_up_at(&host, &key), Some(11));
    assert_eq!(
        HybridHost::refused_at(&host, &key).map(|r| (r.generation, r.promised_to)),
        Some((12, "n2".to_string()))
    );
    assert_eq!(
        *fence.calls.lock(),
        vec![
            ("serve_without_lease", 1),
            ("sessions_without_lease", 2),
            ("passed", 3),
            ("withheld", 4),
            ("probed_clear", 5),
            ("probed_ahead", 6),
            ("opened_since", 8),
            ("promoting", 9),
        ]
    );
}

#[test]
fn the_host_stands_in_for_the_fence_and_the_serving_state() {
    let (host, serving, fence) = host();
    let key = key();

    // The driver and the quorum waits see the same answers through the
    // existing traits.
    assert_eq!(ShardServing::generation(&host, &key), Some(7));
    assert!(ShardServing::replicated(&host, &key));
    assert!(!ShardServing::lease_valid(&host));
    ShardServing::record_ack_refusal(&host);
    assert_eq!(serving.refusals.load(SeqCst), 1);
    assert!(!WriteFence::quiesced(&host, &key));
    WriteFence::deposed(&host, &key, 3);
    WriteFence::passed(&host, &key, 4);
    assert_eq!(WriteFence::gave_up_at(&host, &key), Some(11));
    assert!(WriteFence::withheld(&host, &key, 5));
    WriteFence::serve_without_lease(&host, &key, 6);
    WriteFence::sessions_without_lease(&host, &key, 7);
    WriteFence::probed(&host, &key, 8, true);
    assert!(!WriteFence::opened_since(&host, &key, 9));
    assert!(WriteFence::promoting(&host, &key, 10));
    assert_eq!(
        WriteFence::refused_at(&host, &key).map(|r| r.generation),
        Some(12)
    );
    assert_eq!(
        *fence.calls.lock(),
        vec![
            ("quiesced", 0),
            ("deposed", 3),
            ("passed", 4),
            ("withheld", 5),
            ("serve_without_lease", 6),
            ("sessions_without_lease", 7),
            ("probed_clear", 8),
            ("opened_since", 9),
            ("promoting", 10),
        ]
    );
}
