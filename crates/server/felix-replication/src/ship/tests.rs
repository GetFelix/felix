//! Which batches carry the generations that wrote them.

use super::*;

/// **A cache shard's batches are labelled only for a follower that fences
/// caches.** It is the cache fence that compares their labels; a follower
/// with labels but not the cache fence gets cache batches as it always has.
#[test]
fn cache_and_counter_batches_are_labelled_for_a_follower_with_the_cache_fence() {
    use felix_broker::LogKind;
    let labels = PeerCapabilities::GENERATION_LABELS;
    let cache_fence = labels.union(PeerCapabilities::CACHE_FENCE);
    let labelled = |log_kind, offered: PeerCapabilities| {
        labels_needed(log_kind).is_some_and(|needed| offered.contains(needed))
    };

    assert!(labelled(LogKind::Stream, labels));
    for log_kind in [LogKind::Cache, LogKind::Counters] {
        assert!(!labelled(log_kind, labels), "{log_kind:?}");
        assert!(labelled(log_kind, cache_fence), "{log_kind:?}");
    }
    for log_kind in [LogKind::GroupCursors, LogKind::GroupDeadLetters] {
        assert!(!labelled(log_kind, cache_fence), "{log_kind:?}");
    }
}
