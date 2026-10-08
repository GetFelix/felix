use std::time::Duration;

use super::*;

/// The error a caller sees, and what it is allowed to do next.
mod errors {
    use super::*;

    /// **Only a request that was never sent may be retried.** `Disconnected`
    /// and `Timeout` mean the peer may have applied the write before the answer
    /// was lost, so an automatic retry is a duplicate publish, not a repair.
    #[test]
    fn only_a_request_that_was_never_sent_is_retryable() {
        assert!(
            PeerError::Unavailable {
                node_id: "broker-b".to_string(),
                detail: "in backoff".to_string(),
            }
            .is_retryable()
        );

        for error in [
            PeerError::Disconnected {
                node_id: "broker-b".to_string(),
            },
            PeerError::Timeout {
                node_id: "broker-b".to_string(),
                timeout: Duration::from_secs(5),
            },
            PeerError::Handshake {
                node_id: "broker-b".to_string(),
                detail: "refused".to_string(),
            },
            PeerError::ShuttingDown,
        ] {
            assert!(!error.is_retryable(), "{error} should not be retryable");
        }
    }

    /// The labels separate the failures an operator responds to differently:
    /// nothing was sent, the handshake was refused, the connection dropped
    /// mid-request, or no answer came. `ShuttingDown` shares "nothing was sent"
    /// with `Unavailable` on purpose — to the caller they are the same answer,
    /// and a shutdown is already visible elsewhere.
    #[test]
    fn the_labels_separate_the_failures_an_operator_acts_on() {
        let nothing_sent = PeerError::Unavailable {
            node_id: "b".to_string(),
            detail: String::new(),
        }
        .outcome();
        assert_eq!(PeerError::ShuttingDown.outcome(), nothing_sent);

        let distinct = [
            nothing_sent,
            PeerError::Handshake {
                node_id: "b".to_string(),
                detail: String::new(),
            }
            .outcome(),
            PeerError::Disconnected {
                node_id: "b".to_string(),
            }
            .outcome(),
            PeerError::Timeout {
                node_id: "b".to_string(),
                timeout: Duration::ZERO,
            }
            .outcome(),
        ];
        let mut unique = distinct.to_vec();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            distinct.len(),
            "two failures an operator acts on differently share a label: {distinct:?}"
        );
    }

    /// The peer's identity has to survive into the message; an operator reading
    /// one of these needs to know which broker it is about.
    #[test]
    fn the_peer_is_named_in_the_message() {
        let error = PeerError::Timeout {
            node_id: "broker-b".to_string(),
            timeout: Duration::from_secs(5),
        };
        assert!(error.to_string().contains("broker-b"), "{error}");
    }
}

/// **A cache shard's counter log is fenced and read only on a peer that said
/// it answers for it.** A build with the fence but not the counter fence
/// refuses `Counters` as malformed; the promotion must see that as a peer
/// lacking the capability, before anything is sent.
#[test]
fn a_counter_fence_needs_the_cache_fence() {
    use felix_wire::internal::{Fence, ReplicateFetch, ShardRef};
    let shard = ShardRef {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "sessions".to_string(),
        shard: 0,
        generation: 5,
    };
    let fence = |log| {
        InternalMessage::Fence(Fence {
            correlation_id: 0,
            shard: shard.clone(),
            log,
        })
    };
    let fetch = |log| {
        InternalMessage::ReplicateFetch(ReplicateFetch {
            timed: false,
            correlation_id: 0,
            shard: shard.clone(),
            log,
            from_offset: 0,
            max_bytes: 1024,
            labelled: false,
        })
    };
    assert_eq!(
        required_capability(&fence(ReplicaLog::Cache)),
        Some(PeerCapabilities::FENCE)
    );
    assert_eq!(
        required_capability(&fence(ReplicaLog::Counters)),
        Some(PeerCapabilities::FENCE.union(PeerCapabilities::CACHE_FENCE))
    );
    assert_eq!(
        required_capability(&fetch(ReplicaLog::Cache)),
        Some(PeerCapabilities::TAIL_FETCH)
    );
    assert_eq!(
        required_capability(&fetch(ReplicaLog::Counters)),
        Some(PeerCapabilities::TAIL_FETCH.union(PeerCapabilities::CACHE_FENCE))
    );
}
