//! The check a subscription makes before it reads a shard, without a
//! connection.
//!
//! A subscribe that reaches a broker that has stopped serving the shard (the
//! fence closed, a higher generation seen, the lease lapsed) is refused rather
//! than answered from a log that may have fallen behind. See
//! `quic::handlers::redirect::stopped_serving`.

use crate::serving::quic::handlers::redirect::stopped_serving;
use crate::shards::ShardKey;
use crate::shards::routing::IngressRouter;

/// `Err` with the refusal's text if a subscription to `key` on this broker
/// would be refused now.
pub fn check_readable(ingress: Option<&IngressRouter>, key: &ShardKey) -> Result<(), String> {
    match stopped_serving(ingress, key) {
        Some(refusal) => Err(format!("{refusal:?}")),
        None => Ok(()),
    }
}
