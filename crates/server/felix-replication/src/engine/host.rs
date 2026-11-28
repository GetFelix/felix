//! What the engine asks of the broker: its write fence.

use std::sync::Arc;

use crate::ShardKey;

/// The write fence the engine opens and closes a shard's writes through.
///
/// The broker's shard fence answers it; a trait so this crate does not depend
/// on the broker service.
pub trait WriteFence: Send + Sync {
    /// Whether `key` is closed with no write still inside. A shard that was
    /// never fenced here has nothing in flight, so it counts as quiet.
    fn quiesced(&self, key: &ShardKey) -> bool;

    /// The engine opened `key` at `generation`: this broker leads it and may
    /// take writes. The default does nothing, so a shard opened this way stays
    /// closed until a fence implements it.
    fn opened(&self, _key: &ShardKey, _generation: u64) {}

    /// `key` has a newer leader than `generation`: stop taking writes.
    fn deposed(&self, _key: &ShardKey, _generation: u64) {}
}

/// What any engine needs from the broker about a shard.
pub(crate) trait EngineHost: Send + Sync {
    /// This broker leads `key` at `generation` and may take writes.
    fn opened(&self, key: &ShardKey, generation: u64);
    /// `key` has a newer leader than `generation`: stop taking writes.
    fn deposed(&self, key: &ShardKey, generation: u64);
}

/// The broker's write fence, as the engine's host.
pub(crate) struct Host<F: ?Sized> {
    fence: Arc<F>,
}

impl<F: ?Sized> Host<F> {
    pub(crate) fn new(fence: Arc<F>) -> Self {
        Self { fence }
    }
}

impl<F: WriteFence + ?Sized> EngineHost for Host<F> {
    fn opened(&self, key: &ShardKey, generation: u64) {
        self.fence.opened(key, generation);
    }

    fn deposed(&self, key: &ShardKey, generation: u64) {
        self.fence.deposed(key, generation);
    }
}

#[cfg(test)]
mod tests;
