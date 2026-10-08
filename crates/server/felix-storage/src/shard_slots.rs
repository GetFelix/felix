//! One open handle per shard, with a lock per shard rather than one for all.
//!
//! Opening a log recovers it, which can mean scanning a whole active segment.
//! Two opens of the same shard must coalesce — two writers over one directory
//! corrupt it — but an open of one shard must not wait on another's scan. So
//! the map lock is held only to find a shard's slot, and the open runs under
//! that slot's own lock.
//!
//! Closing marks the slot `Closing` until the handle is closed, and only then
//! lets it go. An open that finds it `Closing` fails with
//! [`StorageError::Closed`] instead of opening a second writer over files the
//! close is still flushing.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Result, StorageError};

/// Every shard a store has open, keyed by `K`.
pub(crate) struct ShardSlots<K, V> {
    slots: Arc<Mutex<HashMap<K, SharedSlot<V>>>>,
}

type SharedSlot<V> = Arc<Mutex<Slot<V>>>;

enum Slot<V> {
    Empty,
    Open(V),
    Closing,
}

impl<K, V> ShardSlots<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    pub(crate) fn new() -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The open handle for `key`, or the result of `open`, which runs at most
    /// once however many callers race here. `closed` is the error for a shard
    /// that is closing right now.
    pub(crate) fn get_or_open(
        &self,
        key: &K,
        open: impl FnOnce() -> Result<V>,
        closed: impl FnOnce() -> StorageError,
    ) -> Result<V> {
        let slot = Arc::clone(
            self.slots
                .lock()
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(Slot::Empty))),
        );
        let mut slot = slot.lock();
        match &*slot {
            Slot::Open(value) => Ok(value.clone()),
            Slot::Closing => Err(closed()),
            Slot::Empty => {
                // A failed open leaves the slot empty, so the next call retries.
                let value = open()?;
                *slot = Slot::Open(value.clone());
                Ok(value)
            }
        }
    }

    /// Take `key`'s handle out and close it with `close`; a later open starts
    /// fresh. A no-op for a shard that is not open or already closing.
    ///
    /// The close runs on its own task, so a caller that gives up waiting does
    /// not leave the shard half closed and stuck in `Closing`.
    pub(crate) async fn close<F, Fut>(&self, key: &K, close: F) -> Result<()>
    where
        F: FnOnce(V) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let Some(slot) = self.slots.lock().get(key).cloned() else {
            return Ok(());
        };
        let value = match std::mem::replace(&mut *slot.lock(), Slot::Closing) {
            Slot::Open(value) => Some(value),
            Slot::Empty => None,
            Slot::Closing => return Ok(()),
        };
        let slots = Arc::clone(&self.slots);
        let key = key.clone();
        tokio::spawn(async move {
            let closed = match value {
                Some(value) => close(value).await,
                None => Ok(()),
            };
            let mut slots = slots.lock();
            if slots.get(&key).is_some_and(|held| Arc::ptr_eq(held, &slot)) {
                slots.remove(&key);
            }
            closed
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// The handle for `key` if it is open now. Never opens one, and never
    /// waits: a shard whose open is still under way reads as not open.
    pub(crate) fn get_open(&self, key: &K) -> Option<V> {
        let slot = Arc::clone(self.slots.lock().get(key)?);
        match &*slot.try_lock()? {
            Slot::Open(value) => Some(value.clone()),
            Slot::Empty | Slot::Closing => None,
        }
    }

    /// Every open handle. Waits out opens in progress.
    pub(crate) fn open_values(&self) -> Vec<V> {
        self.open_entries()
            .into_iter()
            .map(|(_, value)| value)
            .collect()
    }

    /// Every open handle with its key. Waits out opens in progress.
    pub(crate) fn open_entries(&self) -> Vec<(K, V)> {
        let slots: Vec<(K, SharedSlot<V>)> = self
            .slots
            .lock()
            .iter()
            .map(|(key, slot)| (key.clone(), Arc::clone(slot)))
            .collect();
        slots
            .into_iter()
            .filter_map(|(key, slot)| match &*slot.lock() {
                Slot::Open(value) => Some((key, value.clone())),
                Slot::Empty | Slot::Closing => None,
            })
            .collect()
    }
}

impl<K, V> std::fmt::Debug for ShardSlots<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardSlots")
            .field("slots", &self.slots.lock().len())
            .finish()
    }
}

#[cfg(test)]
mod tests;
