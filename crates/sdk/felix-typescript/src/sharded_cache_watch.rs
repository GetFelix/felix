//! A prefix watch across every shard of a cache, read with `recv`.

use std::collections::HashMap;
use std::sync::Arc;

use felix_client::{ShardedCacheWatch, ShardedCacheWatchItem as Item};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::types::{CacheChange, ShardedCacheWatchItem, or_null};

/// A prefix watch across every shard of a cache. Close it when done.
#[napi]
pub struct ShardedCacheWatchHandle {
    shards: u32,
    retained_count: Option<u64>,
    inner: Arc<Mutex<Option<ShardedCacheWatch>>>,
    closed: watch::Sender<bool>,
}

impl ShardedCacheWatchHandle {
    pub(crate) fn new(watch: ShardedCacheWatch) -> Self {
        Self {
            shards: watch.shards(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
            closed: watch::Sender::new(false),
        }
    }
}

#[napi]
impl ShardedCacheWatchHandle {
    /// How many shards this watch covers.
    #[napi(getter)]
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// How many retained values arrive before live delivery, summed across
    /// shards, on a retained watch. `0` means the prefix held nothing on any
    /// shard; `stateComplete` marks the moment the state is whole. `null` on a
    /// watch that did not ask for retained delivery.
    #[napi(getter)]
    pub fn retained_count(&self) -> Option<BigInt> {
        self.retained_count.map(BigInt::from)
    }

    /// The next item from any shard, or `null` once every shard has ended.
    #[napi]
    pub async fn recv(&self) -> Result<Option<ShardedCacheWatchItem>> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let mut guard = self.inner.lock().await;
        let Some(handle) = guard.as_mut() else {
            return Ok(None);
        };
        let item = tokio::select! {
            item = handle.recv() => item,
            _ = closed.wait_for(|closed| *closed) => {
                guard.take();
                return Ok(None);
            }
        };
        Ok(item.map(shaped))
    }

    /// Where to resume each shard: pass it back as `resume`. Every shard is
    /// listed. A shard still delivering its state resumes at `0n`, so no key is
    /// missed.
    #[napi]
    pub async fn resume_offsets(&self) -> Result<HashMap<String, BigInt>> {
        let guard = self.inner.lock().await;
        let Some(handle) = guard.as_ref() else {
            return Ok(HashMap::new());
        };
        Ok(handle
            .resume_offsets()
            .into_iter()
            .map(|(shard, offset)| (shard.to_string(), BigInt::from(offset)))
            .collect())
    }

    /// Release the watch. Idempotent, and never waits on a read.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.closed.send_replace(true);
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
}

fn shaped(item: Item) -> ShardedCacheWatchItem {
    let empty = |shard: Option<u32>| ShardedCacheWatchItem {
        shard: or_null(shard),
        change: Either::B(Null),
        state_complete: Either::B(Null),
        lagged_resume_from: Either::B(Null),
        shard_moved: Either::B(Null),
        shard_closed: Either::B(Null),
        shard_closed_error: Either::B(Null),
    };
    match item {
        Item::Change { shard, change } => ShardedCacheWatchItem {
            change: Either::A(CacheChange {
                key: change.key,
                value: or_null(change.value.map(|v| v.to_vec().into())),
                offset: BigInt::from(change.offset),
                expires_at_millis: BigInt::from(change.expires_at_millis),
            }),
            ..empty(Some(shard))
        },
        Item::StateComplete => ShardedCacheWatchItem {
            state_complete: Either::A(true),
            ..empty(None)
        },
        Item::Lagged { shard, resume_from } => ShardedCacheWatchItem {
            lagged_resume_from: Either::A(BigInt::from(resume_from)),
            ..empty(Some(shard))
        },
        Item::ShardMoved { shard, moved } => ShardedCacheWatchItem {
            shard_moved: Either::A(moved.into()),
            ..empty(Some(shard))
        },
        Item::ShardClosed { shard, error } => ShardedCacheWatchItem {
            shard_closed: Either::A(true),
            shard_closed_error: or_null(error),
            ..empty(Some(shard))
        },
    }
}
