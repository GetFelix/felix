//! A cache watch, read with `recv`.

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::types::{CacheChange, CacheWatchItem, or_null};

/// A live cache watch. Close it when done.
#[napi]
pub struct CacheWatchHandle {
    resume_offset: u64,
    resnapshot: bool,
    retained_count: Option<u64>,
    inner: Arc<Mutex<Option<felix_client::ClusterCacheWatch>>>,
    closed: watch::Sender<bool>,
}

impl CacheWatchHandle {
    pub(crate) fn new(watch: felix_client::ClusterCacheWatch) -> Self {
        Self {
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            inner: Arc::new(Mutex::new(Some(watch))),
            closed: watch::Sender::new(false),
        }
    }
}

#[napi]
impl CacheWatchHandle {
    /// The offset live delivery began at. Everything the broker sent before it
    /// — replay, or a retained snapshot — was already reflected there.
    #[napi(getter)]
    pub fn resume_offset(&self) -> BigInt {
        BigInt::from(self.resume_offset)
    }

    /// True when the requested start predated what compaction kept, so the
    /// watch began from each key's current value instead of replaying history.
    #[napi(getter)]
    pub fn resnapshot(&self) -> bool {
        self.resnapshot
    }

    /// How many retained values arrive before live delivery on a retained
    /// watch, so an application knows the exact moment its state is complete.
    ///
    /// `0` is a definite answer — the key or prefix held nothing at join — not
    /// a silence to wait through. `null` on a watch that did not ask for
    /// retained delivery.
    #[napi(getter)]
    pub fn retained_count(&self) -> Option<BigInt> {
        self.retained_count.map(BigInt::from)
    }

    /// The next item, or `null` once the watch has ended.
    #[napi]
    pub async fn recv(&self) -> Result<Option<CacheWatchItem>> {
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
        Ok(item.map(|item| match item {
            felix_client::CacheWatchItem::Change(change) => CacheWatchItem {
                change: Either::A(CacheChange {
                    key: change.key,
                    value: or_null(change.value.map(|v| v.to_vec().into())),
                    offset: BigInt::from(change.offset),
                    expires_at_millis: BigInt::from(change.expires_at_millis),
                }),
                lagged_resume_from: Either::B(Null),
                shard_moved: Either::B(Null),
            },
            felix_client::CacheWatchItem::Lagged { resume_from } => CacheWatchItem {
                change: Either::B(Null),
                lagged_resume_from: Either::A(BigInt::from(resume_from)),
                shard_moved: Either::B(Null),
            },
            felix_client::CacheWatchItem::ShardMoved(moved) => CacheWatchItem {
                change: Either::B(Null),
                lagged_resume_from: Either::B(Null),
                shard_moved: Either::A(moved.into()),
            },
        }))
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
