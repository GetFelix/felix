//! A subscription across every shard of a stream.

use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::types::{Event, ShardEvent};

/// A subscription across every shard of a stream.
#[napi]
pub struct ShardedSubscriptionHandle {
    shards: u32,
    inner: Arc<Mutex<Option<felix_client::ShardedSubscription>>>,
    closed: watch::Sender<bool>,
}

impl ShardedSubscriptionHandle {
    pub(crate) fn new(subscription: felix_client::ShardedSubscription) -> Self {
        Self {
            shards: subscription.shards(),
            inner: Arc::new(Mutex::new(Some(subscription))),
            closed: watch::Sender::new(false),
        }
    }
}

#[napi]
impl ShardedSubscriptionHandle {
    /// How many shards this subscription covers.
    #[napi(getter)]
    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// The next item, or `null` once every shard has ended.
    #[napi]
    pub async fn next_event(&self) -> Result<Option<ShardEvent>> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let mut guard = self.inner.lock().await;
        let Some(sub) = guard.as_mut() else {
            return Ok(None);
        };
        loop {
            let item = tokio::select! {
                item = sub.next() => item,
                _ = closed.wait_for(|closed| *closed) => {
                    guard.take();
                    return Ok(None);
                }
            };
            let Some(item) = item else {
                return Ok(None);
            };
            if let Some(event) = shard_event(item) {
                return Ok(Some(event));
            }
        }
    }

    /// The last offset seen from each shard, for resuming.
    ///
    /// Only shards that have delivered something appear. Passed back as
    /// `resume`, each listed shard continues at `offset + 1` and the rest
    /// start wherever `start` says.
    #[napi]
    pub async fn positions(&self) -> Result<HashMap<String, BigInt>> {
        let guard = self.inner.lock().await;
        let Some(sub) = guard.as_ref() else {
            return Ok(HashMap::new());
        };
        Ok(sub
            .positions()
            .into_iter()
            .map(|(shard, offset)| (shard.to_string(), BigInt::from(offset)))
            .collect())
    }

    /// Release the subscription. Idempotent, and never waits on a read.
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

/// The binding's shape for a sharded item, or `None` for one it has no shape
/// for: `ShardEvent` is non-exhaustive, so a kind added later is skipped until
/// it gets one.
fn shard_event(item: felix_client::ShardEvent) -> Option<ShardEvent> {
    Some(match item {
        felix_client::ShardEvent::Record { shard, event } => ShardEvent {
            shard,
            event: Some(Event {
                tenant_id: event.tenant_id.to_string(),
                namespace: event.namespace.to_string(),
                stream: event.stream.to_string(),
                payload: event.payload.to_vec().into(),
                offset: event.offset.map(BigInt::from),
                skipped_before: BigInt::from(event.skipped_before),
            }),
            lost_error: None,
            recovered: None,
            shard_moved: None,
        },
        felix_client::ShardEvent::ShardLost { shard, error } => ShardEvent {
            shard,
            event: None,
            lost_error: Some(error),
            recovered: None,
            shard_moved: None,
        },
        felix_client::ShardEvent::ShardRecovered { shard } => ShardEvent {
            shard,
            event: None,
            lost_error: None,
            recovered: Some(true),
            shard_moved: None,
        },
        felix_client::ShardEvent::ShardMoved { shard, moved } => ShardEvent {
            shard,
            event: None,
            lost_error: None,
            recovered: None,
            shard_moved: Some(moved.into()),
        },
        _ => return None,
    })
}
