//! How many shards each stream has, so a keyed publish can name its shard.
//!
//! Every keyed publish through a client gets its writer from the width the
//! client keeps here, whoever made it: a plain publisher, a `ClusterClient`
//! or an idempotent producer. A caller's own idea of the shard never picks
//! the writer, so two callers that disagree about a width cannot put one key
//! on two writers.
//!
//! The width is asked of the broker once per stream and kept, failures
//! included: an answer that changed while publishes were in flight would
//! move keys to other writers, and they could reach the broker out of order.
//! A stream whose width could not be learned is kept as one shard, which
//! puts all its keyed publishes on one writer. A stream the broker reports
//! gone (`forget`) is asked again: it may come back with another width, and
//! nothing in flight to it can land meanwhile.
//!
//! A broker that advertises `FEATURE_PUBLISH_WIDTH` is told the width each
//! keyed batch was routed by (`publish_width`), and refuses a batch routed by
//! one the stream no longer has. Nothing sent by a stale width lands there,
//! so a changed answer is safe to take. Against such a broker a learned width
//! is asked again every [`REVALIDATE_AFTER`] while the stream is published
//! to, and dropped when a publish is refused with `stale_width`. That is how
//! a client catches a stream deleted and created again at another width
//! without being told.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use ahash::RandomState;
use anyhow::Result;
use bytes::Bytes;
use felix_wire::Message;
use felix_wire::routing::ShardRouting;
use hashbrown::HashMap;
use tokio::sync::OnceCell;
use tokio::time::Instant;

use super::routing::{StreamKey, StreamKeyRef};

/// How long a learned width is used before it is asked again, against a
/// broker that checks declared widths. Bounds how long fire-and-forget keyed
/// publishes to a stream created again at another width keep being dropped.
pub(crate) const REVALIDATE_AFTER: Duration = Duration::from_secs(5);

/// Asks the broker for a stream's width and mapping
/// ([`crate::Client::stream_routing`]).
pub(crate) type LearnWidth = Arc<
    dyn Fn(
            String,
            String,
            String,
        ) -> Pin<Box<dyn Future<Output = Result<(u32, ShardRouting)>> + Send>>
        + Send
        + Sync,
>;

/// The width a stream's keyed publishes are routed by.
#[derive(Debug)]
pub(crate) struct Width {
    pub(crate) shards: u32,
    pub(crate) routing: ShardRouting,
    /// The framed `publish_width` for it, when the broker takes one and this
    /// is the broker's answer rather than the one-shard fallback. The
    /// fallback keeps the stream on one writer, which needs no check.
    pub(crate) declaration: Option<Bytes>,
}

impl Width {
    fn fallback() -> Self {
        Self {
            shards: 1,
            routing: ShardRouting::default(),
            declaration: None,
        }
    }

    fn learned(key: StreamKeyRef<'_>, shards: u32, routing: ShardRouting, declare: bool) -> Self {
        let declaration = declare
            .then(|| {
                Message::PublishWidth {
                    tenant_id: key.tenant_id.to_string(),
                    namespace: key.namespace.to_string(),
                    stream: key.stream.to_string(),
                    shards,
                    routing: (!routing.is_modulo()).then_some(routing),
                }
                .encode()
                .ok()
            })
            .flatten()
            .map(|frame| frame.encode());
        Self {
            shards,
            routing,
            declaration,
        }
    }
}

/// One stream's width, set once, and when it is next asked again.
#[derive(Default)]
struct Entry {
    width: OnceCell<Arc<Width>>,
    /// Milliseconds since `StreamWidths::origin`. `u64::MAX` while a check is
    /// running, so only one runs.
    due: AtomicU64,
}

/// The widths of the streams one client has made keyed publishes to.
///
/// Not bounded: an entry is a stream name, and evicting one would let it be
/// learned again with a different answer.
pub(crate) struct StreamWidths {
    learn: LearnWidth,
    known: Arc<RwLock<HashMap<StreamKey, Arc<Entry>, RandomState>>>,
    /// The broker advertised `FEATURE_PUBLISH_WIDTH`.
    declare: bool,
    /// [`REVALIDATE_AFTER`], but for tests.
    check_after: Duration,
    origin: Instant,
}

impl StreamWidths {
    pub(crate) fn new(learn: LearnWidth, declare: bool) -> Self {
        Self {
            learn,
            known: Arc::new(RwLock::new(HashMap::with_hasher(RandomState::new()))),
            declare,
            check_after: REVALIDATE_AFTER,
            origin: Instant::now(),
        }
    }

    #[cfg(test)]
    pub(crate) fn checking_every(mut self, every: Duration) -> Self {
        self.check_after = every;
        self
    }

    /// The shard the broker routes `key` to on this stream, and the width
    /// that says so.
    pub(crate) async fn shard_of(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: &[u8],
    ) -> (u32, Arc<Width>) {
        let stream_key = StreamKeyRef::new(tenant_id, namespace, stream);
        let entry = self.entry(stream_key);
        // Concurrent first publishes share one question, and whichever answer
        // lands first is the one every later publish uses.
        let width = Arc::clone(
            entry
                .width
                .get_or_init(|| async {
                    let width = self.ask(stream_key).await;
                    entry.due.store(self.due_from_now(), Ordering::Relaxed);
                    Arc::new(width)
                })
                .await,
        );
        if width.declaration.is_some() {
            self.check_again_if_due(&entry, stream_key, &width);
        }
        let shard = felix_wire::routing::shard_for_routing(width.routing, width.shards, Some(key));
        (shard, width)
    }

    /// Drop what is kept for the stream, so its next keyed publish asks again.
    pub(crate) fn forget(&self, tenant_id: &str, namespace: &str, stream: &str) {
        self.known
            .write()
            .expect("stream widths")
            .remove(&StreamKeyRef::new(tenant_id, namespace, stream));
    }

    async fn ask(&self, key: StreamKeyRef<'_>) -> Width {
        match (self.learn)(
            key.tenant_id.to_string(),
            key.namespace.to_string(),
            key.stream.to_string(),
        )
        .await
        {
            Ok((shards, routing)) if shards > 0 => {
                Width::learned(key, shards, routing, self.declare)
            }
            Ok(_) => Width::fallback(),
            Err(err) => {
                tracing::debug!(
                    stream = key.stream,
                    error = %err,
                    "could not learn the stream's width; its keyed publishes share one writer",
                );
                Width::fallback()
            }
        }
    }

    /// Ask the width again in the background once it is due. A different
    /// answer replaces the entry, and the broker refuses whatever is still in
    /// flight under the old one. A failed or empty answer keeps what is held.
    fn check_again_if_due(&self, entry: &Arc<Entry>, key: StreamKeyRef<'_>, current: &Width) {
        let due = entry.due.load(Ordering::Relaxed);
        if millis_since(self.origin) < due
            || entry
                .due
                .compare_exchange(due, u64::MAX, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let learn = Arc::clone(&self.learn);
        let known = Arc::clone(&self.known);
        let entry = Arc::clone(entry);
        let held = (current.shards, current.routing);
        let owned = StreamKey::new(key.tenant_id, key.namespace, key.stream);
        let (tenant_id, namespace, stream) = (
            key.tenant_id.to_string(),
            key.namespace.to_string(),
            key.stream.to_string(),
        );
        let (origin, check_after) = (self.origin, self.check_after);
        tokio::spawn(async move {
            let answer = learn(tenant_id.clone(), namespace.clone(), stream.clone()).await;
            let due = millis_since(origin).saturating_add(check_after.as_millis() as u64);
            match answer {
                Ok((shards, routing)) if shards > 0 && (shards, routing) != held => {
                    let width = Width::learned(
                        StreamKeyRef::new(&tenant_id, &namespace, &stream),
                        shards,
                        routing,
                        true,
                    );
                    let fresh = Arc::new(Entry {
                        width: OnceCell::new_with(Some(Arc::new(width))),
                        due: AtomicU64::new(due),
                    });
                    let mut known = known.write().expect("stream widths");
                    // Unless it was forgotten or replaced meanwhile.
                    if let Some(slot) = known.get_mut(&owned)
                        && Arc::ptr_eq(slot, &entry)
                    {
                        *slot = fresh;
                    }
                }
                _ => entry.due.store(due, Ordering::Relaxed),
            }
        });
    }

    fn entry(&self, key: StreamKeyRef<'_>) -> Arc<Entry> {
        if let Some(entry) = self.known.read().expect("stream widths").get(&key) {
            return Arc::clone(entry);
        }
        let mut known = self.known.write().expect("stream widths");
        Arc::clone(
            known
                .entry(StreamKey::new(key.tenant_id, key.namespace, key.stream))
                .or_default(),
        )
    }

    fn due_from_now(&self) -> u64 {
        millis_since(self.origin).saturating_add(self.check_after.as_millis() as u64)
    }
}

fn millis_since(origin: Instant) -> u64 {
    origin.elapsed().as_millis() as u64
}
