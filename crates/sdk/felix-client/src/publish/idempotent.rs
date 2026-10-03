//! A producer whose publishes land once, however many times they are sent.
//!
//! The broker hands out a producer id; the producer numbers its batches on
//! each shard from zero and sends the number with the batch. An unkeyed
//! batch goes to shard 0; a keyed one to the shard its key routes to. The shard's
//! leader appends the number it expects and answers a re-send of one it
//! already holds, so a batch the producer never got an answer for can be
//! sent again without a second copy landing. On a durable stream the numbers
//! are stored in the shard's log, so that holds across a failover or a move
//! too: the producer keeps re-sending through the leader change and the new
//! leader answers from the records it holds. That is the whole
//! contract, and it is what `retry.ambiguous_outcomes_are_not_silently_retried`
//! could not offer: with a sequence the ambiguous outcome is not ambiguous
//! any more.
//!
//! The sequence advances only on an acknowledgement. A publish that fails
//! for any reason but a typed refusal leaves it where it was and holds on to
//! the batch, because the batch may have landed under that number. The next
//! call on the stream must be that same batch, re-sent; a different one is
//! refused rather than sent. A leader that advertises
//! `FEATURE_SEQUENCE_REUSED` would refuse it too (`sequence_reused`), but an
//! older one answers it from memory and reports success without appending
//! it, so the check stays here. A typed refusal ends the
//! producer on that stream, because a gap or a forgotten producer is not
//! something a re-send can mend. A forgotten producer is not started
//! again under a new id here: whether its last batch landed is exactly what
//! the shard can no longer say, so the caller has to decide.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_wire::routing::ShardRouting;
use tokio::sync::Mutex;

use crate::client::Client;
use crate::cluster::{Attempt, ClusterClient, Next, Retrying};
use crate::{PublishRefusalReason, PublishRefused};

/// A producer whose batches are appended once, however many times they are
/// sent. See the module documentation.
///
/// One sequence per shard, so a producer may publish to several streams and,
/// with keys, to several shards of one; calls are serialised, since each
/// sequence has to be. A batch
/// of any size takes one sequence. [`Self::publish_batches`] keeps several
/// batches of one call in flight at once.
pub struct IdempotentProducer<'a> {
    source: Source<'a>,
    producer_id: u64,
    /// Set when a publish future was dropped between sending a batch and
    /// learning what happened to it. See [`Self::publish_batch`].
    ///
    /// Producer-wide rather than per stream, which is exactly as coarse as the
    /// cursor lock already is: publishes on this producer serialise behind that
    /// lock whatever stream they are for.
    in_doubt: AtomicBool,
    /// Per shard, because that is what the leader numbers. An unkeyed batch
    /// is shard 0's.
    cursors: Mutex<HashMap<ShardKey, Cursor>>,
    /// The broker a refusal named as the leader of a shard, kept so the next
    /// batch goes straight there rather than being refused again.
    leaders: Mutex<HashMap<ShardKey, Arc<Client>>>,
    /// Each keyed stream's width and mapping, asked once.
    widths: Mutex<HashMap<StreamKey, (u32, ShardRouting)>>,
}

impl<'a> IdempotentProducer<'a> {
    pub(crate) fn for_client(client: &'a Client, producer_id: u64) -> Self {
        Self::new(Source::Single(client), producer_id)
    }

    pub(crate) fn for_cluster(cluster: &'a ClusterClient, producer_id: u64) -> Self {
        Self::new(Source::Cluster(cluster), producer_id)
    }

    fn new(source: Source<'a>, producer_id: u64) -> Self {
        Self {
            source,
            producer_id,
            in_doubt: AtomicBool::new(false),
            cursors: Mutex::new(HashMap::new()),
            leaders: Mutex::new(HashMap::new()),
            widths: Mutex::new(HashMap::new()),
        }
    }

    /// The id the broker assigned this producer.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    /// Publish one payload, once, returning its log offset when the leader
    /// reported one. See [`Self::publish_batch`].
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
    ) -> Result<Option<u64>> {
        self.publish_batch(tenant_id, namespace, stream, vec![payload])
            .await
    }

    /// Publish a batch, once. The batch takes one sequence whatever its size.
    ///
    /// Returns once the leader has acknowledged it, which under `Quorum`
    /// means a majority holds it. On any error but a typed refusal the
    /// sequence is not advanced and the batch is in doubt: calling again with
    /// the same payloads re-sends it and cannot duplicate it, and calling with
    /// different payloads fails without sending anything, because the leader
    /// may already hold the first batch under that sequence and would
    /// acknowledge the second without appending it. Re-send until it succeeds,
    /// or replace the producer. A [`PublishRefused`] ends this producer on the
    /// stream: every later call fails with the same reason, because the
    /// broker no longer knows where this producer is.
    ///
    /// Returns the log offset of the batch's first record, from the ack that
    /// settled it; a re-send the leader already held reports where the batch
    /// landed the first time. `None` when the leader reported no offset
    /// (it predates `FLAG_BINARY_PUBLISH_ACK_OFFSET`, or the stream has no
    /// log), or when an earlier call had already settled this batch and
    /// nothing was sent.
    ///
    /// **Cancelling this stops the producer.** Dropping the future between
    /// sending a batch and learning what happened to it leaves the sequence in
    /// doubt: the batch may have been appended under it, and the cursor still
    /// points at it. Since the broker answers a remembered sequence from memory
    /// *without appending*, reusing it would discard a different batch and
    /// report success — so the next call refuses instead, and the producer has
    /// to be replaced. Do not race this against a timeout; a producer is cheap
    /// to re-initialise and silently dropped records are not cheap at all.
    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Option<u64>> {
        let offsets = self
            .publish_batches_at(tenant_id, namespace, stream, None, vec![payloads])
            .await?;
        Ok(offsets.first().copied().flatten())
    }

    /// [`Self::publish`] with a routing key, which decides the shard as it
    /// does for [`Publisher::publish_keyed`](crate::Publisher::publish_keyed).
    pub async fn publish_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Bytes,
        payload: Vec<u8>,
    ) -> Result<Option<u64>> {
        self.publish_batch_keyed(tenant_id, namespace, stream, key, vec![payload])
            .await
    }

    /// [`Self::publish_batch`] with a routing key, which decides the shard.
    ///
    /// The leader numbers batches per shard, so this producer keeps a
    /// sequence per shard: keys that share a shard share its sequence, and
    /// the rules of [`Self::publish_batch`] apply to that shard. A batch in
    /// doubt is re-sent only with the same key as well as the same payloads.
    ///
    /// The shard is worked out here from the stream's width and mapping,
    /// asked of the broker once, so this needs one that advertises
    /// `FEATURE_STREAM_SHARDS` and fails without sending anything otherwise.
    /// If the stream's width changed after it was asked, a batch can reach a
    /// shard whose sequence it does not continue; the leader refuses that as
    /// a gap, which ends the producer on that shard rather than misplacing a
    /// record.
    pub async fn publish_batch_keyed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Bytes,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Option<u64>> {
        let offsets = self
            .publish_batches_at(tenant_id, namespace, stream, Some(key), vec![payloads])
            .await?;
        Ok(offsets.first().copied().flatten())
    }

    /// Publish several batches, once each, under consecutive sequences.
    ///
    /// Against a broker that pipelines publishes (`FEATURE_PUBLISH_PIPELINE`)
    /// the batches go out without waiting for each other's answers, up to the
    /// publish window and never more than 64 at once; against any other
    /// broker they go one at a time. Either way the result is the same as
    /// calling [`Self::publish_batch`] for each in turn: every batch is
    /// appended once, in order, or the call fails.
    ///
    /// On failure the batches that were acknowledged stay acknowledged, and
    /// the rest are in doubt together. Making the same call again re-sends
    /// only those; a call may also start with the batches in doubt, in the
    /// same order, and carry more after them. A call that starts with
    /// anything else is refused without sending. The other
    /// rules of [`Self::publish_batch`] apply unchanged, including that
    /// cancelling the call stops the producer.
    pub async fn publish_batches(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        batches: Vec<Vec<Vec<u8>>>,
    ) -> Result<()> {
        self.publish_batches_at(tenant_id, namespace, stream, None, batches)
            .await
            .map(|_| ())
    }

    /// [`Self::publish_batches`], returning the offsets of the batches this
    /// call sent, in order.
    async fn publish_batches_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        routing_key: Option<Bytes>,
        batches: Vec<Vec<Vec<u8>>>,
    ) -> Result<Vec<Option<u64>>> {
        let shard = self
            .shard_for(tenant_id, namespace, stream, routing_key.as_deref())
            .await?;
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            shard,
        );
        if self.in_doubt.load(Ordering::Acquire) {
            anyhow::bail!(
                "a publish on this producer was cancelled before the broker answered, \
                 so its sequence may or may not have been appended. Reusing that \
                 sequence would have the broker answer the new batch from memory \
                 without appending it, and report success — so this producer will not \
                 publish again. Call producer_init for a fresh producer id; the batch \
                 in doubt is the only one whose fate is unknown.",
            );
        }
        // Held for the whole publish: the sequence is only meaningful if the
        // batches carrying consecutive numbers are sent in that order.
        let mut cursors = self.cursors.lock().await;
        let mut batches = batches;
        let (first, doubted) = match cursors.get(&key) {
            None => (0, Vec::new()),
            Some(Cursor::Next(sequence)) => (*sequence, Vec::new()),
            Some(Cursor::InDoubt {
                sequence,
                routing_key: pending_key,
                batches: pending,
                settled,
            }) => {
                // Another key on the same shard is a different record, however
                // alike the payloads, and the leader would answer it from
                // memory without appending it.
                if *pending_key != routing_key {
                    anyhow::bail!(
                        "the last batch on this shard failed without a definite answer, so \
                         sequence {sequence} may already hold it. A batch with another key \
                         under that sequence would be acknowledged without being appended, \
                         so it is not sent: re-send the same batch with the same key, or \
                         call producer_init for a fresh producer.",
                    );
                }
                // The same call again: what it already landed is not sent.
                if !settled.is_empty()
                    && batches.starts_with(settled)
                    && starts_alike(&batches[settled.len()..], pending)
                {
                    batches.drain(..settled.len());
                }
                if !starts_alike(&batches, pending) {
                    anyhow::bail!(
                        "the last batch on this stream failed without a definite answer, so \
                         sequence {sequence} may already hold it. A different batch under that \
                         sequence would be acknowledged without being appended, so it is not \
                         sent: re-send the same batch, or call producer_init for a fresh \
                         producer.",
                    );
                }
                let overlap = pending.len().min(batches.len());
                (*sequence, pending[overlap..].to_vec())
            }
            Some(Cursor::Ended(refused)) => {
                return Err(refused.clone()).context("this producer was ended on the stream");
            }
        };
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        // Armed across the send and disarmed the instant it answers: between
        // those two points the caller's future may be dropped, and that is the
        // window where the cursor and the broker can disagree.
        let cancelled = InDoubtOnCancel::armed(&self.in_doubt);
        let (acked, result) = self.send(&key, routing_key.as_ref(), &batches, first).await;
        cancelled.disarm();
        let settled = first + acked.len() as u64;
        match result {
            Ok(()) if doubted.is_empty() => {
                cursors.insert(key, Cursor::Next(settled));
                Ok(acked)
            }
            // A shorter call than the run in doubt settles its prefix only.
            Ok(()) => {
                cursors.insert(
                    key,
                    Cursor::InDoubt {
                        sequence: settled,
                        routing_key,
                        batches: doubted,
                        settled: Vec::new(),
                    },
                );
                Ok(acked)
            }
            Err(err) => {
                // Anything short of a terminal refusal may have landed: a
                // timeout, a dropped connection, or a not-leader refusal that
                // ended a run of retries whose earlier attempts went unanswered.
                let cursor = match err.downcast_ref::<PublishRefused>() {
                    Some(refused)
                        if !matches!(refused.reason, PublishRefusalReason::NotLeader { .. }) =>
                    {
                        Cursor::Ended(refused.clone())
                    }
                    _ => {
                        let mut unsettled = batches.split_off(acked.len());
                        unsettled.extend(doubted);
                        Cursor::InDoubt {
                            sequence: settled,
                            routing_key,
                            batches: unsettled,
                            settled: batches,
                        }
                    }
                };
                cursors.insert(key, cursor);
                Err(err)
            }
        }
    }

    /// Batches under consecutive sequences from `first`, re-sent from the
    /// first unanswered one until all are answered or the policy runs out.
    /// Returns the offsets of those acknowledged, from the first. Only ever the
    /// same numbers: a re-send is safe *because* the numbers did not move.
    async fn send(
        &self,
        key: &ShardKey,
        routing_key: Option<&Bytes>,
        batches: &[Vec<Vec<u8>>],
        first: u64,
    ) -> (Vec<Option<u64>>, Result<()>) {
        match self.source {
            Source::Single(client) => {
                self.send_via(client, key, routing_key, batches, first)
                    .await
            }
            Source::Cluster(cluster) => {
                let started = std::time::Instant::now();
                let policy = cluster.policy();
                let mut last: Option<anyhow::Error> = None;
                let mut retrying = Retrying::default();
                let mut acked = Vec::new();
                // What the last failure asked for: `None` goes again at once
                // through the entry broker, `Some` backs off at least that long.
                let mut wait: Option<std::time::Duration> = None;
                for attempt in 0..policy.attempts.max(1) {
                    if attempt > 0
                        && let Some(at_least) = wait
                    {
                        let delay = policy.delay_before(attempt - 1).max(at_least);
                        if let Some(budget) = policy.deadline
                            && started.elapsed() + delay >= budget
                        {
                            break;
                        }
                        tokio::time::sleep(delay).await;
                        // The leader this producer remembered may be the
                        // broker that just failed; forget it and let the
                        // next refusal name the new one.
                        self.leaders.lock().await.remove(key);
                        if let Err(err) = cluster.reconnect().await {
                            last = Some(err.context("no broker answered"));
                            continue;
                        }
                    }
                    let client = cluster.client().await;
                    let (settled, result) = self
                        .send_via(
                            &client,
                            key,
                            routing_key,
                            &batches[acked.len()..],
                            first + acked.len() as u64,
                        )
                        .await;
                    acked.extend(settled);
                    let err = match result {
                        Ok(()) => return (acked, Ok(())),
                        Err(err) => err,
                    };
                    // A leader remembered now was either used for this attempt
                    // or learned by following a refusal during it, so the error
                    // came from that leader.
                    let attempt = Attempt {
                        routed: self.leaders.lock().await.contains_key(key),
                        // The sequence is what makes a re-send safe: the leader
                        // answers one it already holds from memory.
                        resend_ambiguous: true,
                        not_found_for: None,
                    };
                    match retrying.next(&err, attempt) {
                        // A typed refusal or a fatal code is the broker's
                        // answer, and no other broker answers it differently.
                        Next::Fail => return (acked, Err(err)),
                        Next::Reroute => {
                            self.leaders.lock().await.remove(key);
                            wait = None;
                        }
                        Next::Backoff { at_least } => wait = Some(at_least),
                    }
                    last = Some(err);
                }
                let sequence = first + acked.len() as u64;
                (
                    acked,
                    Err(last
                        .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
                        .context(format!(
                            "gave up after {:?} and at most {} attempts; sequence {sequence} \
                             was not advanced; re-send the same batch under it",
                            started.elapsed(),
                            policy.attempts.max(1),
                        ))),
                )
            }
        }
    }

    /// Send to the shard's leader if one is remembered, else to `client`,
    /// following one not-leader refusal to the broker it names.
    async fn send_via(
        &self,
        client: &Client,
        key: &ShardKey,
        routing_key: Option<&Bytes>,
        batches: &[Vec<Vec<u8>>],
        first: u64,
    ) -> (Vec<Option<u64>>, Result<()>) {
        let remembered = self.leaders.lock().await.get(key).cloned();
        let target = remembered.as_deref().unwrap_or(client);
        let (acked, first_result) = self
            .publish_on(target, key, routing_key, batches, first)
            .await;
        let err = match first_result {
            Ok(()) => return (acked, Ok(())),
            Err(err) => err,
        };
        let (node_id, addr) = match err.downcast_ref::<PublishRefused>() {
            Some(PublishRefused {
                reason: PublishRefusalReason::NotLeader { node_id, addr },
                ..
            }) => (node_id.clone(), addr.clone()),
            _ => return (acked, Err(err)),
        };
        let rest = &batches[acked.len()..];
        let next = first + acked.len() as u64;
        // Only the leader holds the sequences, so the batch goes to it
        // rather than through a forward. One hop: a correct cluster needs
        // one, and a second refusal means the answer is moving.
        let Some(addr) = addr else {
            return (
                acked,
                Err(err.context(format!(
                    "{node_id} leads the shard but its client address is not published, \
                     so there is nowhere to send the batch"
                ))),
            );
        };
        let addr: SocketAddr = match addr
            .parse()
            .with_context(|| format!("the leader's address {addr:?} is not usable"))
        {
            Ok(addr) => addr,
            Err(err) => return (acked, Err(err)),
        };
        let leader: Arc<Client> = match self.source {
            Source::Single(_) => {
                return (
                    acked,
                    Err(err.context(format!(
                        "{node_id} at {addr} leads the shard; connect a client there, or use a \
                         ClusterClient, which follows the refusal itself"
                    ))),
                );
            }
            Source::Cluster(cluster) => match cluster
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the shard's leader {node_id} at {addr}"))
            {
                Ok(leader) => leader,
                Err(err) => return (acked, Err(err)),
            },
        };
        let (more, result) = self.publish_on(&leader, key, routing_key, rest, next).await;
        if result.is_ok() {
            self.leaders.lock().await.insert(key.clone(), leader);
        }
        let mut acked = acked;
        acked.extend(more);
        (acked, result)
    }

    async fn publish_on(
        &self,
        client: &Client,
        key: &ShardKey,
        routing_key: Option<&Bytes>,
        batches: &[Vec<Vec<u8>>],
        first: u64,
    ) -> (Vec<Option<u64>>, Result<()>) {
        // Under a ClusterClient every publish names its shard, so each shard
        // can have its own stream. A plain Client's do not, so its producer
        // stays on the pool with the rest of that client's publishes.
        let publisher = match self.source {
            Source::Cluster(_) => client.shard_publisher(),
            Source::Single(_) => match client.publisher().await {
                Ok(publisher) => publisher,
                Err(err) => return (Vec::new(), Err(err)),
            },
        };
        publisher
            .publish_idempotent_pipelined(
                &key.0,
                &key.1,
                &key.2,
                routing_key,
                key.3,
                batches,
                self.producer_id,
                first,
            )
            .await
    }

    /// The shard a batch's sequence belongs to: 0 without a key, else the one
    /// the broker would route the key to.
    ///
    /// Unlike a plain keyed publish, which falls back to shard 0 when the
    /// width is unknown and lets the broker route, a guess here would number
    /// the batch against the wrong shard's sequence. So an unknown width is
    /// an error.
    async fn shard_for(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        routing_key: Option<&[u8]>,
    ) -> Result<u32> {
        let Some(routing_key) = routing_key else {
            return Ok(0);
        };
        let stream_key: StreamKey = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        let known = self.widths.lock().await.get(&stream_key).copied();
        let (shards, routing) = match known {
            Some(width) => width,
            None => {
                let client = match self.source {
                    Source::Single(client) => {
                        client.stream_routing(tenant_id, namespace, stream).await
                    }
                    Source::Cluster(cluster) => {
                        cluster
                            .client()
                            .await
                            .stream_routing(tenant_id, namespace, stream)
                            .await
                    }
                };
                let width = client.with_context(|| {
                    format!(
                        "learn {stream}'s shards, to number a keyed batch against the right one"
                    )
                })?;
                anyhow::ensure!(
                    width.0 > 0,
                    "the broker knows of no stream {stream} in {tenant_id}/{namespace}"
                );
                self.widths.lock().await.insert(stream_key, width);
                width
            }
        };
        Ok(felix_wire::routing::shard_for_routing(
            routing,
            shards,
            Some(routing_key),
        ))
    }
}

/// A stream.
type StreamKey = (String, String, String);

/// One shard of one stream: what a sequence numbers.
type ShardKey = (String, String, String, u32);

/// True when the shorter of `a` and `b` is a prefix of the other.
fn starts_alike(a: &[Vec<Vec<u8>>], b: &[Vec<Vec<u8>>]) -> bool {
    let overlap = a.len().min(b.len());
    a[..overlap] == b[..overlap]
}

/// Where a producer's batches go: one broker, or whichever a cluster client
/// is using, with the shard's leader remembered once a refusal names it.
enum Source<'a> {
    Single(&'a Client),
    Cluster(&'a ClusterClient),
}

/// Where a producer stands on one stream.
#[derive(Debug, Clone)]
enum Cursor {
    Next(u64),
    /// Batches went out from `sequence` on and no answer said whether they
    /// landed. Only the same batches, in the same order, may go out under
    /// those numbers again.
    InDoubt {
        sequence: u64,
        /// The key they went with. Only the same key may re-send them.
        routing_key: Option<Bytes>,
        batches: Vec<Vec<Vec<u8>>>,
        /// The batches of the failing call that were acknowledged, so the
        /// same call made again re-sends only the rest.
        settled: Vec<Vec<Vec<u8>>>,
    },
    Ended(PublishRefused),
}

/// Marks the producer in doubt unless the publish that armed it finished.
///
/// A cancelled publish is the one case the sequence mechanism cannot absorb.
/// Everything else about it is built so a re-send is safe *because the number
/// did not move* — but that holds only while the client knows whether the
/// number was used. Drop the future mid-send and it does not: the batch may
/// have been appended under that sequence, and the cursor still points at it.
///
/// The next batch would then go out under a spent number, and the broker's
/// contract is to answer a remembered sequence from memory *without appending*
/// — so a caller publishing different records would be told `Ok` and lose them
/// with nothing reported anywhere. Refusing afterwards is the only honest
/// answer, and this is what notices.
struct InDoubtOnCancel<'p> {
    flag: &'p AtomicBool,
    armed: bool,
}

impl<'p> InDoubtOnCancel<'p> {
    fn armed(flag: &'p AtomicBool) -> Self {
        Self { flag, armed: true }
    }

    /// The publish was answered, so the cursor is right either way.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for InDoubtOnCancel<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Release);
        }
    }
}
