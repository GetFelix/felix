//! A producer whose publishes land once, however many times they are sent.
//!
//! The broker hands out a producer id; the producer numbers its batches on
//! each stream from zero and sends the number with the batch. The shard's
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
//! refused rather than sent, since the leader would answer it from memory
//! and report success without appending it. A typed refusal ends the
//! producer on that stream, because a gap or a forgotten producer is not
//! something a re-send can mend. A forgotten producer is not started
//! again under a new id here: whether its last batch landed is exactly what
//! the shard can no longer say, so the caller has to decide.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use tokio::sync::Mutex;

use crate::client::Client;
use crate::cluster::{Attempt, ClusterClient, Next, Retrying};
use crate::{PublishRefusalReason, PublishRefused};

/// A producer whose batches are appended once, however many times they are
/// sent. See the module documentation.
///
/// One sequence per stream, so a producer may publish to several streams;
/// publishes to one stream are serialised, since the sequence has to be. A
/// batch of any size takes one sequence.
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
    cursors: Mutex<HashMap<(String, String, String), Cursor>>,
    /// The broker a refusal named as the leader of a stream, kept so the next
    /// batch goes straight there rather than being refused again.
    leaders: Mutex<HashMap<(String, String, String), Arc<Client>>>,
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
        }
    }

    /// The id the broker assigned this producer.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    /// Publish one payload, once.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
    ) -> Result<()> {
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
    ) -> Result<()> {
        let key = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
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
        let sequence = match cursors.get(&key) {
            None => 0,
            Some(Cursor::Next(sequence)) => *sequence,
            Some(Cursor::InDoubt {
                sequence,
                payloads: pending,
            }) => {
                if *pending != payloads {
                    anyhow::bail!(
                        "the last batch on this stream failed without a definite answer, so \
                         sequence {sequence} may already hold it. A different batch under that \
                         sequence would be acknowledged without being appended, so it is not \
                         sent: re-send the same batch, or call producer_init for a fresh \
                         producer.",
                    );
                }
                *sequence
            }
            Some(Cursor::Ended(refused)) => {
                return Err(refused.clone()).context("this producer was ended on the stream");
            }
        };
        // Armed across the send and disarmed the instant it answers: between
        // those two points the caller's future may be dropped, and that is the
        // window where the cursor and the broker can disagree.
        let cancelled = InDoubtOnCancel::armed(&self.in_doubt);
        let result = self
            .send(tenant_id, namespace, stream, &payloads, sequence, &key)
            .await;
        cancelled.disarm();
        match result {
            Ok(()) => {
                cursors.insert(key, Cursor::Next(sequence + 1));
                Ok(())
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
                    _ => Cursor::InDoubt { sequence, payloads },
                };
                cursors.insert(key, cursor);
                Err(err)
            }
        }
    }

    /// One batch under one sequence, re-sent until answered or the policy
    /// runs out. Only ever the same number: a re-send is safe *because* the
    /// number did not move.
    async fn send(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
        sequence: u64,
        key: &(String, String, String),
    ) -> Result<()> {
        match self.source {
            Source::Single(client) => {
                self.send_via(
                    client, tenant_id, namespace, stream, payloads, sequence, key,
                )
                .await
            }
            Source::Cluster(cluster) => {
                let started = std::time::Instant::now();
                let policy = cluster.policy();
                let mut last: Option<anyhow::Error> = None;
                let mut retrying = Retrying::default();
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
                    let err = match self
                        .send_via(
                            &client, tenant_id, namespace, stream, payloads, sequence, key,
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
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
                        Next::Fail => return Err(err),
                        Next::Reroute => {
                            self.leaders.lock().await.remove(key);
                            wait = None;
                        }
                        Next::Backoff { at_least } => wait = Some(at_least),
                    }
                    last = Some(err);
                }
                Err(last
                    .unwrap_or_else(|| anyhow::anyhow!("publish failed"))
                    .context(format!(
                        "gave up after {:?} and at most {} attempts; sequence {sequence} \
                         was not advanced; re-send the same batch under it",
                        started.elapsed(),
                        policy.attempts.max(1),
                    )))
            }
        }
    }

    /// Send to the stream's leader if one is remembered, else to `client`,
    /// following one not-leader refusal to the broker it names.
    #[allow(clippy::too_many_arguments)]
    async fn send_via(
        &self,
        client: &Client,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
        sequence: u64,
        key: &(String, String, String),
    ) -> Result<()> {
        let remembered = self.leaders.lock().await.get(key).cloned();
        let first = match &remembered {
            Some(leader) => {
                self.publish_on(leader, tenant_id, namespace, stream, payloads, sequence)
                    .await
            }
            None => {
                self.publish_on(client, tenant_id, namespace, stream, payloads, sequence)
                    .await
            }
        };
        let err = match first {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        let (node_id, addr) = match err.downcast_ref::<PublishRefused>() {
            Some(PublishRefused {
                reason: PublishRefusalReason::NotLeader { node_id, addr },
                ..
            }) => (node_id.clone(), addr.clone()),
            _ => return Err(err),
        };
        // Only the leader holds the sequences, so the batch goes to it
        // rather than through a forward. One hop: a correct cluster needs
        // one, and a second refusal means the answer is moving.
        let Some(addr) = addr else {
            return Err(err.context(format!(
                "{node_id} leads the shard but its client address is not published, \
                 so there is nowhere to send the batch"
            )));
        };
        let addr: SocketAddr = addr
            .parse()
            .with_context(|| format!("the leader's address {addr:?} is not usable"))?;
        let leader = Arc::new(match self.source {
            Source::Single(_) => {
                return Err(err.context(format!(
                    "{node_id} at {addr} leads the shard; connect a client there, or use a \
                     ClusterClient, which follows the refusal itself"
                )));
            }
            Source::Cluster(cluster) => cluster
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the shard's leader {node_id} at {addr}"))?,
        });
        let result = self
            .publish_on(&leader, tenant_id, namespace, stream, payloads, sequence)
            .await;
        if result.is_ok() {
            self.leaders.lock().await.insert(key.clone(), leader);
        }
        result
    }

    async fn publish_on(
        &self,
        client: &Client,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
        sequence: u64,
    ) -> Result<()> {
        client
            .publisher()
            .await?
            .publish_idempotent_batch(
                tenant_id,
                namespace,
                stream,
                payloads.to_vec(),
                self.producer_id,
                sequence,
            )
            .await
    }
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
    /// A batch went out under `sequence` and no answer said whether it
    /// landed. Only the same batch may go out under it again.
    InDoubt {
        sequence: u64,
        payloads: Vec<Vec<u8>>,
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
