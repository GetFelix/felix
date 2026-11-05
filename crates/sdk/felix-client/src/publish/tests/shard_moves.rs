//! Moving a shard between the pool and a stream of its own, against fake
//! brokers that answer only when told: what lands where and in what order,
//! and that nothing waits on itself.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use felix_wire::{AckMode, Message};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};

use crate::publish::shard_streams::{CALM_FIRST, HeadWatch, STALL_FLOOR, ShardTicket, now_nanos};
use crate::publish::writer::{PublishRequest, PublishWorker, note_head_answered};
use crate::publish::{AckOutcome, Acked, OpenWorker, Publisher, PublisherInner};

/// One broker behind every stream of the client. It commits a stream's
/// requests in the order the stream carried them, and holds one producer's
/// sequence the way a shard leader does: the next one is taken, an earlier
/// one is a duplicate answered without landing again, a later one is a gap
/// and refused.
#[derive(Default)]
struct Broker {
    /// What landed, in order: the stream it came on and its number.
    log: Mutex<Vec<(&'static str, u64)>>,
    next_sequence: Mutex<u64>,
}

impl Broker {
    fn commit(&self, stream: &'static str, message: &Message) -> AckOutcome {
        match message {
            Message::PublishBatch { payloads, .. } => {
                self.log
                    .lock()
                    .unwrap()
                    .push((stream, number(&payloads[0])));
            }
            Message::PublishIdempotent { sequence, .. } => {
                let mut next = self.next_sequence.lock().unwrap();
                match (*sequence).cmp(&*next) {
                    std::cmp::Ordering::Less => return Ok(Acked::default()),
                    std::cmp::Ordering::Greater => {
                        anyhow::bail!("sequence {sequence} out of order, expected {next}")
                    }
                    std::cmp::Ordering::Equal => *next += 1,
                }
                self.log.lock().unwrap().push((stream, *sequence));
            }
            other => panic!("unexpected request {other:?}"),
        }
        Ok(Acked::default())
    }

    fn landed(&self) -> Vec<(&'static str, u64)> {
        self.log.lock().unwrap().clone()
    }
}

fn number(payload: &[u8]) -> u64 {
    std::str::from_utf8(payload).unwrap().parse().unwrap()
}

/// A stream to `broker` that answers one request per permit of `gate`, in
/// order, and times its head as the real writer does. `kill` ends it the
/// way a lost connection does, failing what it holds.
fn gated_worker(
    stream: &'static str,
    broker: &Arc<Broker>,
    gate: &Arc<Semaphore>,
    kill: &Arc<Notify>,
) -> PublishWorker {
    struct Held {
        message: Message,
        response: oneshot::Sender<AckOutcome>,
        ticket: Option<ShardTicket>,
    }
    let shard = |held: &Held| held.ticket.as_ref().map_or(0, ShardTicket::id);
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(64);
    let head = Arc::new(HeadWatch::default());
    let watch = Arc::clone(&head);
    let (broker, gate, kill) = (Arc::clone(broker), Arc::clone(gate), Arc::clone(kill));
    let handle = tokio::spawn(async move {
        let mut held: VecDeque<Held> = VecDeque::new();
        loop {
            tokio::select! {
                biased;
                () = kill.notified() => {
                    watch.set(None, 0);
                    for held in held.drain(..) {
                        let _ = held.response.send(Err(anyhow::anyhow!("connection lost")));
                    }
                    return Ok(());
                }
                request = rx.recv() => match request {
                    Some(PublishRequest::Message { message, response, ticket, .. }) => {
                        let request = Held { message, response, ticket };
                        if held.is_empty() {
                            watch.set(Some(shard(&request)), now_nanos());
                        }
                        held.push_back(request);
                    }
                    Some(PublishRequest::Finish { response }) => {
                        let _ = response.send(Ok(Acked::default()));
                        return Ok(());
                    }
                    Some(PublishRequest::BinaryBytes { .. }) => panic!("binary to a JSON broker"),
                    None => return Ok(()),
                },
                permit = gate.acquire(), if !held.is_empty() => {
                    permit.expect("gate").forget();
                    let answered = held.pop_front().expect("a held request");
                    let outcome = broker.commit(stream, &answered.message);
                    note_head_answered(&watch, answered.ticket.as_ref(), held.front().map(shard));
                    let _ = answered.response.send(outcome);
                }
            }
        }
    });
    PublishWorker {
        tx,
        handle: tokio::sync::Mutex::new(Some(handle)),
        request_counter: AtomicU64::new(1),
        // JSON only, so the fake can read every request.
        server_flags: felix_wire::ORIGINAL_V1_FLAGS,
        publish_window: 0,
        takes_width: true,
        takes_shard: true,
        takes_record_headers: false,
        head,
    }
}

const OPEN: usize = 1 << 20;

struct Fixture {
    publisher: Publisher,
    broker: Arc<Broker>,
    pool_gate: Arc<Semaphore>,
    own_gate: Arc<Semaphore>,
    /// Ends every stream opened for a shard of its own.
    kill_own: Arc<Notify>,
}

/// A client with one pooled stream; every stream opened for a shard shares
/// `own_gate`. Both gates start shut.
fn fixture() -> Fixture {
    let broker = Arc::new(Broker::default());
    let pool_gate = Arc::new(Semaphore::new(0));
    let own_gate = Arc::new(Semaphore::new(0));
    let kill_own = Arc::new(Notify::new());
    let pool = gated_worker("pool", &broker, &pool_gate, &Arc::new(Notify::new()));
    let open: OpenWorker = {
        let (broker, own_gate, kill_own) = (
            Arc::clone(&broker),
            Arc::clone(&own_gate),
            Arc::clone(&kill_own),
        );
        Arc::new(move || {
            let worker = gated_worker("own", &broker, &own_gate, &kill_own);
            Box::pin(async move { Ok(worker) })
        })
    };
    Fixture {
        publisher: Publisher {
            inner: Arc::new(PublisherInner::with_shard_streams(
                Arc::new(vec![pool]),
                8,
                open,
            )),
        },
        broker,
        pool_gate,
        own_gate,
        kill_own,
    }
}

/// Let every task run until it waits on something.
async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

/// Start publishing `n` to `shard` and let it get as far as it can.
async fn publish(f: &Fixture, shard: u32, n: u64) -> tokio::task::JoinHandle<AckOutcome> {
    let publisher = f.publisher.clone();
    let task = tokio::spawn(async move {
        publisher
            .publish_batch_json_on_shard(
                "t",
                "ns",
                "orders",
                shard,
                vec![n.to_string().into_bytes()],
                AckMode::PerBatch,
            )
            .await
    });
    settle().await;
    task
}

/// Start the idempotent batch `sequence` of producer 7, shard 0.
async fn publish_sequence(f: &Fixture, sequence: u64) -> tokio::task::JoinHandle<AckOutcome> {
    let publisher = f.publisher.clone();
    let task = tokio::spawn(async move {
        publisher
            .publish_idempotent_batch("t", "ns", "orders", vec![b"x".to_vec()], 7, sequence)
            .await
            .map(|_| Acked::default())
    });
    settle().await;
    task
}

async fn all_ok(tasks: Vec<tokio::task::JoinHandle<AckOutcome>>) {
    for task in tasks {
        task.await.expect("join").expect("publish");
    }
}

/// The numbers that landed, with those after the first `before` sorted.
fn in_order(landed: &[(&'static str, u64)], before: usize) -> Vec<u64> {
    let mut numbers: Vec<u64> = landed.iter().map(|(_, n)| *n).collect();
    numbers[before..].sort_unstable();
    numbers
}

fn own_count(f: &Fixture) -> usize {
    f.publisher
        .inner
        .shard_streams
        .as_ref()
        .expect("shard streams")
        .own_count()
}

/// Publishes handed to one stream before a move land before any sent after
/// it, both ways. The ones before are with their stream, unanswered, when
/// the ones after start, so the move has to wait for those answers. The ones
/// after wait on the move together, so only they may land in any order.
#[tokio::test(start_paused = true)]
async fn a_shards_order_holds_across_a_move_both_ways() {
    let f = fixture();
    f.own_gate.add_permits(OPEN);
    let mut before = Vec::new();
    for n in 0..3 {
        before.push(publish(&f, 1, n).await);
    }
    // The pool's head, shard 1's first publish, is overdue.
    tokio::time::advance(STALL_FLOOR * 2).await;
    let mut after = Vec::new();
    for n in 3..6 {
        after.push(publish(&f, 1, n).await);
    }
    assert!(f.broker.landed().is_empty());
    assert!(after.iter().all(|task| !task.is_finished()));
    f.pool_gate.add_permits(OPEN);
    all_ok(before).await;
    all_ok(after).await;
    let landed = f.broker.landed();
    assert_eq!(
        in_order(&landed, 3),
        (0..6).collect::<Vec<_>>(),
        "out of order across the move out: {landed:?}"
    );
    assert!(landed[3..].iter().all(|(stream, _)| *stream == "own"));

    // The way back with publishes in flight on the shard's own stream, sent
    // just before it has been calm for long enough, and not yet overdue
    // when it has.
    let quarter = STALL_FLOOR / 4;
    tokio::time::advance(CALM_FIRST - quarter).await;
    let own_gate = Arc::clone(&f.own_gate);
    own_gate.forget_permits(OPEN);
    let mut before = Vec::new();
    for n in 6..9 {
        before.push(publish(&f, 1, n).await);
    }
    tokio::time::advance(quarter * 2).await;
    let mut after = Vec::new();
    for n in 9..12 {
        after.push(publish(&f, 1, n).await);
    }
    assert_eq!(f.broker.landed().len(), 6);
    own_gate.add_permits(OPEN);
    all_ok(before).await;
    all_ok(after).await;
    let landed = f.broker.landed();
    assert_eq!(
        in_order(&landed[6..], 3),
        (6..12).collect::<Vec<_>>(),
        "out of order across the move back: {landed:?}"
    );
    assert!(landed[9..].iter().all(|(stream, _)| *stream == "pool"));
    assert_eq!(own_count(&f), 0);
}

/// One producer's sequence across a move: every batch lands once, with no
/// gap, and a batch sent again after the move is answered as a duplicate.
/// A batch sent on the new stream before the old one's were answered would
/// reach the leader ahead of them and be refused as a gap.
#[tokio::test(start_paused = true)]
async fn a_producers_sequence_has_no_gap_or_duplicate_across_a_move() {
    let f = fixture();
    f.own_gate.add_permits(OPEN);
    let mut sent = Vec::new();
    for sequence in 0..4 {
        sent.push(publish_sequence(&f, sequence).await);
    }
    tokio::time::advance(STALL_FLOOR * 2).await;
    sent.push(publish_sequence(&f, 4).await);
    f.pool_gate.add_permits(OPEN);
    all_ok(sent).await;
    for sequence in 5..8 {
        publish_sequence(&f, sequence).await.await.unwrap().unwrap();
    }
    // A batch whose answer the producer lost goes out again.
    publish_sequence(&f, 5).await.await.unwrap().unwrap();
    let landed = f.broker.landed();
    assert_eq!(
        landed
            .iter()
            .map(|(_, sequence)| *sequence)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>(),
        "{landed:?}"
    );
    assert!(landed[4..].iter().all(|(stream, _)| *stream == "own"));
}

/// A shard stuck at its pooled stream's head holds up only itself: the
/// other shards of the stream, publishing steadily, leave the pool rather
/// than queue behind it, and come back only once the pool is calm.
#[tokio::test(start_paused = true)]
async fn a_stalled_shard_does_not_hold_up_its_stream_mates() {
    let f = fixture();
    f.own_gate.add_permits(OPEN);
    let stuck = publish(&f, 1, 0).await;
    tokio::time::advance(STALL_FLOOR * 2).await;
    let mut healthy = Vec::new();
    for shard in 2..8 {
        let publisher = f.publisher.clone();
        healthy.push(tokio::spawn(async move {
            for n in 0..20 {
                publisher
                    .publish_batch_json_on_shard(
                        "t",
                        "ns",
                        "orders",
                        shard,
                        vec![(100 * shard as u64 + n).to_string().into_bytes()],
                        AckMode::PerBatch,
                    )
                    .await?;
            }
            anyhow::Ok(())
        }));
    }
    settle().await;
    for task in healthy {
        assert!(
            task.is_finished(),
            "a healthy shard waited on the stalled one"
        );
        task.await.expect("join").expect("publish");
    }
    assert!(!stuck.is_finished());
    // Calm themselves, but the pool still is not: they stay out.
    tokio::time::advance(CALM_FIRST * 2).await;
    let still_out = publish(&f, 2, 900).await;
    assert!(
        still_out.is_finished(),
        "a calm shard went back to a stalled pool"
    );
    still_out.await.expect("join").expect("publish");
    f.pool_gate.add_permits(OPEN);
    stuck.await.expect("join").expect("publish");
    publish(&f, 2, 901)
        .await
        .await
        .expect("join")
        .expect("publish");
    let landed = f.broker.landed();
    assert_eq!(landed.last(), Some(&("pool", 901)), "{landed:?}");
}

/// A shard's own stream lost with publishes on it: they fail rather than
/// hang, the shard goes back to the pool, and its place under the cap is
/// given back.
#[tokio::test(start_paused = true)]
async fn a_lost_shard_stream_sends_the_shard_back_to_the_pool() {
    let f = fixture();
    f.pool_gate.add_permits(OPEN);
    // Moved out by a late answer at the pool's head.
    let pool_gate = Arc::clone(&f.pool_gate);
    pool_gate.forget_permits(OPEN);
    let late = publish(&f, 1, 0).await;
    tokio::time::advance(STALL_FLOOR * 2).await;
    pool_gate.add_permits(OPEN);
    late.await.expect("join").expect("publish");
    let on_own = vec![publish(&f, 1, 1).await, publish(&f, 1, 2).await];
    assert_eq!(own_count(&f), 1);
    f.kill_own.notify_waiters();
    for task in on_own {
        assert!(
            task.await.expect("join").is_err(),
            "a lost publish succeeded"
        );
    }
    publish(&f, 1, 3)
        .await
        .await
        .expect("join")
        .expect("publish");
    assert_eq!(f.broker.landed().last(), Some(&("pool", 3)));
    assert_eq!(own_count(&f), 0);
}

/// A run of idempotent batches sent one at a time routes each batch itself.
/// Holding the run's own route across them would keep the shard from moving
/// once a late answer to the first asks for a move: the second batch would
/// wait on the run, and the run on it, for good.
#[tokio::test(start_paused = true)]
async fn a_run_of_batches_does_not_wait_on_its_own_move() {
    let f = fixture();
    f.own_gate.add_permits(OPEN);
    let publisher = f.publisher.clone();
    let run = tokio::spawn(async move {
        let batches = vec![vec![b"x".to_vec()]; 3];
        publisher
            .publish_idempotent_pipelined("t", "ns", "orders", None, 0, None, &batches, 7, 0)
            .await
    });
    settle().await;
    tokio::time::advance(STALL_FLOOR * 2).await;
    f.pool_gate.add_permits(OPEN);
    let (acked, outcome) = tokio::time::timeout(std::time::Duration::from_secs(60), run)
        .await
        .expect("the run waited on its own move")
        .expect("join");
    outcome.expect("run");
    assert_eq!(acked.len(), 3);
    let landed = f.broker.landed();
    assert_eq!(landed, [("pool", 0), ("own", 1), ("own", 2)]);
}
