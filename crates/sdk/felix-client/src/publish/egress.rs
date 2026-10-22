//! One egress task per connection for all of its publish streams.
//!
//! Each publish stream has its own task (`writer`) that batches its queue, but
//! with per-shard streams a stream usually holds one or two frames, so a write
//! per stream is a packet per stream. quinn builds packets in the connection
//! driver, woken by the first stream write: it packs whatever streams have
//! data at that moment into shared packets. This task makes the streams'
//! writes land together. A writer hands its send half and burst over and
//! waits; every burst that arrived since the last pass is started in one poll
//! of this task, before the driver gets to run.
//!
//! A write that cannot finish (flow control) stays in `flights`, with its own
//! waker, and is polled only when quinn wakes it. The other streams' writes
//! are not behind it, and neither are their acks, which each writer reads on
//! its own stream. A stream alone on its connection has nothing to share
//! packets with and writes directly.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Wake, Waker};

use anyhow::{Context, Result};
use bytes::Bytes;
use quinn::SendStream;
use tokio::sync::{mpsc, oneshot};

/// A send half and its burst, handed to the egress task for one write.
struct Job {
    send: SendStream,
    chunks: Vec<Bytes>,
    done: oneshot::Sender<Back>,
}

/// What a write hands back: the send half, the emptied chunk list, and how
/// the write went.
struct Back {
    send: SendStream,
    chunks: Vec<Bytes>,
    result: Result<()>,
}

type Write = Pin<Box<dyn Future<Output = (oneshot::Sender<Back>, Back)> + Send>>;

/// Counters the tests read.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct EgressStats {
    pub(crate) direct_writes: AtomicUsize,
    pub(crate) relayed_writes: AtomicUsize,
    pub(crate) flight_polls: AtomicUsize,
    /// Set when the task has returned.
    pub(crate) finished: std::sync::atomic::AtomicBool,
}

/// The handle to a connection's egress task. The task ends once this and every
/// stream attached to it are dropped.
pub(crate) struct Egress {
    tx: mpsc::UnboundedSender<Job>,
    attached: AtomicUsize,
    #[cfg(test)]
    pub(crate) stats: Arc<EgressStats>,
}

impl Egress {
    /// Start the egress task. Needs a running tokio runtime.
    pub(crate) fn spawn() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        #[cfg(test)]
        let stats = Arc::<EgressStats>::default();
        let egress = Arc::new(Self {
            tx,
            attached: AtomicUsize::new(0),
            #[cfg(test)]
            stats: Arc::clone(&stats),
        });
        tokio::spawn(run(
            rx,
            #[cfg(test)]
            stats,
        ));
        egress
    }

    /// Put a stream's send half under this connection's egress.
    pub(crate) fn attach(self: &Arc<Self>, send: SendStream) -> EgressStream {
        self.attached.fetch_add(1, Ordering::SeqCst);
        EgressStream {
            egress: Arc::clone(self),
            send: Some(send),
        }
    }
}

/// One stream's send half, written through the connection's egress task.
pub(crate) struct EgressStream {
    egress: Arc<Egress>,
    /// Absent only while the egress task holds it for a write.
    send: Option<SendStream>,
}

impl EgressStream {
    /// Write every chunk in order and leave `chunks` empty for reuse. At most
    /// one write per stream is outstanding: callers await it.
    pub(crate) async fn write(&mut self, chunks: &mut Vec<Bytes>) -> Result<()> {
        let mut send = self.send.take().context("the stream's send half is lost")?;
        if self.egress.attached.load(Ordering::SeqCst) <= 1 {
            #[cfg(test)]
            self.egress
                .stats
                .direct_writes
                .fetch_add(1, Ordering::Relaxed);
            let result = send
                .write_all_chunks(chunks)
                .await
                .context("write publish frames");
            chunks.clear();
            self.send = Some(send);
            return result;
        }
        #[cfg(test)]
        self.egress
            .stats
            .relayed_writes
            .fetch_add(1, Ordering::Relaxed);
        let (done, answer) = oneshot::channel();
        let job = Job {
            send,
            chunks: std::mem::take(chunks),
            done,
        };
        if self.egress.tx.send(job).is_err() {
            anyhow::bail!("connection egress closed");
        }
        let back = answer.await.context("connection egress closed")?;
        self.send = Some(back.send);
        *chunks = back.chunks;
        chunks.clear();
        back.result
    }

    /// Close the send side, as `SendStream::finish` does.
    pub(crate) fn finish(&mut self) -> Result<()> {
        self.send
            .as_mut()
            .context("the stream's send half is lost")?
            .finish()
            .map_err(anyhow::Error::from)
    }
}

impl Drop for EgressStream {
    fn drop(&mut self) {
        self.egress.attached.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Wakes one write and, through it, the egress task.
struct Shared {
    ready: Mutex<Vec<u64>>,
    parent: Mutex<Option<Waker>>,
}

struct FlightWake {
    id: u64,
    shared: Arc<Shared>,
}

impl Wake for FlightWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.ready.lock().expect("ready").push(self.id);
        if let Some(parent) = self.shared.parent.lock().expect("parent").as_ref() {
            parent.wake_by_ref();
        }
    }
}

struct Flight {
    write: Write,
    waker: Waker,
}

// The task holds no `Egress`: that owns the sender, and a task that kept it
// would keep its own inbox open forever.
async fn run(mut rx: mpsc::UnboundedReceiver<Job>, #[cfg(test)] stats: Arc<EgressStats>) {
    let shared = Arc::new(Shared {
        ready: Mutex::new(Vec::new()),
        parent: Mutex::new(None),
    });
    let mut flights: HashMap<u64, Flight> = HashMap::new();
    let mut to_poll: Vec<u64> = Vec::new();
    let mut next_id = 0u64;
    let mut inbox_open = true;
    poll_fn(|cx| {
        {
            let mut parent = shared.parent.lock().expect("parent");
            if !parent
                .as_ref()
                .is_some_and(|known| known.will_wake(cx.waker()))
            {
                *parent = Some(cx.waker().clone());
            }
        }
        // Take every job that is waiting, so the new writes all start in this
        // one poll of the task.
        while inbox_open {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(job)) => {
                    let id = next_id;
                    next_id += 1;
                    flights.insert(
                        id,
                        Flight {
                            write: Box::pin(write_all(job)),
                            waker: Waker::from(Arc::new(FlightWake {
                                id,
                                shared: Arc::clone(&shared),
                            })),
                        },
                    );
                    to_poll.push(id);
                }
                Poll::Ready(None) => inbox_open = false,
                Poll::Pending => break,
            }
        }
        {
            let mut ready = shared.ready.lock().expect("ready");
            to_poll.append(&mut ready);
        }
        to_poll.sort_unstable();
        to_poll.dedup();
        for id in to_poll.drain(..) {
            let Some(flight) = flights.get_mut(&id) else {
                continue;
            };
            #[cfg(test)]
            stats.flight_polls.fetch_add(1, Ordering::Relaxed);
            let mut flight_cx = TaskContext::from_waker(&flight.waker);
            if let Poll::Ready((done, back)) = flight.write.as_mut().poll(&mut flight_cx) {
                flights.remove(&id);
                let _ = done.send(back);
            }
        }
        if !inbox_open && flights.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    #[cfg(test)]
    stats.finished.store(true, Ordering::SeqCst);
}

/// Write the burst. A writer that is gone before the write lands (its task
/// was dropped) resets the stream, which frees the buffered burst and tells
/// the peer, as dropping the stream mid-write did when the writer owned it.
async fn write_all(job: Job) -> (oneshot::Sender<Back>, Back) {
    let Job {
        mut send,
        mut chunks,
        mut done,
    } = job;
    let result = tokio::select! {
        biased;
        () = done.closed() => {
            let _ = send.reset(quinn::VarInt::from_u32(0));
            Err(anyhow::anyhow!("publish writer is gone"))
        }
        written = send.write_all_chunks(&mut chunks) => {
            written.context("write publish frames")
        }
    };
    (
        done,
        Back {
            send,
            chunks,
            result,
        },
    )
}
