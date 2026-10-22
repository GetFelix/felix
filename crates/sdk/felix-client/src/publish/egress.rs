//! One writer per connection for all of its publish streams.
//!
//! Each publish stream has its own task (`writer`) that batches its queue, but
//! with per-shard streams a stream usually holds one or two frames, so a write
//! per stream is a packet per stream. quinn builds packets in the connection
//! driver, woken by the first stream write: it packs whatever streams have
//! data at that moment into shared packets. This task makes the streams'
//! writes land together. A writer hands its burst over and waits, and every
//! burst that arrived since the last pass is written in one poll of this task,
//! before the driver gets to run.
//!
//! A write that cannot finish (flow control) stays in `flights` and is
//! re-polled on its own wakeups. The other streams' writes are not behind it,
//! and neither are their acks, which each writer reads on its own stream.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

use anyhow::{Context, Result};
use bytes::Bytes;
use quinn::SendStream;
use tokio::sync::{mpsc, oneshot};

type Done = oneshot::Sender<(Vec<Bytes>, Result<()>)>;
type Write = Pin<Box<dyn Future<Output = (SendStream, Vec<Bytes>, Result<()>)> + Send>>;

enum Command {
    Attach {
        id: u64,
        send: SendStream,
    },
    Write {
        id: u64,
        chunks: Vec<Bytes>,
        done: Done,
    },
    Finish {
        id: u64,
        done: oneshot::Sender<Result<()>>,
    },
    Close {
        id: u64,
    },
}

/// The handle to a connection's egress task. Dropping the last one ends it.
pub(crate) struct Egress {
    tx: mpsc::UnboundedSender<Command>,
    next_id: AtomicU64,
}

impl Egress {
    /// Start the egress task. Needs a running tokio runtime.
    pub(crate) fn spawn() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(rx));
        Arc::new(Self {
            tx,
            next_id: AtomicU64::new(0),
        })
    }

    /// Put a stream's send half under this task's control.
    pub(crate) fn attach(self: &Arc<Self>, send: SendStream) -> EgressStream {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.send(Command::Attach { id, send });
        EgressStream {
            id,
            egress: Arc::clone(self),
        }
    }
}

/// One stream's send half, written through the connection's egress task.
pub(crate) struct EgressStream {
    id: u64,
    egress: Arc<Egress>,
}

impl EgressStream {
    /// Write every chunk in order, then leave `chunks` empty for reuse. At
    /// most one write per stream is outstanding: callers await it.
    pub(crate) async fn write(&self, chunks: &mut Vec<Bytes>) -> Result<()> {
        let (done, answer) = oneshot::channel();
        let command = Command::Write {
            id: self.id,
            chunks: std::mem::take(chunks),
            done,
        };
        if self.egress.tx.send(command).is_err() {
            anyhow::bail!("connection egress closed");
        }
        let (mut returned, result) = answer.await.context("connection egress closed")?;
        returned.clear();
        *chunks = returned;
        result
    }

    /// Close the send side, as `SendStream::finish` does.
    pub(crate) async fn finish(&self) -> Result<()> {
        let (done, answer) = oneshot::channel();
        if self
            .egress
            .tx
            .send(Command::Finish { id: self.id, done })
            .is_err()
        {
            anyhow::bail!("connection egress closed");
        }
        answer.await.context("connection egress closed")?
    }
}

impl Drop for EgressStream {
    fn drop(&mut self) {
        let _ = self.egress.tx.send(Command::Close { id: self.id });
    }
}

struct Flight {
    id: u64,
    write: Write,
    done: Done,
}

async fn run(mut rx: mpsc::UnboundedReceiver<Command>) {
    use std::collections::HashMap;
    // A stream is absent from `idle` while its write is in flight.
    let mut idle: HashMap<u64, SendStream> = HashMap::new();
    // Streams closed while a write was in flight: dropped when it lands.
    let mut closed: Vec<u64> = Vec::new();
    let mut flights: Vec<Flight> = Vec::new();
    let mut inbox_open = true;
    poll_fn(|cx| {
        // Take every command that is waiting, then poll the flights once: the
        // new writes all happen in this one poll of the task.
        while inbox_open {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(command)) => match command {
                    Command::Attach { id, send } => {
                        idle.insert(id, send);
                    }
                    Command::Write { id, chunks, done } => match idle.remove(&id) {
                        Some(send) => flights.push(Flight {
                            id,
                            write: Box::pin(write_all(send, chunks)),
                            done,
                        }),
                        None => {
                            let _ = done.send((chunks, Err(anyhow::anyhow!("stream is gone"))));
                        }
                    },
                    Command::Finish { id, done } => {
                        let result = match idle.get_mut(&id) {
                            Some(send) => send.finish().map_err(anyhow::Error::from),
                            None => Err(anyhow::anyhow!("stream is gone")),
                        };
                        let _ = done.send(result);
                    }
                    Command::Close { id } => {
                        if idle.remove(&id).is_none() && flights.iter().any(|f| f.id == id) {
                            closed.push(id);
                        }
                    }
                },
                Poll::Ready(None) => inbox_open = false,
                Poll::Pending => break,
            }
        }
        let mut index = 0;
        while index < flights.len() {
            match flights[index].write.as_mut().poll(cx) {
                Poll::Pending => index += 1,
                Poll::Ready((send, chunks, result)) => {
                    let flight = flights.swap_remove(index);
                    let landed = flight.done.send((chunks, result)).is_ok();
                    match closed.iter().position(|id| *id == flight.id) {
                        Some(at) => {
                            closed.swap_remove(at);
                        }
                        None if landed => {
                            idle.insert(flight.id, send);
                        }
                        None => {}
                    }
                }
            }
        }
        if !inbox_open && flights.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

async fn write_all(
    mut send: SendStream,
    mut chunks: Vec<Bytes>,
) -> (SendStream, Vec<Bytes>, Result<()>) {
    let result = send
        .write_all_chunks(&mut chunks)
        .await
        .context("write publish frames");
    (send, chunks, result)
}
