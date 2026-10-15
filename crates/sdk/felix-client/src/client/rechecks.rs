//! Re-asking stream widths over one long-lived control stream.
//!
//! A client asks each stream it publishes keyed batches to for its width
//! again every few seconds. Opening and authenticating a stream per question
//! would cost a handshake each, and taking turns on one stream would make
//! each question wait out every round trip ahead of it. So questions are
//! pipelined on one stream and their answers matched by request id.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::Message;
use felix_wire::routing::ShardRouting;
use hashbrown::HashMap;
use tokio::sync::{Mutex, oneshot};

use crate::connection::{Credentials, NodeConnections, OpenedStream, StreamLease};
use crate::frame_io::{read_message_with_limit, write_message};

type Answer = Result<(u32, ShardRouting)>;
type Pending = Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Answer>>>>;

/// Asks `stream_shards` on one shared stream, opened on first use and again
/// after it fails.
pub(super) struct WidthRechecks {
    node: Arc<NodeConnections>,
    credentials: Arc<Credentials>,
    max_frame_bytes: usize,
    stream: Mutex<Option<Arc<Shared>>>,
}

/// The open stream: its send half, and the questions waiting on the task
/// that reads answers off its receive half.
struct Shared {
    send: Mutex<quinn::SendStream>,
    pending: Pending,
    /// Set once the reader stops, so the next question opens a new stream.
    dead: Arc<AtomicBool>,
    reader: tokio::task::JoinHandle<()>,
    _lease: StreamLease,
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// Takes a question's slot back if it is given up before its answer.
struct Asked {
    pending: Pending,
    request_id: u64,
}

impl Drop for Asked {
    fn drop(&mut self) {
        self.pending
            .lock()
            .expect("pending widths")
            .remove(&self.request_id);
    }
}

impl WidthRechecks {
    pub(super) fn new(
        node: Arc<NodeConnections>,
        credentials: Arc<Credentials>,
        max_frame_bytes: usize,
    ) -> Self {
        Self {
            node,
            credentials,
            max_frame_bytes,
            stream: Mutex::new(None),
        }
    }

    /// The stream's width and mapping, asked alongside whatever else is in
    /// flight on the shared stream.
    pub(super) async fn ask(
        &self,
        request_id: u64,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Answer {
        let shared = self.shared().await?;
        let (_asked, answer_rx) = wait_for(&shared.pending, &shared.dead, request_id)?;
        let sent = write_message(
            &mut *shared.send.lock().await,
            Message::StreamShards {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                request_id,
            },
        )
        .await;
        if let Err(err) = sent {
            shared.dead.store(true, Ordering::Release);
            return Err(err).context("send stream shards request");
        }
        answer_rx
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("the width stream closed")))
    }

    async fn shared(&self) -> Result<Arc<Shared>> {
        let mut slot = self.stream.lock().await;
        if let Some(shared) = slot.as_ref()
            && !shared.dead.load(Ordering::Acquire)
        {
            return Ok(Arc::clone(shared));
        }
        let OpenedStream {
            send, recv, lease, ..
        } = self.node.open_as(&self.credentials).await?;
        let pending: Pending = Arc::default();
        let dead = Arc::new(AtomicBool::new(false));
        let reader = tokio::spawn(read_answers(
            recv,
            Arc::clone(&pending),
            Arc::clone(&dead),
            self.max_frame_bytes,
        ));
        let shared = Arc::new(Shared {
            send: Mutex::new(send),
            pending,
            dead,
            reader,
            _lease: lease,
        });
        *slot = Some(Arc::clone(&shared));
        Ok(shared)
    }
}

/// Register a question's answer slot on the stream.
///
/// Fails at once if the reader has already stopped: it sets `dead` before
/// it fails the questions it holds, so a question registered after that
/// would wait for an answer nothing will give. Checked after the insert, so
/// either the reader's drain sees this question or this sees `dead`.
fn wait_for(
    pending: &Pending,
    dead: &AtomicBool,
    request_id: u64,
) -> Result<(Asked, oneshot::Receiver<Answer>)> {
    let (answer_tx, answer_rx) = oneshot::channel();
    pending
        .lock()
        .expect("pending widths")
        .insert(request_id, answer_tx);
    let asked = Asked {
        pending: Arc::clone(pending),
        request_id,
    };
    anyhow::ensure!(!dead.load(Ordering::Acquire), "the width stream closed");
    Ok((asked, answer_rx))
}

/// Hand each answer to its question until the stream ends. Anything but an
/// answer ends it: an error carries no request id, and the broker closes the
/// stream after one, so every question still waiting fails with it.
async fn read_answers(
    mut recv: quinn::RecvStream,
    pending: Pending,
    dead: Arc<AtomicBool>,
    max_frame_bytes: usize,
) {
    let mut scratch = BytesMut::with_capacity(4 * 1024);
    let ended = loop {
        match read_message_with_limit(&mut recv, &mut scratch, max_frame_bytes).await {
            Ok(Some(Message::StreamShardsView {
                shards,
                routing,
                request_id,
            })) => {
                if let Some(asked) = pending.lock().expect("pending widths").remove(&request_id) {
                    let _ = asked.send(Ok((shards, routing.unwrap_or_default())));
                }
            }
            Ok(Some(Message::Error {
                message,
                code,
                retry,
                detail,
            })) => {
                break crate::error::refused(
                    "stream shards rejected",
                    message,
                    code,
                    retry,
                    detail,
                );
            }
            Ok(Some(other)) => {
                break anyhow::anyhow!("unexpected stream shards response: {other:?}");
            }
            Ok(None) => break anyhow::anyhow!("the width stream closed"),
            Err(err) => break err,
        }
    };
    dead.store(true, Ordering::Release);
    let message = format!("{ended:#}");
    for (_, asked) in pending.lock().expect("pending widths").drain() {
        let _ = asked.send(Err(anyhow::anyhow!("{message}")));
    }
}

#[cfg(test)]
mod tests;
