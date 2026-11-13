//! How a proposer hears where its entry was written.
//!
//! openraft answers a write only once the entry is applied. A proposer that
//! acknowledges earlier puts a [`WrittenToken`] on its command, and the
//! log's `append` resolves it as soon as the entry is written, before the
//! fsync.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::oneshot;

use super::LogPoint;
#[cfg(doc)]
use super::{EntryCodec, LogPositions};

/// Rides on a proposed command, in memory only. See
/// [`EntryCodec::written_token`].
///
/// A clone is the same token, and it resolves once.
#[derive(Clone)]
pub struct WrittenToken(Arc<parking_lot::Mutex<Option<oneshot::Sender<LogPoint>>>>);

/// Resolves to the index and term a token's entry was written at.
///
/// Written is not flushed and not committed: a power cut can take the
/// entry, and a later leader can cut it and give the index to another
/// term's entry. Whoever acts on it checks the term again at commit, and
/// waits on [`LogPositions::flushed`] where the fsync matters.
///
/// `None` means no append reported the entry written: the command was
/// dropped before it reached the log, or its append failed. An append that
/// fails part way may have written it all the same, so `None` is an unknown
/// outcome, not a promise that the entry is absent.
pub struct WrittenAt(oneshot::Receiver<LogPoint>);

impl WrittenToken {
    /// A token, and the future its append resolves.
    pub fn channel() -> (Self, WrittenAt) {
        let (sender, receiver) = oneshot::channel();
        (
            Self(Arc::new(parking_lot::Mutex::new(Some(sender)))),
            WrittenAt(receiver),
        )
    }

    pub(super) fn resolve(&self, at: LogPoint) {
        if let Some(sender) = self.0.lock().take() {
            // The proposer may have stopped waiting.
            let _ = sender.send(at);
        }
    }
}

impl fmt::Debug for WrittenToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WrittenToken")
    }
}

impl Future for WrittenAt {
    type Output = Option<LogPoint>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(Result::ok)
    }
}
