//! How a snapshot travels between members of a Raft group on the segment
//! store, whatever carries it: the body's framing, the size cap, the one
//! install slot and the timeouts. A group's transport moves the bytes and
//! decides what goes in the head; the rest is here so every group behaves
//! the same. The design is in `docs/replication-engine.md`, "Sending a
//! snapshot".
//!
//! A body is:
//!
//! ```text
//! form       u8   FORM; anything else is refused, never guessed at
//! head_len   u32  big-endian, at most MAX_HEAD
//! data_len   u64  big-endian
//! head       head_len bytes, the transport's own (the sender's vote, the meta)
//! data       data_len bytes
//! crc        u32  big-endian crc32 of the data
//! ```
//!
//! The receiver reads the head first and can refuse the transfer (a stale
//! sender, a length over the cap, a busy slot) before any data is read.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};

/// The one body form this build reads and writes.
pub const FORM: u8 = 1;
/// The form byte, the head's length and the data's length.
pub const PREFIX_LEN: usize = 13;
/// The largest head a receiver reads.
pub const MAX_HEAD: usize = 64 * 1024;
/// How much data a sender reads from its file at a time.
pub const CHUNK: usize = 64 * 1024;
/// A transfer fails when no bytes move for this long, in either direction.
pub const IDLE: Duration = Duration::from_secs(30);
/// How long a sender that is refused the slot waits for a holder it
/// preempted to let go.
const PREEMPT_WAIT: Duration = Duration::from_secs(10);

/// Why a transfer was refused or failed.
#[derive(Debug)]
pub enum TransferError {
    /// A form byte this build does not know.
    UnknownForm(u8),
    /// A head over [`MAX_HEAD`].
    HeadTooLarge(usize),
    /// A declared data length over the receiver's cap.
    OverCap { data_len: u64, cap: u64 },
    /// The body ended before the length it declared.
    Short { received: u64 },
    /// Bytes after the crc.
    Trailing,
    /// The crc after the data does not match it.
    Crc,
    /// No bytes moved for [`IDLE`], or the install after the last byte
    /// took longer than [`install_bound`].
    Stalled { received: u64, elapsed: Duration },
    /// The receive was told to stop: a newer sender took the slot, or the
    /// store is closing.
    Cancelled,
    /// Reading the body or writing the file failed.
    Io(io::Error),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownForm(form) => write!(f, "unknown snapshot form {form}"),
            Self::HeadTooLarge(len) => {
                write!(f, "snapshot head of {len} bytes is over {MAX_HEAD}")
            }
            Self::OverCap { data_len, cap } => {
                write!(
                    f,
                    "snapshot of {data_len} bytes is over this member's cap of {cap}"
                )
            }
            Self::Short { received } => {
                write!(f, "snapshot body ended after {received} bytes")
            }
            Self::Trailing => write!(f, "bytes after the snapshot's crc"),
            Self::Crc => write!(f, "snapshot data does not match its crc"),
            Self::Stalled { received, elapsed } => write!(
                f,
                "snapshot transfer stalled after {received} bytes in {elapsed:?}"
            ),
            Self::Cancelled => write!(f, "snapshot receive cancelled"),
            Self::Io(err) => write!(f, "snapshot transfer: {err}"),
        }
    }
}

impl std::error::Error for TransferError {}

impl From<io::Error> for TransferError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// How long the receiver may take after the last byte (fsync, decode,
/// rename, apply) before the sender gives up: 30 s plus 1 s per 16 MiB.
pub fn install_bound(data_len: u64) -> Duration {
    Duration::from_secs(30 + data_len.div_ceil(16 << 20))
}

/// The bytes that start a body.
pub fn encode_start(head: &[u8], data_len: u64) -> Bytes {
    let head_len = u32::try_from(head.len()).expect("a snapshot head is far below 4 GiB");
    let mut out = BytesMut::with_capacity(PREFIX_LEN + head.len());
    out.extend_from_slice(&[FORM]);
    out.extend_from_slice(&head_len.to_be_bytes());
    out.extend_from_slice(&data_len.to_be_bytes());
    out.extend_from_slice(head);
    out.freeze()
}

/// The crc a sender writes after the data, kept as the data goes out.
#[derive(Debug, Clone, Default)]
pub struct DataCrc(crc32fast::Hasher);

impl DataCrc {
    pub fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    /// The four bytes that end a body.
    pub fn trailer(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0.clone().finalize().to_be_bytes())
    }
}

/// Where the receiver puts the data.
pub trait DataSink {
    fn write(&mut self, chunk: Bytes) -> impl Future<Output = io::Result<()>> + Send;
}

impl DataSink for super::StagedSnapshot {
    fn write(&mut self, chunk: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        super::StagedSnapshot::write(self, chunk)
    }
}

/// Where the receiver's body comes from: the transport's pieces, in order,
/// `None` at the end.
pub trait Pieces {
    fn next_piece(&mut self) -> impl Future<Output = Option<io::Result<Bytes>>> + Send;
}

/// A body as the receiver reads it, from whatever hands out its pieces.
/// Every wait for a piece is bounded by [`IDLE`].
pub struct Incoming<N> {
    next: N,
    pending: Bytes,
    received: u64,
    started: tokio::time::Instant,
}

/// What the head says about the data that follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    /// The transport's own head bytes.
    pub head: Bytes,
    pub data_len: u64,
}

impl<N: Pieces> Incoming<N> {
    pub fn new(next: N) -> Self {
        Self {
            next,
            pending: Bytes::new(),
            received: 0,
            started: tokio::time::Instant::now(),
        }
    }

    /// Bytes read so far.
    pub fn received(&self) -> u64 {
        self.received
    }

    /// Read the form, the lengths and the head, and nothing past them.
    /// Refuses an unknown form, a head over [`MAX_HEAD`] and a data length
    /// over `cap` before reading the data.
    pub async fn head(&mut self, cap: u64) -> Result<Head, TransferError> {
        let prefix = self.exactly(PREFIX_LEN).await?;
        if prefix[0] != FORM {
            return Err(TransferError::UnknownForm(prefix[0]));
        }
        let head_len = u32::from_be_bytes(prefix[1..5].try_into().expect("4 bytes")) as usize;
        let data_len = u64::from_be_bytes(prefix[5..13].try_into().expect("8 bytes"));
        if head_len > MAX_HEAD {
            return Err(TransferError::HeadTooLarge(head_len));
        }
        if data_len > cap {
            return Err(TransferError::OverCap { data_len, cap });
        }
        let head = self.exactly(head_len).await?;
        Ok(Head { head, data_len })
    }

    /// Copy `data_len` bytes of data into `sink`, then check the crc and
    /// that the body ends there. Stops with [`TransferError::Cancelled`]
    /// as soon as `cancel` resolves.
    pub async fn data(
        &mut self,
        data_len: u64,
        sink: &mut impl DataSink,
        cancel: impl Future<Output = ()>,
    ) -> Result<(), TransferError> {
        let mut cancel = std::pin::pin!(cancel);
        let copied = async {
            let mut crc = crc32fast::Hasher::new();
            let mut left = data_len;
            while left > 0 {
                if self.pending.is_empty() && !self.more().await? {
                    return Err(TransferError::Short {
                        received: self.received,
                    });
                }
                let take = usize::try_from(left)
                    .unwrap_or(usize::MAX)
                    .min(self.pending.len());
                let chunk = self.pending.split_to(take);
                crc.update(&chunk);
                left -= chunk.len() as u64;
                sink.write(chunk).await?;
            }
            let trailer = self.exactly(4).await?;
            if trailer[..] != crc.finalize().to_be_bytes() {
                return Err(TransferError::Crc);
            }
            if !self.pending.is_empty() || self.more().await? {
                return Err(TransferError::Trailing);
            }
            Ok(())
        };
        tokio::select! {
            copied = copied => copied,
            () = &mut cancel => Err(TransferError::Cancelled),
        }
    }

    async fn exactly(&mut self, len: usize) -> Result<Bytes, TransferError> {
        if self.pending.len() >= len {
            return Ok(self.pending.split_to(len));
        }
        let mut out = BytesMut::with_capacity(len);
        while out.len() < len {
            if self.pending.is_empty() && !self.more().await? {
                return Err(TransferError::Short {
                    received: self.received,
                });
            }
            let take = (len - out.len()).min(self.pending.len());
            out.extend_from_slice(&self.pending.split_to(take));
        }
        Ok(out.freeze())
    }

    /// Wait for the next piece. False at the body's end.
    async fn more(&mut self) -> Result<bool, TransferError> {
        match tokio::time::timeout(IDLE, self.next.next_piece()).await {
            Err(_) => Err(TransferError::Stalled {
                received: self.received,
                elapsed: self.started.elapsed(),
            }),
            Ok(None) => Ok(false),
            Ok(Some(Err(err))) => Err(TransferError::Io(err)),
            Ok(Some(Ok(piece))) => {
                self.received += piece.len() as u64;
                self.pending = piece;
                Ok(true)
            }
        }
    }
}

/// A sender's view of its transfer: when bytes last moved and whether the
/// last one has gone. [`Self::expired`] is what bounds the send.
#[derive(Debug, Clone)]
pub struct Progress {
    shared: Arc<ProgressShared>,
}

#[derive(Debug)]
struct ProgressShared {
    started: tokio::time::Instant,
    data_len: u64,
    /// Since `started`, in milliseconds.
    last_moved: AtomicU64,
    sent: AtomicU64,
    all_sent: AtomicBool,
}

impl Progress {
    pub fn new(data_len: u64) -> Self {
        Self {
            shared: Arc::new(ProgressShared {
                started: tokio::time::Instant::now(),
                data_len,
                last_moved: AtomicU64::new(0),
                sent: AtomicU64::new(0),
                all_sent: AtomicBool::new(false),
            }),
        }
    }

    /// `len` more bytes went out.
    pub fn moved(&self, len: usize) {
        self.shared.sent.fetch_add(len as u64, Ordering::Relaxed);
        self.touch();
    }

    /// The last byte went out; from here the receiver installs.
    pub fn all_sent(&self) {
        self.touch();
        self.shared.all_sent.store(true, Ordering::Release);
    }

    fn touch(&self) {
        let now = self.shared.started.elapsed().as_millis() as u64;
        self.shared.last_moved.store(now, Ordering::Release);
    }

    /// Resolves once nothing has moved for [`IDLE`] while sending, or the
    /// receiver has had [`install_bound`] since the last byte.
    pub async fn expired(&self) -> TransferError {
        loop {
            let limit = if self.shared.all_sent.load(Ordering::Acquire) {
                install_bound(self.shared.data_len)
            } else {
                IDLE
            };
            let last = Duration::from_millis(self.shared.last_moved.load(Ordering::Acquire));
            let deadline = self.shared.started + last + limit;
            if tokio::time::Instant::now() >= deadline {
                return TransferError::Stalled {
                    received: self.shared.sent.load(Ordering::Relaxed),
                    elapsed: self.shared.started.elapsed(),
                };
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

/// The one snapshot install a member runs at a time, ranked by `R` (for
/// a Raft group, the sender's vote). A second sender is refused at once
/// rather than queued with its body open. A sender ranked above the holder
/// cancels it and takes the slot, so a stale leader that trickles bytes
/// cannot hold off the real one.
#[derive(Debug)]
pub struct InstallSlot<R> {
    held: parking_lot::Mutex<Option<Held<R>>>,
    released: tokio::sync::Notify,
    next_id: AtomicU64,
}

#[derive(Debug)]
struct Held<R> {
    rank: R,
    id: u64,
    cancel: tokio::sync::watch::Sender<bool>,
}

/// The slot is held by a sender ranked at or above this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Busy;

impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "another snapshot install is in progress")
    }
}

impl std::error::Error for Busy {}

impl<R: PartialOrd> Default for InstallSlot<R> {
    fn default() -> Self {
        Self {
            held: parking_lot::Mutex::new(None),
            released: tokio::sync::Notify::new(),
            next_id: AtomicU64::new(0),
        }
    }
}

impl<R: PartialOrd> InstallSlot<R> {
    /// Take the slot for a sender ranked `rank`. Refused with [`Busy`]
    /// when the holder ranks the same or higher, or cannot be compared,
    /// or does not let go within a bounded wait after being cancelled.
    pub async fn take(self: &Arc<Self>, rank: R) -> Result<SlotGuard<R>, Busy> {
        let deadline = tokio::time::Instant::now() + PREEMPT_WAIT;
        loop {
            let released = self.released.notified();
            let mut released = std::pin::pin!(released);
            {
                let mut held = self.held.lock();
                match held.as_ref() {
                    None => {
                        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                        let (cancel, cancelled) = tokio::sync::watch::channel(false);
                        *held = Some(Held { rank, id, cancel });
                        return Ok(SlotGuard {
                            slot: Arc::clone(self),
                            id,
                            cancelled,
                        });
                    }
                    Some(holder) if rank > holder.rank => {
                        holder.cancel.send_replace(true);
                        released.as_mut().enable();
                    }
                    Some(_) => return Err(Busy),
                }
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return Err(Busy);
            }
        }
    }
}

/// The slot, held until dropped.
#[derive(Debug)]
pub struct SlotGuard<R: PartialOrd> {
    slot: Arc<InstallSlot<R>>,
    id: u64,
    cancelled: tokio::sync::watch::Receiver<bool>,
}

impl<R: PartialOrd> SlotGuard<R> {
    /// Resolves once a higher-ranked sender has asked for the slot.
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut cancelled = self.cancelled.clone();
        async move {
            let _ = cancelled.wait_for(|cancelled| *cancelled).await;
        }
    }
}

impl<R: PartialOrd> Drop for SlotGuard<R> {
    fn drop(&mut self) {
        let mut held = self.slot.held.lock();
        if held.as_ref().is_some_and(|holder| holder.id == self.id) {
            *held = None;
        }
        drop(held);
        self.slot.released.notify_waiters();
    }
}
