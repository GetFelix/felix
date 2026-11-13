//! The snapshot body's framing, the cap, the install slot and the
//! timeouts, apart from any transport.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::raft_log::transfer::{
    self, Busy, DataCrc, DataSink, IDLE, Incoming, InstallSlot, Pieces, Progress, TransferError,
};

/// A sink that keeps what it is given.
#[derive(Default)]
struct Kept(Vec<u8>);

impl DataSink for Kept {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.0.extend_from_slice(&chunk);
        Ok(())
    }
}

fn body(head: &[u8], data: &[u8]) -> Vec<u8> {
    let mut crc = DataCrc::default();
    crc.update(data);
    let mut out = transfer::encode_start(head, data.len() as u64).to_vec();
    out.extend_from_slice(data);
    out.extend_from_slice(&crc.trailer());
    out
}

/// Pieces handed out in order, each after `gap`, counting how many were
/// asked for.
struct Fed {
    pieces: VecDeque<Bytes>,
    gap: Duration,
    asked: Arc<std::sync::atomic::AtomicUsize>,
}

impl Fed {
    fn new(bytes: &[u8], size: usize, gap: Duration) -> Self {
        Self {
            pieces: bytes.chunks(size).map(Bytes::copy_from_slice).collect(),
            gap,
            asked: Arc::default(),
        }
    }
}

impl Pieces for Fed {
    async fn next_piece(&mut self) -> Option<io::Result<Bytes>> {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.gap.is_zero() {
            tokio::time::sleep(self.gap).await;
        }
        self.pieces.pop_front().map(Ok)
    }
}

async fn receive(
    bytes: &[u8],
    size: usize,
    gap: Duration,
    cap: u64,
) -> Result<(Bytes, Vec<u8>), TransferError> {
    let pieces = Fed::new(bytes, size, gap);
    let mut incoming = Incoming::new(pieces);
    let head = incoming.head(cap).await?;
    let mut kept = Kept::default();
    incoming
        .data(head.data_len, &mut kept, std::future::pending())
        .await?;
    Ok((head.head, kept.0))
}

/// A body read back in pieces of any size gives the head and the data it
/// was made from.
#[tokio::test]
async fn a_body_round_trips_in_any_pieces() {
    let data: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
    let bytes = body(b"the head", &data);
    for size in [1, 7, 13, 64, bytes.len()] {
        let (head, got) = receive(&bytes, size, Duration::ZERO, u64::MAX)
            .await
            .expect("receive");
        assert_eq!(&head[..], b"the head", "pieces of {size}");
        assert_eq!(got, data, "pieces of {size}");
    }
}

/// A form byte this build does not know is refused, not parsed.
#[tokio::test]
async fn an_unknown_snapshot_form_is_refused() {
    let mut bytes = body(b"head", b"data");
    bytes[0] = transfer::FORM + 1;
    let err = receive(&bytes, 64, Duration::ZERO, u64::MAX)
        .await
        .expect_err("unknown form");
    assert!(matches!(err, TransferError::UnknownForm(2)), "{err}");
}

/// A declared length over the cap, or a head over its limit, is refused
/// from the first piece, before any data is asked for.
#[tokio::test]
async fn a_snapshot_over_the_cap_is_refused_before_its_data_is_read() {
    let data = vec![7u8; 4096];
    let bytes = body(b"head", &data);
    let pieces = Fed::new(&bytes, 64, Duration::ZERO);
    let asked = Arc::clone(&pieces.asked);
    let mut incoming = Incoming::new(pieces);
    let err = incoming.head(1024).await.expect_err("over the cap");
    assert!(
        matches!(
            err,
            TransferError::OverCap {
                data_len: 4096,
                cap: 1024
            }
        ),
        "{err}"
    );
    assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 1);

    let mut big_head = transfer::encode_start(&[], 0).to_vec();
    big_head[1..5].copy_from_slice(&((transfer::MAX_HEAD as u32) + 1).to_be_bytes());
    let err = receive(&big_head, 64, Duration::ZERO, u64::MAX)
        .await
        .expect_err("head too large");
    assert!(matches!(err, TransferError::HeadTooLarge(_)), "{err}");
}

/// The crc must match the data, the body must end after it, and a body
/// that stops early is short.
#[tokio::test]
async fn a_damaged_body_is_refused() {
    let good = body(b"head", b"some data");

    let mut bad_crc = good.clone();
    let last = bad_crc.len() - 1;
    bad_crc[last] ^= 1;
    let err = receive(&bad_crc, 5, Duration::ZERO, u64::MAX)
        .await
        .expect_err("crc");
    assert!(matches!(err, TransferError::Crc), "{err}");

    let mut trailing = good.clone();
    trailing.push(0);
    let err = receive(&trailing, 5, Duration::ZERO, u64::MAX)
        .await
        .expect_err("trailing");
    assert!(matches!(err, TransferError::Trailing), "{err}");

    let err = receive(&good[..good.len() - 6], 5, Duration::ZERO, u64::MAX)
        .await
        .expect_err("short");
    assert!(matches!(err, TransferError::Short { .. }), "{err}");
}

/// A transfer that keeps moving lands however long it takes in all: here
/// four minutes, with a piece every 20 s.
#[tokio::test(start_paused = true)]
async fn a_slow_snapshot_still_lands() {
    let data = vec![1u8; 120];
    let bytes = body(b"head", &data);
    let started = tokio::time::Instant::now();
    let (_, got) = receive(&bytes, 12, Duration::from_secs(20), u64::MAX)
        .await
        .expect("a moving transfer");
    assert_eq!(got, data);
    assert!(started.elapsed() > Duration::from_secs(120));
}

/// A transfer with no bytes for the idle limit is given up.
#[tokio::test(start_paused = true)]
async fn a_stalled_snapshot_is_given_up() {
    let bytes = body(b"head", b"data");
    let started = tokio::time::Instant::now();
    let err = receive(&bytes, 64, IDLE + Duration::from_secs(1), u64::MAX)
        .await
        .expect_err("stalled");
    assert!(matches!(err, TransferError::Stalled { .. }), "{err}");
    assert!(started.elapsed() < IDLE + Duration::from_secs(1));
}

/// A receive that is told to stop stops while it waits for data.
#[tokio::test(start_paused = true)]
async fn a_cancelled_receive_stops_waiting() {
    let bytes = body(b"head", &[3u8; 100]);
    let pieces = Fed::new(&bytes, 30, Duration::from_secs(10));
    let mut incoming = Incoming::new(pieces);
    let head = incoming.head(u64::MAX).await.expect("head");
    let err = incoming
        .data(
            head.data_len,
            &mut Kept::default(),
            tokio::time::sleep(Duration::from_secs(1)),
        )
        .await
        .expect_err("cancelled");
    assert!(matches!(err, TransferError::Cancelled), "{err}");
}

/// A sender gives up when nothing moves for the idle limit, and after the
/// last byte waits a bound that grows with the snapshot's size.
#[tokio::test(start_paused = true)]
async fn a_sender_is_bounded_by_progress() {
    let progress = Progress::new(1 << 30);
    let watching = progress.clone();
    let expired = tokio::spawn(async move { watching.expired().await });
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(20)).await;
        progress.moved(1);
    }
    assert!(!expired.is_finished(), "a moving send expired");
    progress.all_sent();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(
        !expired.is_finished(),
        "the install bound did not grow with size"
    );
    let started = tokio::time::Instant::now();
    let err = expired.await.expect("join");
    assert!(
        matches!(err, TransferError::Stalled { received: 10, .. }),
        "{err}"
    );
    assert_eq!(transfer::install_bound(1 << 30), Duration::from_secs(94));
    assert!(started.elapsed() <= Duration::from_secs(34));

    let idle = Progress::new(10);
    let started = tokio::time::Instant::now();
    idle.expired().await;
    assert_eq!(started.elapsed(), IDLE);
}

/// One install at a time: a sender ranked the same or lower is refused at
/// once and the holder keeps the slot.
#[tokio::test]
async fn a_second_install_is_refused_while_one_runs() {
    let slot = Arc::new(InstallSlot::<u64>::default());
    let held = slot.take(5).await.expect("take");
    assert_eq!(slot.take(5).await.expect_err("same rank"), Busy);
    assert_eq!(slot.take(4).await.expect_err("lower rank"), Busy);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), held.cancelled())
            .await
            .is_err(),
        "a refused sender cancelled the holder"
    );
    drop(held);
    slot.take(4).await.expect("after the holder");
}

/// A sender ranked above the holder cancels it and takes the slot once the
/// holder lets go.
#[tokio::test]
async fn a_newer_leader_takes_the_slot() {
    let slot = Arc::new(InstallSlot::<u64>::default());
    let held = slot.take(5).await.expect("take");
    let newer = {
        let slot = Arc::clone(&slot);
        tokio::spawn(async move { slot.take(6).await.map(|_| ()) })
    };
    tokio::time::timeout(Duration::from_secs(5), held.cancelled())
        .await
        .expect("the holder was not cancelled");
    assert!(!newer.is_finished());
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), newer)
        .await
        .expect("the newer sender waited on")
        .expect("join")
        .expect("the newer sender was refused");
}
