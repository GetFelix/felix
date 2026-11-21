use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::DiskLog;
use crate::log::{AppendRecord, CondPiece, FsyncMode, LogConfig};

fn piece(position: u32, len: u32) -> RecordMark {
    RecordMark::CondPiece(CondPiece { position, len })
}

fn at(offset: Offset, mark: RecordMark) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 1,
        checksum: 0,
        payload: Bytes::from(offset.to_string()),
        mark,
        publisher: None,
        headers: None,
        generation: Some(1),
    }
}

/// Records at consecutive offsets from `first`.
fn run(first: Offset, marks: &[RecordMark]) -> Vec<LogRecord> {
    marks
        .iter()
        .zip(first..)
        .map(|(mark, offset)| at(offset, *mark))
        .collect()
}

fn offsets(records: &[LogRecord]) -> Vec<Offset> {
    records.iter().map(|record| record.offset).collect()
}

const BLANK: RecordMark = RecordMark::GenerationStart;
const PLAIN: RecordMark = RecordMark::None;

/// A whole batch is read whole, also from a page that starts inside it.
#[test]
fn a_whole_batch_is_read() {
    let records = run(0, &[PLAIN, piece(0, 3), piece(1, 3), piece(2, 3), PLAIN]);
    let resolved = resolve(records.clone(), 5, 5);
    assert_eq!(offsets(&resolved.records), [0, 1, 2, 3, 4]);
    assert!(resolved.pending.is_empty());
    assert_eq!(resolved.next, 5);

    let resolved = resolve(records[2..].to_vec(), 5, 5);
    assert_eq!(offsets(&resolved.records), [2, 3, 4]);
}

/// A batch whose next piece's offset holds anything else was cut: none of
/// it is read, and the read goes on past it.
#[test]
fn a_cut_batch_is_skipped() {
    for after in [BLANK, PLAIN] {
        let records = run(0, &[piece(0, 4), piece(1, 4), after, PLAIN]);
        assert_eq!(
            offsets(&resolve(records, 4, 4).records),
            [2, 3],
            "{after:?}"
        );
    }
    // A piece that does not continue the batch, then one that does not
    // continue that.
    for after in [piece(0, 4), piece(1, 4), piece(2, 5), piece(3, 5)] {
        let records = run(0, &[piece(0, 4), piece(1, 4), after, PLAIN]);
        assert_eq!(offsets(&resolve(records, 4, 4).records), [3], "{after:?}");
    }
}

/// A last piece of another length where this batch's last piece belongs
/// does not finish it.
#[test]
fn a_last_piece_of_another_batch_does_not_finish_one() {
    // The last piece of a shorter batch where the next piece belongs. Like
    // the model, the rule reads marks only, so that piece is read on its own.
    let records = run(0, &[piece(0, 3), piece(1, 2), PLAIN]);
    assert_eq!(offsets(&resolve(records, 3, 3).records), [1, 2]);
    let records = run(0, &[piece(0, 3), piece(1, 3), piece(2, 4), PLAIN]);
    assert_eq!(offsets(&resolve(records, 4, 4).records), [3]);
    let records = run(0, &[piece(0, 2), piece(0, 1), PLAIN]);
    assert_eq!(offsets(&resolve(records, 3, 3).records), [1, 2]);
}

/// An offset the read skipped (a trim record) inside a batch cuts it.
#[test]
fn a_skipped_offset_inside_a_batch_cuts_it() {
    let mut records = run(0, &[piece(0, 3), piece(1, 3)]);
    records.push(at(3, PLAIN));
    assert_eq!(offsets(&resolve(records, 4, 4).records), [3]);
    // At the page's end: the read covered the offset and found no piece.
    let resolved = resolve(run(0, &[piece(0, 3), piece(1, 3)]), 3, 9);
    assert!(resolved.records.is_empty());
    assert!(resolved.pending.is_empty());
    assert_eq!(resolved.next, 3);
}

/// A batch that runs past the committed end is held back with everything
/// after it, and the read resumes at its first piece.
#[test]
fn an_undecided_batch_stops_the_read() {
    let records = run(0, &[PLAIN, piece(0, 3), piece(1, 3), piece(2, 3)]);
    let resolved = resolve(records.clone(), 4, 3);
    assert_eq!(offsets(&resolved.records), [0]);
    assert_eq!(offsets(&resolved.pending), [1, 2]);
    assert_eq!(resolved.next, 1);
    assert_eq!(bound(3, piece(1, 3)), 1);

    // The same when the page, not the commit, ends inside the batch.
    let resolved = resolve(records[..3].to_vec(), 3, 9);
    assert_eq!(offsets(&resolved.pending), [1, 2]);
    assert_eq!(resolved.next, 1);
}

/// A piece no batch can have is skipped and does not hold the read.
#[test]
fn an_impossible_piece_is_skipped() {
    let records = run(0, &[piece(3, 3), piece(0, 0), PLAIN]);
    let resolved = resolve(records, 3, 3);
    assert_eq!(offsets(&resolved.records), [2]);
    assert_eq!(resolved.next, 3);
    assert_eq!(bound(1, piece(3, 3)), 1);
    assert_eq!(bound(1, piece(0, 0)), 1);
}

/// What the generated log holds at an offset.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Truth {
    Plain,
    Skipped,
    /// A piece of batch `id`, and whether the batch is whole.
    Piece {
        id: usize,
        whole: bool,
    },
    /// A trim, which `read_range` leaves out.
    Trim,
}

/// A log shaped the way Raft writes one: batches as consecutive runs, a cut
/// batch a strict prefix followed by something that does not continue it.
fn generate(seed: u64, len: usize) -> Vec<(RecordMark, Truth)> {
    let mut state = seed | 1;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut log = Vec::new();
    let mut id = 0;
    while log.len() < len {
        match next(6) {
            0 => log.push((BLANK, Truth::Skipped)),
            1 => log.push((PLAIN, Truth::Plain)),
            2 => log.push((RecordMark::Trim { floor: 0 }, Truth::Trim)),
            _ => {
                let n = 1 + next(4) as u32;
                let whole = n == 1 || next(2) == 0;
                let kept = if whole {
                    n
                } else {
                    1 + next(u64::from(n - 1)) as u32
                };
                id += 1;
                for position in 0..kept {
                    log.push((piece(position, n), Truth::Piece { id, whole }));
                }
                if !whole {
                    log.push((BLANK, Truth::Skipped));
                }
            }
        }
    }
    log
}

/// What `read_range` returns of the generated log: every record but trims.
fn read_of(log: &[(RecordMark, Truth)]) -> Vec<LogRecord> {
    log.iter()
        .zip(0..)
        .filter(|((_, truth), _)| *truth != Truth::Trim)
        .map(|((mark, _), offset)| at(offset, *mark))
        .collect()
}

/// For every committed end of many generated logs: a reader sees only whole
/// batches, all of each, and every record below the bound; the bound never
/// falls; and what is seen below it never changes as the commit grows. The
/// same rules as `WholeBatchesOnly` and `ReadsStable` in
/// `docs/formal/FelixRaftCond.tla`.
#[test]
fn readers_see_whole_batches_and_reads_stay_stable() {
    for seed in 1..400u64 {
        let log = generate(seed, 30);
        let records = read_of(&log);
        let tail = log.len() as Offset;
        let mut before: Option<(Offset, BTreeSet<Offset>)> = None;
        for end in 0..=tail {
            let resolved = resolve(records.clone(), tail, end);
            let read_bound = resolved.next;
            let seen: BTreeSet<Offset> = offsets(&resolved.records).into_iter().collect();
            let last = end.checked_sub(1).map_or(PLAIN, |at| log[at as usize].0);
            let last = if matches!(last, RecordMark::Trim { .. }) {
                PLAIN
            } else {
                last
            };
            assert_eq!(bound(end, last), read_bound, "seed {seed} end {end}");

            let mut batches: BTreeMap<usize, Vec<Offset>> = BTreeMap::new();
            for (offset, (_, truth)) in (0..).zip(&log) {
                if let Truth::Piece { id, .. } = truth {
                    batches.entry(*id).or_default().push(offset);
                }
            }
            for (offset, (_, truth)) in (0..end).zip(&log) {
                let below = offset < read_bound;
                let expected = match truth {
                    Truth::Plain => below,
                    Truth::Skipped => below,
                    Truth::Trim => false,
                    Truth::Piece { whole: false, .. } => false,
                    Truth::Piece { id, whole: true } => {
                        below && batches[id].iter().all(|piece| *piece < read_bound)
                    }
                };
                assert_eq!(
                    seen.contains(&offset),
                    expected,
                    "seed {seed} end {end} offset {offset} {truth:?}"
                );
            }
            assert!(seen.iter().all(|offset| *offset < read_bound));

            if let Some((bound_before, seen_before)) = &before {
                assert!(read_bound >= *bound_before, "seed {seed} end {end}");
                let still: BTreeSet<Offset> = seen.range(..*bound_before).copied().collect();
                assert_eq!(&still, seen_before, "seed {seed} end {end}");
            }
            before = Some((read_bound, seen));
        }
    }
}

fn append_record(mark: RecordMark, offset: usize) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from(offset.to_string()),
        timestamp_micros: 1,
        mark,
        publisher: None,
        headers: None,
    }
}

/// Read from a real log a page at a time, each page a single record plus
/// the batch it ends inside, the reader sees what one whole read sees, and
/// stops at an undecided batch at the commit.
#[tokio::test]
async fn paged_reads_from_a_log_match_one_whole_read() {
    for seed in [3u64, 17, 29, 101] {
        let dir = tempdir().expect("dir");
        let log = DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            LogConfig {
                segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 400,
                fsync_mode: FsyncMode::OnCommit,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("open");
        // Trims are left out: appending one applies its floor.
        let generated: Vec<_> = generate(seed, 40)
            .into_iter()
            .filter(|(_, truth)| *truth != Truth::Trim)
            .collect();
        let batch: Vec<AppendRecord> = generated
            .iter()
            .enumerate()
            .map(|(offset, (mark, _))| append_record(*mark, offset))
            .collect();
        log.append_v9_for_tests(&batch, 1).await.expect("append");
        let tail = batch.len() as Offset;
        let records = read_of(&generated);

        for end in [tail, tail - 1, tail / 2] {
            let whole = resolve(records.clone(), tail, end);
            let mut seen = Vec::new();
            let mut start = 0;
            loop {
                let page = read_range(
                    &log,
                    ReadRange {
                        start,
                        max_bytes: 1,
                    },
                    end,
                )
                .await
                .expect("read");
                seen.extend(offsets(&page.records));
                if page.next <= start {
                    break;
                }
                start = page.next;
            }
            assert_eq!(seen, offsets(&whole.records), "seed {seed} end {end}");
            assert_eq!(start, whole.next, "seed {seed} end {end}");
            assert_eq!(
                read_bound(&log, end).await.expect("bound"),
                whole.next,
                "seed {seed} end {end}"
            );
        }
    }
}

/// A page that ends inside a batch reads on to that batch's end and no
/// further, even when the next page would also end inside one.
#[tokio::test]
async fn a_page_grows_by_at_most_the_batch_it_ends_inside() {
    let dir = tempdir().expect("dir");
    let log = DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("open");
    let batch: Vec<AppendRecord> = (0..12)
        .map(|offset| AppendRecord {
            payload: Bytes::from_static(b"0123456789"),
            ..append_record(piece(offset % 3, 3), offset as usize)
        })
        .collect();
    log.append_v9_for_tests(&batch, 1).await.expect("append");
    let page = read_range(
        &log,
        ReadRange {
            start: 0,
            max_bytes: 40,
        },
        12,
    )
    .await
    .expect("read");
    assert_eq!(offsets(&page.records), [0, 1, 2, 3, 4, 5]);
    assert_eq!(page.next, 6);
}
