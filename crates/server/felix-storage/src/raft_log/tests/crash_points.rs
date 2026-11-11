//! Power loss around each write of the Raft log, on the simulator's
//! filesystem.
//!
//! An append is cut before its flush, inside the write, and after its
//! callback; a truncation before it starts, after each unlink, and after it
//! returns. What reopens must hold every entry whose callback fired, be a
//! gap-free prefix of what was written with each entry's term and kind
//! intact, never be shorter than a truncation was asked to keep, and take
//! the next append. The vote's crash points are in
//! `disk_log/tests/vote_crash_points.rs`.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use std::path::PathBuf;

use futures_util::FutureExt;
use openraft::storage::RaftLogStorage;

use super::writes::{append, append_flushed};
use super::*;
use crate::io::writeback::SplitMix64;
use crate::sim_fs;

const SEEDS: u64 = 8;

fn config() -> LogConfig {
    LogConfig {
        // A few pages per segment, so a cut lands inside and between them.
        segment_size_bytes: 2 * 4096 + 300,
        index_spacing_bytes: 512,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: true,
        raft_log: true,
        ..LogConfig::default()
    }
}

/// The entry at `index`: ten to a term, each term opening with a blank
/// entry and changing membership half way.
fn entry(index: u64) -> TestEntry {
    let term = 1 + index / 10;
    match index % 10 {
        0 => blank(term, index),
        5 => membership(term, index),
        _ => {
            let mut rng = SplitMix64::new(index.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let len = 1 + rng.below(700) as usize;
            let body: String = (0..len)
                .map(|_| char::from(b'a' + rng.below(26) as u8))
                .collect();
            normal(term, index, &format!("{index}:{body}"))
        }
    }
}

fn entries(range: std::ops::Range<u64>) -> Vec<TestEntry> {
    range.map(entry).collect()
}

struct Sim(PathBuf);

impl Sim {
    fn new(name: &str, seed: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-raft-{}-{name}-{seed}",
            std::process::id()
        ));
        sim_fs::register_root(&root);
        Self(root)
    }

    fn open(&self) -> RaftLog<Types, Codec> {
        let log = DiskLog::open(self.0.join("log"), "t/ns/raft/0", config()).expect("open");
        RaftLog::new(log, Codec).expect("a Raft log")
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// Open what the cut left. It must hold at least the first `kept` of
/// `written` and nothing that is not a prefix of it, then take an append and
/// reopen. Returns how many entries came back.
async fn recover(sim: &Sim, kept: usize, written: &[TestEntry], at: &str) -> usize {
    let mut raft = sim.open();
    let found = all(&raft).await.expect("read after the cut");
    assert!(found.len() >= kept, "{at}: {} of {kept} kept", found.len());
    assert!(
        found.len() <= written.len() && found == written[..found.len()],
        "{at}: not a prefix of what was written"
    );
    assert_eq!(
        raft.log_state().await.expect("state").last_log_id,
        found.last().map(|entry| entry.log_id),
        "{at}"
    );

    let next = normal(1_000, found.len() as u64, "after the cut");
    append_flushed(&mut raft, std::slice::from_ref(&next)).await;
    raft.disk_log().shutdown().await.expect("shutdown");
    drop(raft);
    let raft = sim.open();
    let reopened = all(&raft).await.expect("read");
    assert_eq!(reopened.last(), Some(&next), "{at}");
    assert_eq!(reopened[..found.len()], found, "{at}");
    raft.disk_log().shutdown().await.expect("shutdown");
    found.len()
}

/// The cut lands after the append returned and before its flush. The
/// callback has not fired, so the entries may go, but nothing flushed does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_before_the_flush_keeps_what_was_flushed() {
    for seed in 0..SEEDS {
        let sim = Sim::new("unflushed", seed);
        let mut raft = sim.open();
        let written = entries(0..26);
        append_flushed(&mut raft, &written[..20]).await;

        let log = raft.disk_log().clone();
        let held = log.hold_flushes().await;
        let mut flushed = append(&mut raft, &written[20..]).await.expect("append");
        assert_eq!(all(&raft).await.expect("read"), written);
        assert!(flushed.as_mut().now_or_never().is_none());
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(held);

        recover(&sim, 20, &written, &format!("seed {seed}")).await;
    }
}

/// The cut lands inside the write, tearing an entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_inside_an_append_is_repaired() {
    for seed in 0..SEEDS {
        let sim = Sim::new("torn", seed);
        let mut raft = sim.open();
        let written = entries(0..26);
        append_flushed(&mut raft, &written[..20]).await;

        sim_fs::arm_power_cut(&sim.0, seed, |_, _| {}).expect("arm");
        let _ = append(&mut raft, &written[20..]).await;
        assert!(
            sim_fs::reboot(&sim.0).expect("reboot"),
            "seed {seed}: no cut"
        );

        recover(&sim, 20, &written, &format!("seed {seed}")).await;
    }
}

/// The cut lands after the callback fired: every entry is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_after_the_callback_keeps_the_entries() {
    for seed in 0..SEEDS {
        let sim = Sim::new("flushed", seed);
        let mut raft = sim.open();
        let written = entries(0..26);
        append_flushed(&mut raft, &written[..20]).await;
        // One batch, so its callback is the only thing vouching for it.
        let flushed = append(&mut raft, &written[20..]).await.expect("append");
        flushed.await.expect("flush");
        sim_fs::crash(&sim.0, seed).expect("crash");

        let kept = recover(&sim, 26, &written, &format!("seed {seed}")).await;
        assert_eq!(kept, 26);
    }
}

/// A truncation stopped after each unlink in turn, then the power cut.
/// What reopens is never shorter than the truncation was asked to keep and
/// has no gap; once the truncation returned, it is exactly that long.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_at_each_step_of_a_truncation_leaves_no_gap() {
    const KEEP: u64 = 12;
    let written = entries(0..90);
    // Whole segments above the cut go one unlink each.
    let unlinks = {
        let sim = Sim::new("truncate-count", 0);
        let mut raft = sim.open();
        append_flushed(&mut raft, &written).await;
        let doomed = raft
            .disk_log()
            .segments()
            .iter()
            .filter(|segment| segment.base_offset > KEEP)
            .count();
        raft.disk_log().shutdown().await.expect("shutdown");
        doomed
    };
    assert!(unlinks >= 2, "only {unlinks} segments above the cut");

    // The last stop is past every unlink: the truncation runs to its end.
    for stop in 0..=unlinks {
        for seed in 0..SEEDS {
            let sim = Sim::new(&format!("truncate-{stop}"), seed);
            let mut raft = sim.open();
            append_flushed(&mut raft, &written).await;
            raft.disk_log().stop_after_unlinks(stop);
            let cut = raft.truncate_after(Some(entry(KEEP).log_id)).await;
            assert_eq!(cut.is_ok(), stop == unlinks, "stop {stop}");
            sim_fs::crash(&sim.0, seed).expect("crash");

            let at = format!("stopped after {stop} of {unlinks} unlinks, seed {seed}");
            let kept = recover(&sim, KEEP as usize + 1, &written, &at).await;
            if cut.is_ok() {
                assert_eq!(kept, KEEP as usize + 1, "{at}: cut entries came back");
            }
        }
    }
}

/// A truncation's cut is on disk when `truncate_after` returns, so the
/// callback of an append after it never fires ahead of the cut (#2221). The
/// power is cut right after the truncation, and again after a later
/// append's callback: none of the cut entries come back either time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_is_on_disk_before_a_later_appends_callback() {
    for seed in 0..SEEDS {
        for later_append in [false, true] {
            let sim = Sim::new(&format!("cut-first-{later_append}"), seed);
            let mut raft = sim.open();
            append_flushed(&mut raft, &entries(0..90)).await;
            raft.truncate_after(Some(entry(12).log_id))
                .await
                .expect("truncate");
            let mut written = entries(0..13);
            if later_append {
                written.extend([blank(9, 13), normal(9, 14, "new"), normal(9, 15, "newer")]);
                let flushed = append(&mut raft, &written[13..]).await.expect("append");
                flushed.await.expect("flush");
            }
            sim_fs::crash(&sim.0, seed).expect("crash");

            let at = format!("seed {seed}, later append {later_append}");
            let kept = recover(&sim, written.len(), &written, &at).await;
            assert_eq!(kept, written.len(), "{at}");
        }
    }
}

/// openraft saves a committed id before the entry it names is flushed here.
/// A power cut can then take the entry and keep the id. The id read back is
/// never past the log, and stays where it was lowered to: entries another
/// leader writes at the lost indexes are not committed, and can be cut
/// (#2221).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_id_never_reads_back_past_the_log() {
    let mut lost_some = false;
    for seed in 0..SEEDS {
        let sim = Sim::new("committed", seed);
        let mut raft = sim.open();
        let written = entries(0..26);
        append_flushed(&mut raft, &written[..20]).await;
        let log = raft.disk_log().clone();
        let held = log.hold_flushes().await;
        let _unflushed = append(&mut raft, &written[20..]).await.expect("append");
        raft.save_committed(Some(written[25].log_id))
            .await
            .expect("save");
        log.commit_offset_persisted().await;
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(held);

        let mut raft = sim.open();
        let found = all(&raft).await.expect("read after the cut");
        assert!(found.len() >= 20 && found == written[..found.len()]);
        lost_some |= found.len() < written.len();
        let last = found.last().map(|entry| entry.log_id);
        assert_eq!(
            raft.read_committed().await.expect("committed"),
            last,
            "seed {seed}: {} entries",
            found.len()
        );

        let from = found.len() as u64;
        let other: Vec<_> = (from..from + 8)
            .map(|index| normal(50, index, "another leader's"))
            .collect();
        append_flushed(&mut raft, &other).await;
        raft.truncate_after(last)
            .await
            .expect("entries past the committed id can be cut");
        raft.disk_log().shutdown().await.expect("shutdown");
        drop(raft);
        let mut raft = sim.open();
        assert_eq!(raft.read_committed().await.expect("committed"), last);
        assert_eq!(all(&raft).await.expect("read"), found);
        raft.disk_log().shutdown().await.expect("shutdown");
    }
    assert!(lost_some, "no seed lost an unflushed entry");
}

/// After a truncation returned and another term's entries took the place of
/// what it cut, a power cut brings none of the old entries back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_after_a_truncation_keeps_the_new_entries() {
    for seed in 0..SEEDS {
        let sim = Sim::new("truncated", seed);
        let mut raft = sim.open();
        append_flushed(&mut raft, &entries(0..90)).await;
        raft.truncate_after(Some(entry(12).log_id))
            .await
            .expect("truncate");
        let mut written = entries(0..13);
        written.extend([blank(9, 13), normal(9, 14, "new"), normal(9, 15, "newer")]);
        append_flushed(&mut raft, &written[13..]).await;
        sim_fs::crash(&sim.0, seed).expect("crash");

        let kept = recover(&sim, 16, &written, &format!("seed {seed}")).await;
        assert_eq!(kept, 16);
    }
}
