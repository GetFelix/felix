//! The page-level crash model: it must lose only what a real power loss may,
//! and it must actually lose it.

use super::*;

fn bytes(len: usize, fill: u8) -> Vec<u8> {
    vec![fill; len]
}

#[test]
fn an_unchanged_file_comes_back_unchanged() {
    let synced = bytes(3 * PAGE + 17, 7);
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        for writeback in [Writeback::AnySubset, Writeback::InOrder] {
            assert_eq!(
                crash_contents(&synced, &synced, &mut rng, writeback),
                synced
            );
        }
    }
}

#[test]
fn flushed_bytes_of_an_appended_file_always_survive() {
    let synced = bytes(PAGE + 100, 1);
    let mut now = synced.clone();
    now.extend(bytes(3 * PAGE, 2));
    for seed in 0..256 {
        let mut rng = SplitMix64::new(seed);
        for writeback in [Writeback::AnySubset, Writeback::InOrder] {
            let after = crash_contents(&synced, &now, &mut rng, writeback);
            assert!(
                after.len() >= synced.len(),
                "seed {seed}: size went backwards"
            );
            assert_eq!(&after[..synced.len()], &synced[..], "seed {seed}");
            // Past the flushed bytes, every byte is either what was written or
            // a zero from a block that never made it.
            for (i, byte) in after.iter().enumerate().skip(synced.len()) {
                assert!(
                    *byte == now[i] || *byte == 0,
                    "seed {seed}: byte {i} invented"
                );
            }
        }
    }
}

/// A model that never loses anything would make every recovery test pass.
#[test]
fn unflushed_pages_are_really_lost_torn_and_zeroed() {
    let synced = bytes(PAGE, 1);
    let mut now = synced.clone();
    now.extend(bytes(4 * PAGE, 2));
    let (mut lost, mut kept, mut torn, mut short) = (0, 0, 0, 0);
    for seed in 0..512 {
        let mut rng = SplitMix64::new(seed);
        let after = crash_contents(&synced, &now, &mut rng, Writeback::AnySubset);
        if after.len() < now.len() {
            short += 1;
        }
        for page in 1..after.len().div_ceil(PAGE) {
            let got = page_of(&after, page);
            if got == [0u8; PAGE] {
                lost += 1;
            } else if got == page_of(&now, page) {
                kept += 1;
            } else {
                torn += 1;
            }
        }
    }
    assert!(lost > 0 && kept > 0 && torn > 0 && short > 0);
}

#[test]
fn in_order_writeback_never_leaves_a_hole_before_written_data() {
    let synced = bytes(10, 1);
    let mut now = synced.clone();
    now.extend(bytes(6 * PAGE, 2));
    for seed in 0..256 {
        let mut rng = SplitMix64::new(seed);
        let after = crash_contents(&synced, &now, &mut rng, Writeback::InOrder);
        let first_zero_page = (0..after.len().div_ceil(PAGE))
            .find(|page| page_of(&after, *page) != page_of(&now, *page));
        if let Some(from) = first_zero_page {
            for page in from + 1..after.len().div_ceil(PAGE) {
                assert_eq!(
                    page_of(&after, page),
                    [0u8; PAGE],
                    "seed {seed}: page {page} written after an unwritten one",
                );
            }
        }
    }
}

#[test]
fn an_unflushed_truncation_may_be_undone() {
    let synced = bytes(3 * PAGE, 4);
    let now = bytes(PAGE, 4);
    let lengths: std::collections::HashSet<usize> = (0..64)
        .map(|seed| {
            crash_contents(
                &synced,
                &now,
                &mut SplitMix64::new(seed),
                Writeback::AnySubset,
            )
            .len()
        })
        .collect();
    assert!(lengths.contains(&synced.len()) && lengths.contains(&now.len()));
}
