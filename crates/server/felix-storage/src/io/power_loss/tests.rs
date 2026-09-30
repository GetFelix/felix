//! The crash model itself: it must lose only what a real power loss may, and
//! it must actually lose it.

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

/// The tree-level half: a file flushed and its directory flushed comes back;
/// one never flushed may come back empty or not at all.
#[test]
fn directory_entries_and_contents_follow_their_flushes() {
    let root = tempfile::tempdir().expect("dir");
    let observer = PowerLoss::install(root.path()).expect("install");

    let flushed = root.path().join("flushed");
    let file = File::create(&flushed).expect("create");
    std::io::Write::write_all(&mut &file, b"durable").expect("write");
    crate::io::sync_data(&file).expect("sync");
    crate::io::sync_dir(root.path()).expect("sync dir");
    std::io::Write::write_all(&mut &file, b" and not").expect("write more");

    std::fs::write(root.path().join("never-flushed"), b"volatile").expect("write");
    assert!(observer.counts().data >= 1 && observer.counts().dir >= 1);

    let mut saw_missing = false;
    for seed in 0..64 {
        let image = tempfile::tempdir().expect("image");
        observer
            .crash(seed, Writeback::AnySubset, image.path())
            .expect("crash");
        let after = std::fs::read(image.path().join("flushed")).expect("flushed file survives");
        assert!(after.starts_with(b"durable"), "seed {seed}: {after:?}");
        match std::fs::read(image.path().join("never-flushed")) {
            Ok(contents) => assert!(
                contents.len() <= 8
                    && contents
                        .iter()
                        .zip(b"volatile")
                        .all(|(got, wrote)| got == wrote || *got == 0),
                "seed {seed}: {contents:?}",
            ),
            Err(_) => saw_missing = true,
        }
    }
    assert!(saw_missing, "an unflushed create never went missing");
}

/// A large file is read from the page its last capture ended in, so a change
/// before that page is not seen, while one that shrank is read whole again.
/// Small files are always read whole, since they may be rewritten in place.
#[test]
fn a_growing_large_file_is_captured_from_where_the_last_capture_ended() {
    let root = tempfile::tempdir().expect("dir");
    let observer = PowerLoss::install(root.path()).expect("install");
    let captured = |file: &File| {
        let meta = file.metadata().expect("stat");
        let inode = Inode {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        observer
            .state
            .lock()
            .files
            .get(&inode)
            .cloned()
            .expect("captured")
    };
    let rewrite_first_byte = |file: &File, byte: u8| {
        std::os::unix::fs::FileExt::write_at(file, &[byte], 0).expect("rewrite");
    };

    let large = File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(root.path().join("segment"))
        .expect("create");
    let first = bytes(WHOLE_FILE_UP_TO as usize + 100, 1);
    std::io::Write::write_all(&mut &large, &first).expect("write");
    crate::io::sync_data(&large).expect("sync");
    assert_eq!(captured(&large), first);

    rewrite_first_byte(&large, 9);
    std::io::Write::write_all(&mut &large, &bytes(PAGE, 2)).expect("append");
    crate::io::sync_data(&large).expect("sync");
    let after = captured(&large);
    assert_eq!(after.len(), first.len() + PAGE);
    assert_eq!(after[0], 1, "the page before the last capture was re-read");
    assert_eq!(&after[first.len()..], &bytes(PAGE, 2)[..]);

    large.set_len(10).expect("shrink");
    crate::io::sync_data(&large).expect("sync");
    assert_eq!(captured(&large), [9, 1, 1, 1, 1, 1, 1, 1, 1, 1]);

    let small = File::create(root.path().join("index")).expect("create");
    std::io::Write::write_all(&mut &small, &bytes(PAGE, 1)).expect("write");
    crate::io::sync_data(&small).expect("sync");
    rewrite_first_byte(&small, 9);
    crate::io::sync_data(&small).expect("sync");
    assert_eq!(captured(&small)[0], 9);
}

#[test]
fn the_power_loss_directive_needs_a_seed_and_a_directory() {
    use super::trigger::directive;
    assert_eq!(
        directive("fsync_delay_ms=0\npower_loss=7\npower_loss_into=/tmp/image\n"),
        Some((7, "/tmp/image".into()))
    );
    assert_eq!(directive("power_loss=7\n"), None);
    assert_eq!(
        directive("power_loss=x\npower_loss_into=/tmp/image\n"),
        None
    );
    assert_eq!(directive("power_loss_into=/tmp/image\n"), None);
}
