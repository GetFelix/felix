//! What the in-memory filesystem models: reads and writes like a file, and a
//! crash that keeps only what a flush made durable.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::fs::{self, File, OpenOptions};
use crate::io;
use crate::io::writeback::{PAGE, SplitMix64, Writeback};
use crate::sim_fs;

/// A registered root that unregisters on drop. The name and the process id
/// keep tests that run side by side apart.
struct Sim {
    root: PathBuf,
}

impl Sim {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-test-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ));
        sim_fs::register_root(&root);
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn crash(&self, seed: u64) {
        sim_fs::crash(&self.root, seed).expect("crash");
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.root);
    }
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn bytes(len: usize, fill: u8) -> Vec<u8> {
    vec![fill; len]
}

fn create(path: &Path) -> File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("create")
}

/// A file whose bytes are flushed, in a directory whose entry is flushed too.
fn durable_file(path: &Path, contents: &[u8]) {
    let file = create(path);
    io::write_all(&file, contents).expect("write");
    io::sync_data(&file).expect("sync");
    io::sync_dir(path.parent().expect("parent")).expect("sync dir");
}

fn names(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .expect("list")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

#[test]
fn a_file_reads_back_what_was_written() {
    let sim = Sim::new("rw");
    let path = sim.path("f");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("create");
    file.write_all(b"hello world").expect("write");
    io::write_at(&file, b"J", 0).expect("overwrite");
    file.seek(SeekFrom::Start(6)).expect("seek");
    let mut rest = String::new();
    file.read_to_string(&mut rest).expect("read");
    assert_eq!(rest, "world");
    assert_eq!(fs::read(&path).expect("read"), b"Jello world");
    assert_eq!(fs::metadata(&path).expect("stat").len(), 11);

    let mut buf = [0u8; 4];
    assert_eq!(io::read_at(&file, &mut buf, 8).expect("read_at"), 3);
    assert_eq!(&buf[..3], b"rld");
    assert_eq!(
        create_again(&path).kind(),
        std::io::ErrorKind::AlreadyExists
    );
}

fn create_again(path: &Path) -> std::io::Error {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect_err("exists")
}

#[test]
fn set_len_and_append_behave_like_a_file() {
    let sim = Sim::new("len");
    let path = sim.path("f");
    fs::write(&path, b"abcdef").expect("write");
    let file = OpenOptions::new().write(true).open(&path).expect("open");
    file.set_len(3).expect("shrink");
    assert_eq!(fs::read(&path).expect("read"), b"abc");
    file.set_len(5).expect("grow");
    assert_eq!(fs::read(&path).expect("read"), b"abc\0\0");

    let mut appender = OpenOptions::new().append(true).open(&path).expect("open");
    appender.write_all(b"xy").expect("append");
    assert_eq!(fs::read(&path).expect("read"), b"abc\0\0xy");
    assert!(
        fs::read(sim.path("missing")).is_err_and(|err| err.kind() == std::io::ErrorKind::NotFound)
    );
}

#[test]
fn preallocation_keeps_the_length() {
    let sim = Sim::new("prealloc");
    let path = sim.path("seg");
    let file = create(&path);
    io::preallocate(&file, 0, 1 << 20).expect("preallocate");
    assert_eq!(fs::metadata(&path).expect("stat").len(), 0);
    io::write_all(&file, b"abc").expect("write");
    assert_eq!(fs::metadata(&path).expect("stat").len(), 3);
}

#[test]
fn listing_order_does_not_depend_on_creation_order() {
    let sim = Sim::new("order");
    for name in ["zeta", "alpha", "mid", "beta", "00001", "10000"] {
        fs::write(sim.path(name), name).expect("write");
    }
    let first = names(&sim.root);
    assert_eq!(first, ["00001", "10000", "alpha", "beta", "mid", "zeta"]);
    assert_eq!(first, names(&sim.root));
}

#[test]
fn synced_data_survives_a_crash() {
    for seed in 0..64 {
        let sim = Sim::new("synced");
        let dir = sim.path("log");
        io::create_dir_all_durable(&dir).expect("dir");
        durable_file(&dir.join("seg"), &bytes(3 * PAGE + 17, 7));
        sim.crash(seed);
        assert_eq!(
            fs::read(dir.join("seg")).expect("survives"),
            bytes(3 * PAGE + 17, 7),
            "seed {seed}"
        );
    }
}

#[test]
fn a_crash_with_everything_flushed_loses_nothing() {
    let sim = Sim::new("all");
    fs::create_dir_all(sim.path("a/b")).expect("dirs");
    fs::write(sim.path("a/one"), bytes(10_000, 1)).expect("write");
    fs::write(sim.path("a/b/two"), bytes(5, 2)).expect("write");
    fs::rename(sim.path("a/one"), sim.path("a/b/moved")).expect("rename");
    fs::write(sim.path("gone"), b"x").expect("write");
    fs::remove_file(sim.path("gone")).expect("unlink");
    io::sync_filesystems(&sim.root).expect("syncfs");
    for seed in 0..32 {
        sim.crash(seed);
        assert_eq!(names(&sim.root), ["a"], "seed {seed}");
        assert_eq!(names(&sim.path("a")), ["b"], "seed {seed}");
        assert_eq!(names(&sim.path("a/b")), ["moved", "two"], "seed {seed}");
        assert_eq!(
            fs::read(sim.path("a/b/moved")).expect("moved"),
            bytes(10_000, 1)
        );
        assert_eq!(fs::read(sim.path("a/b/two")).expect("two"), bytes(5, 2));
    }
}

/// A model that never loses anything would make every recovery test pass.
#[test]
fn unsynced_bytes_can_be_lost_or_torn() {
    let (mut lost, mut kept, mut torn) = (0, 0, 0);
    for seed in 0..256 {
        let sim = Sim::new("torn");
        let path = sim.path("seg");
        durable_file(&path, &bytes(PAGE, 1));
        let file = OpenOptions::new().write(true).open(&path).expect("open");
        io::write_at(&file, &bytes(3 * PAGE, 2), PAGE as u64).expect("write");
        sim.crash(seed);
        let after = fs::read(&path).expect("read");
        assert_eq!(
            &after[..PAGE],
            &bytes(PAGE, 1)[..],
            "seed {seed}: synced page"
        );
        for page in after[PAGE..].chunks(PAGE) {
            if page.iter().all(|byte| *byte == 0) {
                lost += 1;
            } else if page.iter().all(|byte| *byte == 2) && page.len() == PAGE {
                kept += 1;
            } else {
                torn += 1;
                assert!(
                    page.iter().all(|byte| *byte == 2 || *byte == 0),
                    "seed {seed}: bytes that were never written"
                );
            }
        }
    }
    assert!(lost > 0 && kept > 0 && torn > 0, "{lost} {kept} {torn}");
}

#[test]
fn the_length_can_reach_the_disk_without_the_data() {
    let mut lengths = std::collections::BTreeSet::new();
    for seed in 0..128 {
        let sim = Sim::new("size");
        let path = sim.path("seg");
        durable_file(&path, b"head");
        let file = OpenOptions::new().write(true).open(&path).expect("open");
        io::write_at(&file, &bytes(2 * PAGE, 9), 4).expect("write");
        sim.crash(seed);
        let after = fs::read(&path).expect("read");
        assert_eq!(&after[..4], b"head");
        lengths.insert(after.len());
    }
    assert!(lengths.contains(&4) && lengths.contains(&(4 + 2 * PAGE)));
    assert!(lengths.len() > 2, "no partial length: {lengths:?}");
}

#[test]
fn an_unsynced_rename_comes_back_whole_or_not_at_all() {
    let (mut reverted, mut stuck) = (0, 0);
    for seed in 0..128 {
        let sim = Sim::new("rename");
        durable_file(&sim.path("state.tmp"), b"new");
        fs::rename(sim.path("state.tmp"), sim.path("state")).expect("rename");
        sim.crash(seed);
        // One name, never both and never neither.
        match names(&sim.root).as_slice() {
            [only] if only == "state.tmp" => reverted += 1,
            [only] if only == "state" => {
                stuck += 1;
                assert_eq!(fs::read(sim.path("state")).expect("read"), b"new");
            }
            other => panic!("seed {seed}: {other:?}"),
        }
    }
    assert!(reverted > 0 && stuck > 0, "{reverted} {stuck}");
}

#[test]
fn a_rename_between_directories_is_still_one_change() {
    let (mut moved, mut stayed) = (0, 0);
    for seed in 0..128 {
        let sim = Sim::new("rename-dirs");
        fs::create_dir_all(sim.path("old")).expect("dir");
        fs::create_dir_all(sim.path("new")).expect("dir");
        durable_file(&sim.path("old/f"), b"x");
        io::sync_dir(&sim.root).expect("sync");
        io::sync_dir(&sim.path("new")).expect("sync");
        fs::rename(sim.path("old/f"), sim.path("new/f")).expect("rename");
        sim.crash(seed);
        match (names(&sim.path("old")).len(), names(&sim.path("new")).len()) {
            (1, 0) => stayed += 1,
            (0, 1) => moved += 1,
            other => panic!("seed {seed}: {other:?}"),
        }
    }
    assert!(moved > 0 && stayed > 0, "{moved} {stayed}");
}

#[test]
fn chained_renames_come_back_as_a_prefix() {
    let mut seen = std::collections::BTreeSet::new();
    for seed in 0..128 {
        let sim = Sim::new("rename-chain");
        durable_file(&sim.path("a"), b"x");
        fs::rename(sim.path("a"), sim.path("b")).expect("rename");
        fs::rename(sim.path("b"), sim.path("c")).expect("rename");
        sim.crash(seed);
        let mut after = names(&sim.root);
        assert_eq!(after.len(), 1, "seed {seed}: {after:?}");
        seen.insert(after.remove(0));
    }
    assert_eq!(seen.len(), 3, "{seen:?}");
}

#[test]
fn a_rename_over_a_file_leaves_the_old_or_the_new_one() {
    let (mut old, mut new) = (0, 0);
    for seed in 0..128 {
        let sim = Sim::new("rename-over");
        durable_file(&sim.path("state"), b"old");
        durable_file(&sim.path("state.tmp"), b"new");
        fs::rename(sim.path("state.tmp"), sim.path("state")).expect("rename");
        sim.crash(seed);
        match fs::read(sim.path("state"))
            .expect("state is never missing")
            .as_slice()
        {
            b"old" => {
                old += 1;
                assert_eq!(names(&sim.root), ["state", "state.tmp"], "seed {seed}");
            }
            b"new" => {
                new += 1;
                assert_eq!(names(&sim.root), ["state"], "seed {seed}");
            }
            other => panic!("seed {seed}: {other:?}"),
        }
    }
    assert!(old > 0 && new > 0, "{old} {new}");
}

#[test]
fn a_path_cannot_climb_out_of_a_registered_root() {
    let sim = Sim::new("dotdot");
    fs::create_dir_all(sim.path("a")).expect("dir");
    fs::write(sim.path("a/../f"), b"inside").expect("resolves inside the root");
    assert_eq!(fs::read(sim.path("f")).expect("read"), b"inside");
    for escape in ["../outside", "a/../../outside"] {
        let err = fs::write(sim.path(escape), b"x").expect_err("escapes");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{escape}");
    }
}

#[test]
fn a_rename_followed_by_a_directory_sync_sticks() {
    for seed in 0..128 {
        let sim = Sim::new("renamed");
        durable_file(&sim.path("state.tmp"), b"new");
        fs::rename(sim.path("state.tmp"), sim.path("state")).expect("rename");
        io::sync_dir(&sim.root).expect("sync dir");
        sim.crash(seed);
        assert_eq!(names(&sim.root), ["state"], "seed {seed}");
        assert_eq!(fs::read(sim.path("state")).expect("read"), b"new");
    }
}

#[test]
fn an_unlink_without_a_directory_sync_can_come_back() {
    let (mut back, mut gone) = (0, 0);
    for seed in 0..64 {
        let sim = Sim::new("unlink");
        durable_file(&sim.path("old"), b"data");
        fs::remove_file(sim.path("old")).expect("unlink");
        sim.crash(seed);
        if fs::exists(sim.path("old")) {
            back += 1;
            assert_eq!(fs::read(sim.path("old")).expect("read"), b"data");
        } else {
            gone += 1;
        }
    }
    assert!(back > 0 && gone > 0, "{back} {gone}");
}

#[test]
fn a_file_created_but_never_synced_may_not_exist() {
    let (mut present, mut absent) = (0, 0);
    for seed in 0..64 {
        let sim = Sim::new("fresh");
        let file = create(&sim.path("new"));
        io::write_all(&file, b"unsynced").expect("write");
        sim.crash(seed);
        if fs::exists(sim.path("new")) {
            present += 1;
        } else {
            absent += 1;
        }
    }
    assert!(present > 0 && absent > 0, "{present} {absent}");
}

#[test]
fn a_crash_is_the_same_for_the_same_seed() {
    let run = |seed| {
        let sim = Sim::new("replay");
        durable_file(&sim.path("seg"), &bytes(PAGE, 1));
        let file = OpenOptions::new()
            .write(true)
            .open(sim.path("seg"))
            .expect("open");
        io::write_at(&file, &bytes(4 * PAGE, 2), PAGE as u64).expect("write");
        fs::write(sim.path("other"), b"x").expect("write");
        sim.crash(seed);
        (names(&sim.root), fs::read(sim.path("seg")).ok())
    };
    for seed in 0..16 {
        assert_eq!(run(seed), run(seed));
    }
}

#[test]
fn removing_a_directory_unlinks_each_entry() {
    let (mut whole, mut partial) = (0, 0);
    for seed in 0..64 {
        let sim = Sim::new("rmdir");
        fs::create_dir_all(sim.path("shard")).expect("dir");
        durable_file(&sim.path("shard/a"), b"a");
        durable_file(&sim.path("shard/b"), b"b");
        io::sync_dir(&sim.root).expect("sync");
        fs::remove_dir_all(sim.path("shard")).expect("remove");
        assert!(!fs::exists(sim.path("shard")));
        sim.crash(seed);
        // The directory itself may be gone, with or without its files.
        let left = fs::exists(sim.path("shard")).then(|| names(&sim.path("shard")).len());
        match left {
            None | Some(0) | Some(2) => whole += 1,
            _ => partial += 1,
        }
    }
    assert!(partial > 0 && whole > 0, "{partial} {whole}");
}

#[test]
fn paths_outside_a_registered_root_stay_on_disk() {
    let sim = Sim::new("route");
    let disk = tempfile::tempdir().expect("dir");
    fs::write(disk.path().join("real"), b"on disk").expect("write");
    assert_eq!(
        std::fs::read(disk.path().join("real")).expect("disk"),
        b"on disk"
    );
    assert!(!fs::exists(sim.path("real")));
    assert!(
        std::fs::read_dir(&sim.root).is_err(),
        "nothing was made on disk"
    );
}

#[test]
fn a_rename_between_memory_and_disk_is_refused() {
    let sim = Sim::new("cross");
    let disk = tempfile::tempdir().expect("dir");
    fs::write(sim.path("a"), b"x").expect("write");
    assert!(fs::rename(sim.path("a"), disk.path().join("a")).is_err());
}

#[test]
fn a_flush_copies_only_what_changed_and_still_matches_the_file() {
    // Random writes, truncations and flushes against a plain buffer: after
    // every flush the durable bytes must equal the file.
    let sim = Sim::new("incremental");
    let path = sim.path("f");
    let file = create(&path);
    let mut rng = SplitMix64::new(11);
    for step in 0..400 {
        match rng.below(4) {
            0 | 1 => {
                let at = rng.below(3 * PAGE as u64);
                let len = 1 + rng.below(700) as usize;
                io::write_at(&file, &bytes(len, step as u8), at).expect("write");
            }
            2 => file.set_len(rng.below(3 * PAGE as u64)).expect("set_len"),
            _ => {
                io::sync_data(&file).expect("sync");
                let node = file.as_mem().expect("in memory");
                let data = node.node().data.lock();
                assert_eq!(data.durable, data.current, "step {step}");
            }
        }
    }
}

#[test]
fn crash_replaces_the_tree_and_orphans_old_handles() {
    let sim = Sim::new("orphan");
    durable_file(&sim.path("f"), b"kept");
    let stale = OpenOptions::new()
        .write(true)
        .open(sim.path("f"))
        .expect("open");
    sim.crash(1);
    io::write_all(&stale, b"lost").expect("the old handle still writes");
    assert_eq!(fs::read(sim.path("f")).expect("read"), b"kept");
}

#[test]
fn in_order_writeback_never_leaves_a_hole() {
    for seed in 0..128 {
        let sim = Sim::new("inorder");
        durable_file(&sim.path("seg"), &bytes(PAGE, 1));
        let file = OpenOptions::new()
            .write(true)
            .open(sim.path("seg"))
            .expect("open");
        io::write_at(&file, &bytes(5 * PAGE, 2), PAGE as u64).expect("write");
        let tree = crate::fs::tree_at(&sim.root).expect("tree");
        tree.crash(seed, Writeback::InOrder);
        let after = fs::read(sim.path("seg")).expect("read");
        let mut after_the_cut = false;
        for page in after[PAGE..].chunks(PAGE) {
            let written = page.len() == PAGE && page.iter().all(|byte| *byte == 2);
            if after_the_cut {
                assert!(
                    page.iter().all(|byte| *byte == 0),
                    "seed {seed}: data after a hole"
                );
            } else if !written {
                after_the_cut = true;
            }
        }
    }
}
