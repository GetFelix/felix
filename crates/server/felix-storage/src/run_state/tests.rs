//! A machine crash under a relaxed fsync mode rotates the storage id; a
//! process crash in the same boot, a clean stop, a crash under `OnCommit` and
//! an upgrade without a sign of a crash keep it.
use super::*;

fn counter(start: u8) -> impl FnMut() -> [u8; 16] {
    let mut next = start;
    move || {
        next += 1;
        [next; 16]
    }
}

const PERIODIC: bool = true;
const ON_COMMIT: bool = false;

thread_local! {
    /// Filesystem syncs `begin` asked for on this thread.
    pub(super) static SYNCS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Make those syncs fail.
    pub(super) static SYNC_FAILS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn syncs() -> u32 {
    SYNCS.with(std::cell::Cell::get)
}

const BOOT: Option<BootId> = Some(BootId::from_bytes([1; 16]));
const REBOOTED: Option<BootId> = Some(BootId::from_bytes([2; 16]));

fn no_repair() -> bool {
    false
}

fn begin_on(root: &Path, relaxed: bool, fresh: &mut impl FnMut() -> [u8; 16]) -> Begun {
    begin_in(root, relaxed, BOOT, fresh)
}

fn begin_in(
    root: &Path,
    relaxed: bool,
    boot: Option<BootId>,
    fresh: &mut impl FnMut() -> [u8; 16],
) -> Begun {
    begin(root, relaxed, boot, no_repair, fresh).expect("begin")
}

fn relaxed_crash(boot: Option<BootId>) -> LastRun {
    LastRun::Crashed {
        relaxed: true,
        boot,
    }
}

#[test]
fn a_new_directory_starts_unrecorded_and_running() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(begun.origin, Origin::Created);
    assert_eq!(begun.last_run, LastRun::Unrecorded);
    assert!(!begun.rotated);
    assert_eq!(
        read(dir.path(), begun.id).expect("read"),
        relaxed_crash(BOOT),
        "a run under way reads as a crash until it stops cleanly"
    );
}

/// **A machine crash under `Periodic` or `None` gives the directory a new id
/// (#1284).** The unsynced tail may be gone, so peers must not keep counting
/// it.
#[test]
fn a_crash_in_another_boot_rotates_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    // No `mark_clean`: the machine went down.
    let second = begin_in(dir.path(), PERIODIC, REBOOTED, &mut fresh);
    assert_eq!(second.last_run, relaxed_crash(BOOT));
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
    assert_eq!(
        storage_identity::load_or_create(dir.path(), &mut fresh).expect("load"),
        (second.id, Origin::Loaded),
        "the rotation is on disk"
    );
}

/// **A process crash keeps the id (#1367).** The machine is in the boot the
/// crashed run recorded, so its page cache kept every write that run made.
#[test]
fn a_crash_in_the_same_boot_keeps_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    let second = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    assert_eq!(second.last_run, relaxed_crash(BOOT));
    assert!(!second.rotated);
    assert_eq!(second.id, first.id);
}

/// **A kept id is synced first.** The crashed run's dirty pages, a retired
/// segment it never sealed among them, are on the device before the id is
/// announced, so a later clean stop vouches only for what is there. Nothing
/// else syncs.
#[test]
fn keeping_the_id_after_a_crash_syncs_the_filesystem_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    let before = syncs();
    let kept = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    assert_eq!(kept.id, first.id);
    assert_eq!(syncs(), before + 1, "the keep path syncs once");
    mark_clean(dir.path(), kept.id).expect("mark clean");
    begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    begin_in(dir.path(), PERIODIC, REBOOTED, &mut fresh);
    assert_eq!(syncs(), before + 1, "a clean stop or a rotation does not");
}

/// **A failed sync rotates.** It may be a writeback error the crashed run
/// never saw, so the page cache no longer vouches for it.
#[test]
fn a_failed_sync_after_a_crash_rotates_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    SYNC_FAILS.with(|fails| fails.set(true));
    let second = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    SYNC_FAILS.with(|fails| fails.set(false));
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
}

/// The real sync works on Linux, and anywhere else a crash cannot keep the
/// id: macOS's `sync` only queues the writes.
#[test]
fn the_filesystem_sync_runs_only_where_it_reaches_the_device() {
    let dir = tempfile::tempdir().expect("tempdir");
    let synced = crate::io::sync_filesystems(dir.path());
    assert_eq!(synced.is_ok(), cfg!(target_os = "linux"), "{synced:?}");
}

/// One device per filesystem: a tree on one mount syncs once, and a nested
/// directory or a symlink to one is found without walking it twice.
#[cfg(unix)]
#[test]
fn a_tree_on_one_filesystem_is_one_device() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("caches/shard-a")).expect("mkdir");
    std::fs::create_dir_all(dir.path().join("shard-b")).expect("mkdir");
    std::os::unix::fs::symlink(dir.path().join("shard-b"), dir.path().join("linked"))
        .expect("symlink");
    let devices = crate::io::devices_under(dir.path()).expect("walk");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices.values().next(), Some(&dir.path().to_path_buf()));
}

/// **An unknown boot on either side rotates.** Silence never vouches for a
/// crash: not a platform without a boot id, and not a record from a build
/// that wrote none.
#[test]
fn a_crash_with_an_unknown_boot_rotates_the_id() {
    for (recorded, now) in [(None, BOOT), (BOOT, None), (None, None)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let first = begin_in(dir.path(), PERIODIC, recorded, &mut fresh);
        let second = begin_in(dir.path(), PERIODIC, now, &mut fresh);
        assert_eq!(second.last_run, relaxed_crash(recorded));
        assert!(second.rotated, "recorded {recorded:?}, now {now:?}");
        assert_ne!(second.id, first.id);
    }
}

/// **A failed fsync rotates even in the same boot.** The kernel may have
/// dropped the pages it could not write, so the page cache no longer vouches
/// for the run.
#[test]
fn a_crash_after_the_boot_is_forgotten_rotates_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    forget_boot(dir.path()).expect("forget");
    let second = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    assert_eq!(second.last_run, relaxed_crash(None));
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
}

/// A poisoned log forgets the boot of every directory being watched.
#[test]
fn a_watched_directory_forgets_its_boot_on_poison() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    watch_poison(dir.path());
    forget_watched();
    assert_eq!(
        read(dir.path(), first.id).expect("read"),
        relaxed_crash(None)
    );
}

/// Forgetting the boot leaves an `OnCommit` run and a clean stop as they
/// were: neither depends on the page cache.
#[test]
fn forgetting_the_boot_leaves_other_records_alone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), ON_COMMIT, BOOT, &mut fresh);
    forget_boot(dir.path()).expect("forget");
    assert_eq!(
        read(dir.path(), first.id).expect("read"),
        LastRun::Crashed {
            relaxed: false,
            boot: None
        }
    );
    mark_clean(dir.path(), first.id).expect("mark clean");
    forget_boot(dir.path()).expect("forget");
    assert_eq!(read(dir.path(), first.id).expect("read"), LastRun::Clean);
}

/// **A downgrade after a clean stop keeps the id.** Only a relaxed run under
/// way names its boot; a clean stop and every other record keep the layout
/// older builds decode.
#[test]
fn only_a_relaxed_run_under_way_uses_the_newer_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(FILE_NAME);
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    assert_eq!(std::fs::read(&path).expect("read").len(), ENCODED_LEN_BOOT);
    mark_clean(dir.path(), first.id).expect("mark clean");
    assert_eq!(
        std::fs::read(&path).expect("read"),
        encode(STOPPED, 0, first.id),
        "a clean stop is the record older builds read"
    );
    for (relaxed, boot) in [(ON_COMMIT, BOOT), (PERIODIC, None)] {
        begin_in(dir.path(), relaxed, boot, &mut fresh);
        assert_eq!(std::fs::read(&path).expect("read").len(), ENCODED_LEN);
    }
}

#[test]
fn a_clean_stop_keeps_the_id_under_every_fsync_mode() {
    for relaxed in [PERIODIC, ON_COMMIT] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let first = begin_on(dir.path(), relaxed, &mut fresh);
        mark_clean(dir.path(), first.id).expect("mark clean");
        let second = begin_on(dir.path(), relaxed, &mut fresh);
        assert_eq!(second.last_run, LastRun::Clean);
        assert!(!second.rotated);
        assert_eq!(second.id, first.id, "relaxed: {relaxed}");
    }
}

/// Under `OnCommit` nothing acknowledged was unsynced, so a crash loses
/// nothing peers counted.
#[test]
fn a_crash_under_on_commit_keeps_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), ON_COMMIT, &mut fresh);
    let second = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(
        second.last_run,
        LastRun::Crashed {
            relaxed: false,
            boot: None
        }
    );
    assert_eq!(second.id, first.id);
}

/// The record names the mode of the run that crashed, not the next one's.
#[test]
fn a_periodic_crash_rotates_even_when_the_next_run_is_on_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_in(dir.path(), PERIODIC, BOOT, &mut fresh);
    let second = begin_in(dir.path(), ON_COMMIT, REBOOTED, &mut fresh);
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
}

/// Starting a run takes the clean record away, so a crash after it is seen.
#[test]
fn a_crash_after_a_clean_restart_is_still_seen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), PERIODIC, &mut fresh);
    mark_clean(dir.path(), first.id).expect("mark clean");
    let second = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(second.id, first.id);
    let third = begin_in(dir.path(), PERIODIC, REBOOTED, &mut fresh);
    assert!(third.rotated);
    assert_ne!(third.id, first.id);
}

/// The first start after an upgrade has an id and no record. It rotates only
/// when startup has something to repair, so a rolling upgrade does not make
/// every leader copy every follower again.
#[test]
fn an_upgrade_rotates_only_on_a_pending_repair() {
    for (pending, rotates) in [(false, false), (true, true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let (old, _) = storage_identity::load_or_create(dir.path(), &mut fresh).expect("create");
        let begun = begin(dir.path(), PERIODIC, BOOT, || pending, &mut fresh).expect("begin");
        assert_eq!(begun.last_run, LastRun::Unrecorded);
        assert_eq!(begun.rotated, rotates, "pending: {pending}");
        assert_eq!(begun.id != old, rotates);
    }
}

/// A new id needs no second one, and the repair check is not worth its reads.
#[test]
fn a_created_id_is_never_rotated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin(
        dir.path(),
        PERIODIC,
        BOOT,
        || panic!("asked about repairs for a new directory"),
        &mut fresh,
    )
    .expect("begin");
    assert!(!begun.rotated);
}

#[test]
fn a_damaged_or_foreign_record_reads_as_a_relaxed_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin_on(dir.path(), ON_COMMIT, &mut fresh);
    mark_clean(dir.path(), begun.id).expect("mark clean");
    let path = dir.path().join(FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    let mut flipped = good.clone();
    flipped[10] ^= 0xff;
    let mut unknown_state = good.clone();
    unknown_state[6] = 9;
    for damaged in [flipped, unknown_state, good[..20].to_vec(), Vec::new()] {
        std::fs::write(&path, &damaged).expect("damage");
        assert_eq!(
            read(dir.path(), begun.id).expect("read"),
            relaxed_crash(None)
        );
    }
    std::fs::write(&path, &good).expect("restore");
    assert_eq!(read(dir.path(), begun.id).expect("read"), LastRun::Clean);
    assert_eq!(
        read(dir.path(), StorageId::from_bytes([0xee; 16])).expect("read"),
        relaxed_crash(None),
        "a clean stop under another id says nothing about this one"
    );
}

#[test]
fn records_round_trip() {
    let id = StorageId::from_bytes([7; 16]);
    for (state, flags) in [(RUNNING, 0), (RUNNING, RELAXED), (STOPPED, 0)] {
        assert_eq!(
            decode(&encode(state, flags, id)),
            Some(Record {
                state,
                flags,
                id,
                boot: None
            })
        );
    }
    let boot = BootId::from_bytes([9; 16]);
    assert_eq!(
        decode(&encode_boot(id, boot)),
        Some(Record {
            state: RUNNING,
            flags: RELAXED,
            id,
            boot: Some(boot)
        })
    );
}

/// The newer layout names a boot only for a relaxed run under way, and its
/// checksum covers the boot.
#[test]
fn a_damaged_boot_record_does_not_decode() {
    let good = encode_boot(StorageId::from_bytes([7; 16]), BootId::from_bytes([9; 16]));
    let mut boot_flipped = good;
    boot_flipped[30] ^= 1;
    let mut stopped = good;
    stopped[6] = STOPPED;
    let crc = crate::segment::format::crc32(&[&stopped[..40]]);
    stopped[40..].copy_from_slice(&crc.to_be_bytes());
    let mut short_version = good;
    short_version[4..6].copy_from_slice(&VERSION.to_be_bytes());
    for damaged in [boot_flipped, stopped, short_version] {
        assert_eq!(decode(&damaged), None);
    }
}

#[test]
fn boot_ids_parse_from_the_kernels_text() {
    let parsed = BootId::parse("cf0068e9-9781-4491-a349-ad1a7355a83c\n").expect("parse");
    assert_eq!(
        Some(parsed),
        BootId::parse("CF0068E997814491A349AD1A7355A83C")
    );
    for bad in ["", "cf0068e9", "zz0068e9-9781-4491-a349-ad1a7355a83c"] {
        assert_eq!(BootId::parse(bad), None, "{bad:?}");
    }
}

/// Where the platform has one, the boot id reads and stays put.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn this_machine_has_a_boot_id() {
    let boot = this_boot().expect("a boot id");
    assert_eq!(this_boot(), Some(boot));
}
