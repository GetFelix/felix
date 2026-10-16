use super::*;

#[test]
fn an_unregistered_stream_opens_at_zero() {
    let incarnations = Incarnations::default();
    assert_eq!(incarnations.resolve("t", "n", "s"), Some(0));
}

#[test]
fn a_retired_stream_opens_nothing_until_set_again() {
    let incarnations = Incarnations::default();
    incarnations.set("t", "n", "s", 7);
    assert_eq!(incarnations.resolve("t", "n", "s"), Some(7));
    incarnations.retire("t", "n", "s");
    assert_eq!(incarnations.resolve("t", "n", "s"), None);
    incarnations.set("t", "n", "s", 8);
    assert_eq!(incarnations.resolve("t", "n", "s"), Some(8));
}

#[test]
fn a_retired_directory_leaves_its_place_at_once() {
    let root = tempfile::tempdir().expect("dir");
    let dir = root.path().join("shard");
    std::fs::create_dir(&dir).expect("create");
    std::fs::write(dir.join("00000000000000000000.log"), b"records").expect("write");

    assert!(retire_dir(root.path(), &dir).expect("retire"));
    assert!(!dir.exists());
    // Nothing to do the second time.
    assert!(!retire_dir(root.path(), &dir).expect("retire again"));
}

#[test]
fn purging_empties_the_deleted_directory() {
    let root = tempfile::tempdir().expect("dir");
    let deleted = root.path().join(DELETED_DIR);
    std::fs::create_dir_all(deleted.join("leftover/inner")).expect("create");
    purge(&deleted);
    assert_eq!(std::fs::read_dir(&deleted).expect("read").count(), 0);
}

#[test]
fn a_marker_reads_back_as_written() {
    let root = tempfile::tempdir().expect("dir");
    assert_eq!(
        marker::read(root.path(), "t", "n", "s").expect("read"),
        MarkerRead::Absent
    );
    let mut written = StreamMarker {
        tenant: "t".into(),
        namespace: "n\nwith a line break".into(),
        stream: "s".into(),
        incarnation: 42,
        retiring: false,
        shards: None,
        deleted: None,
    };
    marker::write(root.path(), &written).expect("write");
    assert_eq!(
        marker::read(root.path(), "t", "n\nwith a line break", "s").expect("read"),
        MarkerRead::Valid(written.clone())
    );
    assert!(marker::retiring(root.path()).expect("list").is_empty());

    written.retiring = true;
    marker::write(root.path(), &written).expect("rewrite");
    assert_eq!(
        marker::retiring(root.path()).expect("list"),
        vec![written.clone()]
    );

    let deleted = StreamMarker {
        incarnation: 0,
        retiring: false,
        shards: Some(3),
        deleted: Some(17),
        ..written
    };
    marker::write(root.path(), &deleted).expect("record the delete");
    assert_eq!(marker::all(root.path()).expect("list"), vec![deleted]);
    assert!(marker::retiring(root.path()).expect("list").is_empty());
    marker::remove(root.path(), "t", "n\nwith a line break", "s").expect("remove");
    assert!(marker::retiring(root.path()).expect("list").is_empty());
}

#[test]
fn retiring_a_stream_takes_every_shard_of_that_incarnation_only() {
    let root = tempfile::tempdir().expect("dir");
    let key = |stream: &str, shard| crate::log::ShardKey {
        tenant: "t".into(),
        namespace: "n".into(),
        stream: stream.into(),
        shard,
    };
    let make = |stream, shard, incarnation| {
        let dir = layout::incarnation_dir(root.path(), &key(stream, shard), incarnation);
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    };
    let old = [make("s", 0, 0), make("s", 7, 0)];
    let kept = [make("s", 0, 9), make("s2", 0, 0)];

    let index = DirIndex::default();
    index
        .retire_stream(root.path(), "t", "n", "s", 0)
        .expect("retire");
    assert!(old.iter().all(|dir| !dir.exists()));
    assert!(kept.iter().all(|dir| dir.exists()));
    // Again, with nothing left to do.
    index
        .retire_stream(root.path(), "t", "n", "s", 0)
        .expect("retire again");
}

#[test]
fn a_damaged_marker_reads_as_unreadable_and_is_never_taken_as_a_delete() {
    let root = tempfile::tempdir().expect("dir");
    let marker = StreamMarker {
        tenant: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
        incarnation: 42,
        retiring: true,
        shards: None,
        deleted: None,
    };
    marker::write(root.path(), &marker).expect("write");
    let path = marker::path(root.path(), "t", "n", "s");
    for damage in [
        &b""[..],
        &b"\0\0\0\0"[..],
        &b"felix-stream-marker 1\nincarn"[..],
    ] {
        std::fs::write(&path, damage).expect("damage");
        assert_eq!(
            marker::read(root.path(), "t", "n", "s").expect("read"),
            MarkerRead::Unreadable
        );
        assert!(marker::retiring(root.path()).expect("list").is_empty());
    }
}

/// A power loss at any point leaves the marker as it was or as it was being
/// rewritten, never unreadable, and a retired directory either whole where it
/// was or out of the way, never half deleted in its place.
#[cfg(target_os = "linux")]
#[test]
fn markers_and_retired_directories_survive_a_power_loss() {
    use crate::io::power_loss::{PowerLoss, Writeback};

    let root = tempfile::tempdir().expect("dir");
    let shard = root.path().join("shard");
    std::fs::create_dir(&shard).expect("mkdir");
    std::fs::write(shard.join("records"), b"records").expect("write");
    let observer = PowerLoss::install(root.path()).expect("install");

    let mut marker = StreamMarker {
        tenant: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
        incarnation: 5,
        retiring: false,
        shards: None,
        deleted: None,
    };
    marker::write(root.path(), &marker).expect("write");
    marker.retiring = true;
    marker::write(root.path(), &marker).expect("rewrite");
    retire_dir(root.path(), &shard).expect("retire");

    for seed in 0..64 {
        let image = tempfile::tempdir().expect("image");
        observer
            .crash(seed, Writeback::AnySubset, image.path())
            .expect("crash");
        match marker::read(image.path(), "t", "n", "s").expect("read") {
            MarkerRead::Valid(found) => assert_eq!(found.incarnation, 5, "seed {seed}"),
            other => panic!("seed {seed}: {other:?}"),
        }
        let in_place = image.path().join("shard");
        if in_place.exists() {
            assert_eq!(
                std::fs::read(in_place.join("records")).expect("records"),
                b"records",
                "seed {seed}"
            );
        }
    }
}

/// Live and peak purge workers per `.deleted/` directory.
static LIVE_WORKERS: LazyLock<parking_lot::Mutex<HashMap<PathBuf, (usize, usize)>>> =
    LazyLock::new(Default::default);

pub(super) struct LiveWorker(PathBuf);

impl LiveWorker {
    pub(super) fn enter(deleted: &Path) -> Self {
        let mut live = LIVE_WORKERS.lock();
        let (now, peak) = live.entry(deleted.to_path_buf()).or_default();
        *now += 1;
        *peak = (*peak).max(*now);
        Self(deleted.to_path_buf())
    }
}

impl Drop for LiveWorker {
    fn drop(&mut self) {
        if let Some((now, _)) = LIVE_WORKERS.lock().get_mut(&self.0) {
            *now -= 1;
        }
    }
}

#[test]
fn retiring_many_directories_runs_one_purge_worker() {
    let root = tempfile::tempdir().expect("dir");
    let root = root.path().to_path_buf();
    let dirs: Vec<PathBuf> = (0..200)
        .map(|n| {
            let dir = root.join(format!("shard-{n}"));
            std::fs::create_dir_all(dir.join("inner")).expect("create");
            std::fs::write(dir.join("inner/records"), b"records").expect("write");
            dir
        })
        .collect();

    std::thread::scope(|scope| {
        for chunk in dirs.chunks(25) {
            let root = &root;
            scope.spawn(move || {
                for dir in chunk {
                    assert!(retire_dir(root, dir).expect("retire"));
                }
            });
        }
    });

    let deleted = root.join(DELETED_DIR);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while PURGE_WORKERS.lock().contains_key(&deleted)
        || LIVE_WORKERS
            .lock()
            .get(&deleted)
            .is_some_and(|(live, _)| *live > 0)
        || std::fs::read_dir(&deleted).expect("read").count() > 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "retired directories left behind"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let (_, peak) = LIVE_WORKERS.lock()[&deleted];
    assert_eq!(peak, 1, "one purge worker at a time per root");
}
