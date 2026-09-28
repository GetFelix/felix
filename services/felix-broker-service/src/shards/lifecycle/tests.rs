//! Ownership transitions: what this broker serves, and when it stops.
use super::*;

mod promotion;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn assigned_to(leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(0),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
    }
}

fn lifecycle() -> ShardLifecycle {
    ShardLifecycle::new("broker-a")
}

/// The rule the whole type exists for: an assignment arriving does not mean the
/// shard can be served. The local log has to be open first, or a write would be
/// acknowledged against durability that is not set up.
#[test]
fn an_assignment_does_not_serve_until_the_log_is_open() {
    let mut own = lifecycle();

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 3,
            new_term: true,
            fence: false,
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)), "opening must not serve");

    assert_eq!(own.opened(&key(0), 3), Opened::Activated);
    assert_eq!(own.phase(&key(0)), Phase::Active);
    assert!(own.may_serve(&key(0)));
}

#[test]
fn a_shard_assigned_elsewhere_is_never_opened() {
    let mut own = lifecycle();
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-b", 1))),
        Action::None
    );
    assert_eq!(own.phase(&key(0)), Phase::Unassigned);
    assert!(!own.may_serve(&key(0)));
}

/// Every poll re-delivers the same assignment. Reopening on each would churn the
/// log and drop service.
#[test]
fn repeating_the_same_assignment_does_nothing() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    for _ in 0..5 {
        assert_eq!(
            own.observe(&key(0), Some(&assigned_to("broker-a", 3))),
            Action::None
        );
    }
    assert_eq!(own.phase(&key(0)), Phase::Active);
}

/// A generation below the one already acted on is a duplicate, a reordered
/// poll, or a snapshot replay -- never a reason to change state.
#[test]
fn an_older_generation_is_ignored() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 5)));
    own.opened(&key(0), 5);

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::None
    );
    assert_eq!(own.generation(&key(0)), Some(5));
    assert!(own.may_serve_at(&key(0), 5));
    assert!(!own.may_serve_at(&key(0), 4));
}

/// The shard was moved away and back. The local state has to be re-established,
/// not assumed, or this broker serves a generation it does not hold.
#[test]
fn a_newer_generation_reopens() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 4)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 4,
            new_term: false,
            fence: false,
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)), "must not serve while reopening");
}

/// Losing a shard drains before closing, so in-flight work is accounted for.
#[test]
fn losing_a_shard_drains_before_closing() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);

    let action = own.observe(&key(0), Some(&assigned_to("broker-b", 3)));
    assert_eq!(
        action,
        Action::Release {
            key: key(0),
            generation: 2
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Draining);
    assert!(
        !own.may_serve(&key(0)),
        "a draining shard takes no new writes"
    );

    own.released(&key(0), 2);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
    assert!(!own.may_serve(&key(0)));
}

/// A deleted assignment has to release the shard too, or a broker keeps serving
/// something the cluster no longer believes exists.
#[test]
fn an_unassigned_shard_is_released() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);

    let action = own.observe(&key(0), None);
    assert_eq!(
        action,
        Action::Release {
            key: key(0),
            generation: 2
        }
    );
    assert_eq!(own.phase(&key(0)), Phase::Draining);
}

#[test]
fn releasing_twice_is_harmless() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.observe(&key(0), None);
    own.released(&key(0), 2);

    assert_eq!(own.observe(&key(0), None), Action::None);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// A shard reassigned back to us after we released it opens again.
#[test]
fn a_closed_shard_can_be_reacquired() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    own.opened(&key(0), 2);
    own.observe(&key(0), None);
    own.released(&key(0), 2);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 6)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 6,
            new_term: true,
            fence: false,
        }
    );
    assert_eq!(own.opened(&key(0), 6), Opened::Activated);
    assert!(own.may_serve(&key(0)));
}

#[test]
fn a_failed_open_does_not_serve() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    assert_eq!(own.phase(&key(0)), Phase::Failed);
    assert!(!own.may_serve(&key(0)));
    // Not retried on every poll: the same assignment repeating is not new
    // information, and a failure that logs each tick buries itself.
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 1))),
        Action::None
    );
}

/// A changed assignment is the one thing that retries a failed open.
#[test]
fn a_new_generation_retries_a_failed_open() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    let action = own.observe(&key(0), Some(&assigned_to("broker-a", 2)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 2,
            new_term: true,
            fence: false,
        }
    );
}

/// A failed open never served, so losing the shard closes it without a drain.
#[test]
fn a_failed_shard_closes_without_draining() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.open_failed(&key(0), 1);

    assert_eq!(own.observe(&key(0), None), Action::None);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// The shard was reassigned while we were still recovering it. Activating now
/// would serve a generation we no longer hold.
#[test]
fn an_open_that_finishes_after_a_reassignment_does_not_activate() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    // Reassigned mid-open.
    own.observe(&key(0), Some(&assigned_to("broker-a", 2)));

    assert!(
        own.opened(&key(0), 1) == Opened::Stale,
        "the open for generation 1 must not activate generation 2",
    );
    assert_eq!(own.phase(&key(0)), Phase::Opening);
    assert!(!own.may_serve(&key(0)));

    assert_eq!(own.opened(&key(0), 2), Opened::Activated);
    assert!(own.may_serve_at(&key(0), 2));
}

/// A snapshot replaces the whole picture, so a shard absent from it has to be
/// released as surely as one reassigned away.
#[test]
fn shards_absent_from_a_snapshot_are_reported() {
    let mut own = lifecycle();
    for shard in 0..3 {
        let mut assignment = assigned_to("broker-a", 1);
        assignment.key = key(shard);
        own.observe(&key(shard), Some(&assignment));
        own.opened(&key(shard), 1);
    }

    let still_present = [key(0), key(2)];
    let mut missing = own.missing_from(still_present.iter());
    missing.sort_by_key(|k| k.shard);
    assert_eq!(missing, vec![key(1)]);
}

#[test]
fn a_closed_shard_is_not_reported_missing_again() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    own.observe(&key(0), None);
    own.released(&key(0), 1);

    assert!(own.missing_from(std::iter::empty()).is_empty());
}

#[test]
fn active_lists_only_servable_shards() {
    let mut own = lifecycle();
    let mut second = assigned_to("broker-a", 1);
    second.key = key(1);

    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    own.observe(&key(1), Some(&second)); // left Opening

    let active: Vec<u32> = own.active().map(|k| k.shard).collect();
    assert_eq!(active, vec![0]);
}

#[test]
fn phase_counts_track_the_shards_held() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);
    let mut second = assigned_to("broker-a", 1);
    second.key = key(1);
    own.observe(&key(1), Some(&second));

    let counts = own.counts();
    assert_eq!(counts.get(&Phase::Active).copied(), Some(1));
    assert_eq!(counts.get(&Phase::Opening).copied(), Some(1));
}

/// Driving the state machine against a store, including the paths that only
/// exist because opening and flushing can fail.
mod driver {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingStore {
        opened: StdMutex<Vec<ShardKey>>,
        released: StdMutex<Vec<ShardKey>>,
        /// Every call, in order, so a term's reset can be seen to come first.
        calls: StdMutex<Vec<&'static str>>,
        fail_open: AtomicBool,
        fail_release: AtomicBool,
        /// Releases and closes, in order, kept apart from `calls` so the
        /// tests about terms need not know about closing.
        teardown: StdMutex<Vec<(&'static str, ShardKey)>>,
    }

    #[async_trait::async_trait]
    impl ShardStore for RecordingStore {
        async fn open(
            &self,
            key: &ShardKey,
            _generation: u64,
            _begins_here: bool,
        ) -> anyhow::Result<()> {
            if self.fail_open.load(Ordering::Acquire) {
                return Err(anyhow::anyhow!("injected open failure"));
            }
            self.opened.lock().expect("lock").push(key.clone());
            self.calls.lock().expect("lock").push("open");
            Ok(())
        }

        async fn begin_term(&self, _key: &ShardKey) {
            self.calls.lock().expect("lock").push("begin_term");
        }

        async fn release(&self, key: &ShardKey) -> anyhow::Result<()> {
            self.released.lock().expect("lock").push(key.clone());
            self.teardown
                .lock()
                .expect("lock")
                .push(("release", key.clone()));
            if self.fail_release.load(Ordering::Acquire) {
                return Err(anyhow::anyhow!("injected flush failure"));
            }
            Ok(())
        }

        async fn close(&self, key: &ShardKey) -> anyhow::Result<()> {
            self.teardown
                .lock()
                .expect("lock")
                .push(("close", key.clone()));
            Ok(())
        }
    }

    impl RecordingStore {
        fn take_teardown(&self) -> Vec<(&'static str, u32)> {
            std::mem::take(&mut *self.teardown.lock().expect("lock"))
                .into_iter()
                .map(|(what, key)| (what, key.shard))
                .collect()
        }
    }

    fn with_roles(
        shard: u32,
        leader: &str,
        replicas: &[&str],
        successor: Option<&str>,
        generation: u64,
    ) -> (ShardKey, ShardAssignment) {
        let mut assignment = assigned_to(leader, generation);
        assignment.key = key(shard);
        assignment.replicas = replicas.iter().map(|node| node.to_string()).collect();
        assignment.successor = successor.map(str::to_string);
        (key(shard), assignment)
    }

    /// A shard's logs close only once this broker is neither its leader, a
    /// replica, nor a move's destination -- and after the release, so they
    /// are flushed and their readers ended first.
    #[tokio::test]
    async fn a_shard_is_closed_once_this_broker_has_no_role_in_it() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        let step = |set: Vec<(ShardKey, ShardAssignment)>| {
            let own = &own;
            let store = &store;
            async move {
                reconcile(own, store, &set.into_iter().collect()).await;
                store.take_teardown()
            }
        };

        assert!(
            step(vec![with_roles(0, "broker-a", &[], None, 1)])
                .await
                .is_empty()
        );
        // Led elsewhere, but still a replica here.
        assert_eq!(
            step(vec![with_roles(0, "broker-b", &["broker-a"], None, 2)]).await,
            [("release", 0)],
        );
        // Named as a move's destination: still needed.
        assert!(
            step(vec![with_roles(0, "broker-b", &[], Some("broker-a"), 3)])
                .await
                .is_empty()
        );
        assert_eq!(
            step(vec![with_roles(0, "broker-b", &["broker-c"], None, 4)]).await,
            [("close", 0)],
        );
        // Once, not on every poll.
        assert!(
            step(vec![with_roles(0, "broker-b", &["broker-c"], None, 4)])
                .await
                .is_empty()
        );

        // Coming back reopens it, and losing it again closes it again.
        step(vec![with_roles(0, "broker-a", &[], None, 5)]).await;
        assert!(own.lock().await.may_serve_at(&key(0), 5));
        assert_eq!(
            step(vec![with_roles(0, "broker-b", &[], None, 6)]).await,
            [("release", 0), ("close", 0)],
        );
    }

    /// A replica-only shard never enters the lifecycle's phases, and is still
    /// closed -- including when its assignment disappears outright.
    #[tokio::test]
    async fn a_replica_only_shard_is_closed_when_its_assignment_goes() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(
            &own,
            &store,
            &HashMap::from([
                with_roles(1, "broker-b", &["broker-a"], None, 1),
                with_roles(2, "broker-b", &["broker-c"], None, 1),
            ]),
        )
        .await;
        assert!(store.take_teardown().is_empty());

        reconcile(&own, &store, &HashMap::new()).await;
        assert_eq!(store.take_teardown(), [("close", 1)]);
    }

    fn assignments(pairs: &[(u32, &str, u64)]) -> HashMap<ShardKey, ShardAssignment> {
        pairs
            .iter()
            .map(|(shard, leader, generation)| {
                let mut assignment = assigned_to(leader, *generation);
                assignment.key = key(*shard);
                (key(*shard), assignment)
            })
            .collect()
    }

    /// A group's in-flight state belongs to one term of leading. Reopening a
    /// shard this broker is serving keeps it, including the reopen that
    /// fences it for a move; leading again after the move resets it first.
    #[tokio::test]
    async fn only_leading_again_after_a_move_begins_a_new_term() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        let step = |assignment: ShardAssignment| {
            let own = &own;
            let store = &store;
            async move {
                reconcile(own, store, &HashMap::from([(key(0), assignment)])).await;
                std::mem::take(&mut *store.calls.lock().expect("lock"))
            }
        };

        assert_eq!(
            step(assigned_to("broker-a", 1)).await,
            ["begin_term", "open"]
        );
        // A replica staged for a move: a new generation, same leader.
        assert_eq!(step(assigned_to("broker-a", 2)).await, ["open"]);
        // The fence.
        assert_eq!(step(draining_on("broker-a", 3)).await, ["open"]);
        assert!(step(assigned_to("broker-b", 4)).await.is_empty());
        assert_eq!(
            step(assigned_to("broker-a", 5)).await,
            ["begin_term", "open"]
        );
        assert!(own.lock().await.may_serve_at(&key(0), 5));
    }

    #[tokio::test]
    async fn reconcile_opens_only_our_shards() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(
            &own,
            &store,
            &assignments(&[(0, "broker-a", 1), (1, "broker-b", 1), (2, "broker-a", 1)]),
        )
        .await;

        let mut opened: Vec<u32> = store
            .opened
            .lock()
            .expect("lock")
            .iter()
            .map(|k| k.shard)
            .collect();
        opened.sort_unstable();
        assert_eq!(opened, vec![0, 2]);

        let own = own.lock().await;
        assert!(own.may_serve(&key(0)));
        assert!(own.may_serve(&key(2)));
        assert!(!own.may_serve(&key(1)));
    }

    /// A pass over unchanged assignments must not reopen anything.
    #[tokio::test]
    async fn reconcile_is_idempotent() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        let current = assignments(&[(0, "broker-a", 1)]);

        for _ in 0..5 {
            reconcile(&own, &store, &current).await;
        }
        assert_eq!(store.opened.lock().expect("lock").len(), 1);
    }

    /// A shard that vanished from the assignment set is released, not left
    /// being served.
    #[tokio::test]
    async fn a_vanished_assignment_releases_the_shard() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;
        assert!(own.lock().await.may_serve(&key(0)));

        reconcile(&own, &store, &HashMap::new()).await;

        assert_eq!(store.released.lock().expect("lock").len(), 1);
        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Closed);
        assert!(!own.may_serve(&key(0)));
    }

    #[tokio::test]
    async fn losing_a_shard_to_another_broker_releases_it() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;
        reconcile(&own, &store, &assignments(&[(0, "broker-b", 2)])).await;

        assert_eq!(store.released.lock().expect("lock").len(), 1);
        assert!(!own.lock().await.may_serve(&key(0)));
    }

    /// An open failure must leave the shard unserved and visible, not silently
    /// treated as owned.
    #[tokio::test]
    async fn a_failed_open_leaves_the_shard_unserved() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        store.fail_open.store(true, Ordering::Release);

        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Failed);
        assert!(!own.may_serve(&key(0)));
    }

    #[tokio::test]
    async fn a_failed_open_recovers_on_a_new_generation() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        store.fail_open.store(true, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        store.fail_open.store(false, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 2)])).await;

        assert!(own.lock().await.may_serve(&key(0)));
    }

    /// Ownership has moved regardless of whether the flush worked. Continuing
    /// to serve would be worse than an unflushed tail, so the shard still
    /// closes -- loudly.
    #[tokio::test]
    async fn a_failed_flush_still_gives_up_the_shard() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();
        reconcile(&own, &store, &assignments(&[(0, "broker-a", 1)])).await;

        store.fail_release.store(true, Ordering::Release);
        reconcile(&own, &store, &assignments(&[(0, "broker-b", 2)])).await;

        let own = own.lock().await;
        assert_eq!(own.phase(&key(0)), Phase::Closed);
        assert!(!own.may_serve(&key(0)));
    }

    /// Restart: a broker starting empty rebuilds ownership from the assignments
    /// alone, opening every shard it owns before serving any of them.
    #[tokio::test]
    async fn a_restarted_broker_rebuilds_ownership_from_assignments() {
        let own = Mutex::new(lifecycle());
        let store = RecordingStore::default();

        reconcile(
            &own,
            &store,
            &assignments(&[(0, "broker-a", 4), (1, "broker-a", 9), (2, "broker-b", 1)]),
        )
        .await;

        let own = own.lock().await;
        assert!(own.may_serve_at(&key(0), 4));
        assert!(own.may_serve_at(&key(1), 9));
        assert!(!own.may_serve(&key(2)));
        assert_eq!(own.active().count(), 2);
    }
}

/// Where a leadership begins, recorded against the real storage.
///
/// The offset is only correct at this one moment: recovery has finished, and
/// the shard is still `Opening`, so nothing this broker leads has been written
/// yet. Replication resumes followers from it rather than from zero.
mod recording_where_a_leadership_begins {
    use super::*;
    use felix_storage::log::{FsyncMode, LogConfig};

    async fn storage(
        records: usize,
    ) -> (
        std::sync::Arc<felix_broker::DurableStorage>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = felix_broker::DurableStorage::open(
            dir.path(),
            LogConfig {
                segment_size_bytes: 4 * 1024,
                index_spacing_bytes: 256,
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage");
        let log = storage
            .open_stream(&key(0).tenant_id, &key(0).namespace, &key(0).stream, 0)
            .expect("open");
        for i in 0..records {
            log.append(&[bytes::Bytes::from(format!("v{i}"))])
                .await
                .expect("append");
        }
        (std::sync::Arc::new(storage), dir)
    }

    #[tokio::test]
    async fn taking_a_shard_records_the_tail_as_this_generations_start() {
        let (storage, _dir) = storage(7).await;
        let store = DurableShardStore::new(std::sync::Arc::clone(&storage));

        store.open(&key(0), 4, true).await.expect("open");

        let log = storage
            .open_stream(&key(0).tenant_id, &key(0).namespace, &key(0).stream, 0)
            .expect("reopen");
        assert_eq!(
            log.generations()
                .last()
                .map(|epoch| (epoch.generation, epoch.start_offset)),
            Some((4, 7)),
        );
    }

    /// A restart takes the same shard at the same generation. The start offset
    /// must not move to wherever the tail has since reached, or a later repair
    /// would be bounded by a point this leadership did not begin at.
    #[tokio::test]
    async fn retaking_the_same_generation_does_not_move_its_start() {
        let (storage, _dir) = storage(7).await;
        let store = DurableShardStore::new(std::sync::Arc::clone(&storage));
        store.open(&key(0), 4, true).await.expect("open");

        let log = storage
            .open_stream(&key(0).tenant_id, &key(0).namespace, &key(0).stream, 0)
            .expect("reopen");
        log.append(&[bytes::Bytes::from("led")])
            .await
            .expect("append");
        store.open(&key(0), 4, true).await.expect("reopen");

        assert_eq!(
            log.generations().last().map(|epoch| epoch.start_offset),
            Some(7),
        );
    }

    /// A cache shard's log is opened lazily on first use, not here, so there is
    /// nothing to record against — and replication falls back to comparing from
    /// zero for it.
    #[tokio::test]
    async fn a_cache_shard_records_nothing() {
        let (storage, _dir) = storage(3).await;
        let store = DurableShardStore::new(storage);
        let mut cache_key = key(0);
        cache_key.kind = crate::shards::ShardKind::Cache;

        store.open(&cache_key, 4, true).await.expect("open");
    }

    /// Against the real log: a shard moved off this broker is closed under
    /// anything still holding it, and moving it back recovers it from disk.
    #[tokio::test]
    async fn a_shard_that_moves_away_is_closed_and_reopens_when_it_returns() {
        let (storage, _dir) = storage(3).await;
        let store = DurableShardStore::new(std::sync::Arc::clone(&storage));
        let own = tokio::sync::Mutex::new(lifecycle());
        let led_by =
            |leader: &str, generation| HashMap::from([(key(0), assigned_to(leader, generation))]);
        let open = || {
            storage
                .open_stream(&key(0).tenant_id, &key(0).namespace, &key(0).stream, 0)
                .expect("open")
        };

        reconcile(&own, &store, &led_by("broker-a", 1)).await;
        let held = open();
        held.append(&[bytes::Bytes::from("led")])
            .await
            .expect("append");

        reconcile(&own, &store, &led_by("broker-b", 2)).await;
        assert!(held.read_from(0, 1024).await.is_err(), "still open");
        assert!(held.sync().await.is_err());

        reconcile(&own, &store, &led_by("broker-a", 3)).await;
        assert!(own.lock().await.may_serve_at(&key(0), 3));
        let back = open();
        assert_eq!(back.tail_offset().await.expect("tail"), 4);
        back.append(&[bytes::Bytes::from("again")])
            .await
            .expect("append after reopening");
    }
}

fn draining_on(leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        state: "draining".to_string(),
        ..assigned_to(leader, generation)
    }
}

/// The fence of a planned move: a draining assignment stops this broker
/// serving the shard, and re-delivering it does not start serving again.
#[test]
fn a_draining_assignment_stops_serving_and_stays_stopped() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    let action = own.observe(&key(0), Some(&draining_on("broker-a", 4)));
    assert_eq!(
        action,
        Action::Open {
            key: key(0),
            generation: 4,
            new_term: false,
            fence: false,
        },
        "the new generation is recovered so replication can ship from it",
    );
    assert!(!own.may_serve(&key(0)));
    assert_eq!(own.opened(&key(0), 4), Opened::Draining);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
    assert!(!own.may_serve(&key(0)));
    for _ in 0..3 {
        assert_eq!(
            own.observe(&key(0), Some(&draining_on("broker-a", 4))),
            Action::None
        );
    }
    assert!(!own.may_serve(&key(0)));
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// The fence arriving at the generation already being served releases the
/// shard in place rather than reopening it.
#[test]
fn a_draining_assignment_at_the_served_generation_releases() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    own.opened(&key(0), 3);

    assert_eq!(
        own.observe(&key(0), Some(&draining_on("broker-a", 3))),
        Action::Release {
            key: key(0),
            generation: 3
        }
    );
    assert!(!own.may_serve(&key(0)));
    own.released(&key(0), 3);
    assert_eq!(own.phase(&key(0)), Phase::Closed);
    assert_eq!(
        own.observe(&key(0), Some(&draining_on("broker-a", 3))),
        Action::None
    );
}

/// A drain that arrives while the log is still opening lands closed, never
/// active.
#[test]
fn a_drain_during_an_open_does_not_activate() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(
        own.observe(&key(0), Some(&draining_on("broker-a", 3))),
        Action::None
    );
    assert_eq!(own.opened(&key(0), 3), Opened::Draining);
    assert!(!own.may_serve(&key(0)));
    assert_eq!(own.phase(&key(0)), Phase::Closed);
}

/// The shard came back at a later generation: served again, as after any
/// reassignment.
#[test]
fn a_drained_shard_reassigned_here_serves_again() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&draining_on("broker-a", 3)));
    own.opened(&key(0), 3);
    assert_eq!(own.phase(&key(0)), Phase::Closed);

    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 5))),
        Action::Open {
            key: key(0),
            generation: 5,
            new_term: true,
            fence: false,
        }
    );
    assert_eq!(own.opened(&key(0), 5), Opened::Activated);
    assert!(own.may_serve_at(&key(0), 5));
}

/// The write fence follows the phase: open only while `Active`, and at that
/// generation. A move closes it however it arrives -- as a draining copy of
/// the served generation, or as a new, draining generation -- and it stays
/// closed through the open that recovers the log for shipping. Closed at
/// `observe`, so before the driver acts and before `publish_servable`.
#[test]
fn the_fence_is_open_only_while_active() {
    let mut own = lifecycle();
    let fence = std::sync::Arc::clone(own.fence());
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert!(fence.enter(&key(0), 3).is_none(), "opening");
    own.opened(&key(0), 3);
    assert!(fence.enter(&key(0), 3).is_some(), "active");

    // A move as a release of the served generation.
    own.observe(&key(0), Some(&draining_on("broker-a", 3)));
    assert!(fence.enter(&key(0), 3).is_none(), "released for a move");
    own.released(&key(0), 3);

    // Served again, then a move as a new, draining generation.
    own.observe(&key(0), Some(&assigned_to("broker-a", 5)));
    own.opened(&key(0), 5);
    assert!(fence.enter(&key(0), 5).is_some());
    own.observe(&key(0), Some(&draining_on("broker-a", 6)));
    assert!(fence.enter(&key(0), 5).is_none(), "reopening to hand off");
    assert!(fence.enter(&key(0), 6).is_none(), "opening, draining");
    assert_eq!(own.opened(&key(0), 6), Opened::Draining);
    assert!(fence.enter(&key(0), 6).is_none(), "shipping, not serving");
    assert!(fence.quiesced(&key(0)));

    // Reassigned elsewhere.
    own.observe(&key(1), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(1), 1);
    own.observe(&key(1), Some(&assigned_to("broker-b", 2)));
    assert!(fence.enter(&key(1), 1).is_none(), "lost to another broker");
}

/// Ending a moved shard's readers waits for the writes already inside the
/// fence, so each reader is handed everything this broker committed.
#[tokio::test]
async fn releasing_a_shard_waits_for_writes_in_flight() {
    let own = std::sync::Arc::new(tokio::sync::Mutex::new(lifecycle()));
    let store = std::sync::Arc::new(EphemeralShardStore::default());
    let fence = {
        let mut own = own.lock().await;
        own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
        own.opened(&key(0), 3);
        std::sync::Arc::clone(own.fence())
    };
    let in_flight = fence.enter(&key(0), 3).expect("active");
    let action = own
        .lock()
        .await
        .observe(&key(0), Some(&draining_on("broker-a", 3)));

    let released = tokio::spawn({
        let own = std::sync::Arc::clone(&own);
        let store = std::sync::Arc::clone(&store);
        async move { apply(&own, store.as_ref(), action).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !released.is_finished(),
        "ended readers with a write in flight"
    );

    drop(in_flight);
    tokio::time::timeout(std::time::Duration::from_secs(5), released)
        .await
        .expect("released once the write left")
        .expect("task");
    assert_eq!(own.lock().await.phase(&key(0)), Phase::Closed);
}

/// A move arriving as a new, draining generation reopens the shard to ship it
/// and ends its readers. Doing that must not wait on the lifecycle lock the
/// driver took to record the open, or the feed stalls for good.
#[tokio::test]
async fn opening_a_shard_to_hand_it_off_completes() {
    let own = tokio::sync::Mutex::new(lifecycle());
    let store = EphemeralShardStore::default();
    let action = {
        let mut own = own.lock().await;
        own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
        own.opened(&key(0), 3);
        own.observe(&key(0), Some(&draining_on("broker-a", 4)))
    };
    assert!(matches!(action, Action::Open { generation: 4, .. }));

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        apply(&own, &store, action),
    )
    .await
    .expect("the handoff open never finished");
    assert_eq!(own.lock().await.phase(&key(0)), Phase::Closed);
}

fn moving_to(successor: &str, generation: u64, state: &str) -> ShardAssignment {
    ShardAssignment {
        replicas: vec![successor.to_string()],
        state: state.to_string(),
        successor: Some(successor.to_string()),
        ..assigned_to("broker-b", generation)
    }
}

/// A destination prepares once per move, from the stage onward, and serves
/// only after the cut-over names it leader.
#[test]
fn a_named_destination_prepares_once_and_serves_only_after_the_cut_over() {
    let mut own = lifecycle();

    assert_eq!(
        own.observe(&key(0), Some(&moving_to("broker-a", 2, "active"))),
        Action::Prepare { key: key(0) }
    );
    assert!(own.is_incoming(&key(0)));
    assert!(
        !own.may_serve(&key(0)),
        "a destination must not serve before the cut-over"
    );
    // Re-delivered, and then fenced: already prepared.
    assert_eq!(
        own.observe(&key(0), Some(&moving_to("broker-a", 2, "active"))),
        Action::None
    );
    assert_eq!(
        own.observe(&key(0), Some(&moving_to("broker-a", 3, "draining"))),
        Action::None
    );

    // The cut-over.
    assert_eq!(
        own.observe(&key(0), Some(&assigned_to("broker-a", 4))),
        Action::Open {
            key: key(0),
            generation: 4,
            new_term: true,
            fence: false,
        }
    );
    assert_eq!(own.opened(&key(0), 4), Opened::Activated);
    assert!(
        !own.is_incoming(&key(0)),
        "an arrived move is no longer incoming"
    );
}

/// A move that drops this broker as its destination forgets it, so a later
/// move here prepares again.
#[test]
fn a_destination_dropped_from_a_move_forgets_it() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&moving_to("broker-a", 2, "active")));

    assert_eq!(
        own.observe(&key(0), Some(&moving_to("broker-c", 3, "active"))),
        Action::None
    );
    assert!(!own.is_incoming(&key(0)));
    assert_eq!(
        own.observe(&key(0), Some(&moving_to("broker-a", 4, "active"))),
        Action::Prepare { key: key(0) }
    );
}

/// The old leader is the successor of nothing: a move away from it releases,
/// never prepares.
#[test]
fn the_old_leader_does_not_treat_its_own_move_as_incoming() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    own.opened(&key(0), 1);

    let staged = ShardAssignment {
        successor: Some("broker-b".to_string()),
        replicas: vec!["broker-b".to_string()],
        ..assigned_to("broker-a", 2)
    };
    assert!(matches!(
        own.observe(&key(0), Some(&staged)),
        Action::Open { .. }
    ));
    assert!(!own.is_incoming(&key(0)));
}

/// What a reader ended by a move is told: the successor while this broker
/// still leads the shard (a fence arrives as a draining generation), the new
/// leader once it does not, and nothing once the shard has no assignment.
#[test]
fn a_moving_shard_names_where_it_is_headed() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 3)));
    assert_eq!(
        own.headed(&key(0)),
        Some(felix_broker::ShardHandoff {
            node_id: None,
            addr: None,
            generation: 3
        })
    );

    let mut fenced = moving_to("broker-b", 4, "draining");
    fenced.leader = "broker-a".to_string();
    own.observe(&key(0), Some(&fenced));
    let headed = own.headed(&key(0)).expect("assigned");
    assert_eq!(headed.node_id.as_deref(), Some("broker-b"));
    assert_eq!(headed.generation, 4);

    own.observe(&key(0), Some(&assigned_to("broker-b", 5)));
    let headed = own.headed(&key(0)).expect("assigned");
    assert_eq!(headed.node_id.as_deref(), Some("broker-b"));
    assert_eq!(headed.generation, 5);

    own.observe(&key(0), None);
    assert_eq!(own.headed(&key(0)), None);
}
