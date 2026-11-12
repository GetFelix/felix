//! What idle Raft groups cost a broker in CPU, measured: one runtime leads
//! every group and its peers are stubs that agree, so each group pays a
//! leader's timers and heartbeats and nothing else.
//!
//! Ignored by default, since it is a measurement. Run it optimised:
//!
//! ```text
//! RAFT_IDLE_GROUPS=1500 cargo test --release -p felix-replication \
//!     --test raft_idle_cpu -- --ignored --nocapture
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use felix_replication::raft::{
    AgreeingPeers, GroupId, GroupRuntime, MemberId, Role, RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};

fn shard(n: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: n,
        kind: ShardKind::Stream,
    }
}

/// CPU time this process has used, user and system.
fn cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fills the struct it is given.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let time = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    time(usage.ru_utime) + time(usage.ru_stime)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; see the module docs"]
async fn idle_groups_cost_little_cpu() {
    let groups: u32 = std::env::var("RAFT_IDLE_GROUPS")
        .ok()
        .and_then(|groups| groups.parse().ok())
        .unwrap_or(1500);
    let root = tempfile::tempdir().expect("dir");
    let runtime = GroupRuntime::start("a", RuntimeConfig::default(), Arc::new(AgreeingPeers))
        .expect("timers");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let members: BTreeSet<_> = ["a", "b", "c"].map(|b| MemberId::new(b, 1)).into();

    for n in 0..groups {
        let dir = root.path().join(n.to_string());
        std::fs::create_dir_all(&dir).expect("dir");
        let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
        let group = GroupId {
            shard: shard(n),
            epoch: 0,
        };
        runtime.open(group, 1, log).await.expect("open");
        runtime
            .initialize(&shard(n), members.clone())
            .await
            .expect("initialize");
    }
    let leading = || {
        (0..groups)
            .filter(|n| runtime.status(&shard(*n)).map(|s| s.role) == Some(Role::Leader))
            .count() as u32
    };
    while leading() < groups {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Let the first entries commit and the logs go quiet.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let window = Duration::from_secs(30);
    let ticks = || -> u64 {
        (0..groups)
            .filter_map(|n| runtime.status(&shard(n)))
            .map(|status| status.ticks)
            .sum()
    };
    let (cpu_before, ticks_before) = (cpu(), ticks());
    tokio::time::sleep(window).await;
    let used = cpu() - cpu_before;
    let sent = ticks() - ticks_before;
    println!(
        "{groups} idle groups, all led here: {:.2}% of one core, {:.0} timer checks a second",
        100.0 * used.as_secs_f64() / window.as_secs_f64(),
        sent as f64 / window.as_secs_f64(),
    );
    assert_eq!(leading(), groups, "a group lost its leader while idle");
    runtime.shutdown().await;
}
