//! Throwaway: cost of N three-member openraft groups in one process.
//! usage: openraft-scale <groups> <timers:on|off> <idle_secs>
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use openraft::alias::SnapshotOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::raft::{AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::{Config, OptionalSend};
use openraft_memstore::{TypeConfig, new_mem_store};
use openraft_multi::{GroupNetworkAdapter, GroupNetworkFactory, GroupRouter};

type Raft = openraft::Raft<TypeConfig, Arc<openraft_memstore::MemStateMachine>>;

#[derive(Default)]
struct Inner {
    rafts: RwLock<HashMap<(u64, u64), Raft>>,
    rpcs: AtomicU64,
    heartbeats: AtomicU64,
}

#[derive(Clone, Default)]
struct Router(Arc<Inner>);

impl Router {
    fn get(&self, node: u64, group: u64) -> Result<Raft, RPCError<TypeConfig>> {
        self.0
            .rafts
            .read()
            .unwrap()
            .get(&(node, group))
            .cloned()
            .ok_or_else(|| RPCError::Unreachable(Unreachable::from_string("no such raft")))
    }
}

fn unreach<E: std::error::Error + 'static>(e: E) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&e))
}

impl GroupRouter<TypeConfig, u64> for Router {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn append_entries(
        &self,
        target: u64,
        group: u64,
        rpc: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        self.0.rpcs.fetch_add(1, Relaxed);
        if rpc.entries.is_empty() {
            self.0.heartbeats.fetch_add(1, Relaxed);
        }
        self.get(target, group)?.append_entries(rpc).await.map_err(unreach)
    }

    async fn vote(
        &self,
        target: u64,
        group: u64,
        rpc: VoteRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.0.rpcs.fetch_add(1, Relaxed);
        self.get(target, group)?.vote(rpc).await.map_err(unreach)
    }

    async fn pre_vote(
        &self,
        target: u64,
        group: u64,
        rpc: VoteRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.0.rpcs.fetch_add(1, Relaxed);
        self.get(target, group)?.pre_vote(rpc).await.map_err(unreach)
    }

    async fn full_snapshot(
        &self,
        _t: u64,
        _g: u64,
        _vote: openraft::alias::VoteOf<TypeConfig>,
        _s: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _c: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _o: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Unreachable(Unreachable::from_string("no snapshots")))
    }
}

struct Factory(GroupNetworkFactory<Router, u64>);

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = GroupNetworkAdapter<TypeConfig, u64, Router>;
    async fn new_client(&mut self, target: u64, _node: &()) -> Self::Network {
        GroupNetworkAdapter::new(self.0.factory.clone(), target, self.0.group_id)
    }
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)?.parse().ok()))
        .unwrap_or(0)
}

fn cpu_ticks() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = s.rsplit(')').next().unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    f.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) + f.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n: u64 = a[1].parse().unwrap();
    let timers = a[2] == "on";
    let idle: u64 = a[3].parse().unwrap();
    let rss0 = rss_kb();

    let cfg = Arc::new(
        Config { enable_heartbeat: timers, enable_elect: timers, ..Default::default() }.validate().unwrap(),
    );
    let router = Router::default();
    let t0 = Instant::now();
    for g in 0..n {
        for node in 1..=3u64 {
            let (ls, sm) = new_mem_store();
            let f = Factory(GroupNetworkFactory::new(router.clone(), g));
            let r = Raft::new(node, cfg.clone(), f, ls, sm).await.unwrap();
            router.0.rafts.write().unwrap().insert((node, g), r);
        }
    }
    let members: BTreeSet<u64> = [1, 2, 3].into();
    let mut leaders = Vec::new();
    for g in 0..n {
        let r = router.get(1, g).ok().unwrap();
        r.initialize(members.clone()).await.unwrap();
        r.trigger().elect(false).await.unwrap();
        leaders.push(r);
    }
    let mut ready = 0;
    for _ in 0..120 {
        ready = leaders.iter().filter(|r| r.metrics().borrow_watched().current_leader == Some(1)).count();
        if ready as u64 == n {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let setup = t0.elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_secs(10)).await;

    let m = tokio::runtime::Handle::current().metrics();
    let tasks = m.num_alive_tasks();
    let (c0, h0, p0, w0) = (cpu_ticks(), router.0.heartbeats.load(Relaxed), router.0.rpcs.load(Relaxed), Instant::now());
    tokio::time::sleep(Duration::from_secs(idle)).await;
    let el = w0.elapsed().as_secs_f64();
    let (c1, h1, p1) = (cpu_ticks(), router.0.heartbeats.load(Relaxed), router.0.rpcs.load(Relaxed));
    let cpu_pct = (c1 - c0) as f64 / 100.0 / el * 100.0;
    println!(
        "RESULT groups={n} timers={} leaders={ready} setup_s={setup:.1} rss_base_mb={} rss_mb={} rss_per_group_kb={} tasks={tasks} idle_cpu_pct={cpu_pct:.1} hb_per_s={:.0} rpc_per_s={:.0}",
        a[2],
        rss0 / 1024,
        rss_kb() / 1024,
        (rss_kb() - rss0) / n,
        (h1 - h0) as f64 / el,
        (p1 - p0) as f64 / el,
    );
    std::process::exit(0);
}
