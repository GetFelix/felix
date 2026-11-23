//! CPU per acknowledged record on the hybrid path, to set beside
//! `raft::tests::messages`. A measurement, ignored by default:
//!
//! ```text
//! cargo test -p felix-replication --features raft,testing,felix-common/sim \
//!     --release --lib driver::tests::cpu -- --ignored --nocapture
//! ```
//!
//! Three brokers in one process: broker-a leads six shards and its driver
//! ships to the followers' replica handlers directly, with no QUIC or codec
//! in between, as the Raft measurement's transport does.

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use super::*;
use crate::replica::ReplicaHandler;

const SHARDS: u32 = 6;
const PER_WRITE: usize = 16;
const GEN: u64 = 4;

fn routed(local: &str) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build(
        (0..SHARDS).map(|shard| {
            (
                ShardKey { shard, ..key() },
                "broker-a".to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                GEN,
            )
        }),
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

async fn broker(relaxed: bool) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fsync_mode = if relaxed {
        FsyncMode::Periodic {
            interval: Duration::from_secs(3600),
        }
    } else {
        FsyncMode::OnCommit
    };
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    for shard in 0..SHARDS {
        storage
            .open_stream(TENANT, NAMESPACE, STREAM, shard)
            .expect("open");
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
}

struct Followers(Map<String, ReplicaHandler>);

impl PeerRequester for Followers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let handler = &self.0[node_id];
        Ok(match message {
            InternalMessage::ReplicateRecords(batch) => {
                handler
                    .apply(Some("broker-a"), batch, felix_broker::LogKind::Stream)
                    .await
            }
            other => panic!("unexpected request {other:?}"),
        })
    }
}

/// User plus system CPU this process has used.
fn cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is handed.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let time = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64)
    };
    time(usage.ru_utime) + time(usage.ru_stime)
}

async fn quorum_write(broker: Arc<Broker>, marks: Arc<QuorumMarks>, shard: u32, payload: Bytes) {
    let log = broker
        .shard_log(
            felix_broker::LogKind::Stream,
            TENANT,
            NAMESPACE,
            STREAM,
            shard,
        )
        .await
        .expect("log");
    let payloads = vec![payload; PER_WRITE];
    let appended = log.append(&payloads).await.expect("append");
    mark_shard_appended(&broker, shard);
    // Until the driver's first pass over a shard the marks do not know it
    // is led here, and answer NotLeading.
    loop {
        let waited = marks
            .wait_for(
                &watch_key(&ShardKey { shard, ..key() }),
                GEN,
                appended.last_offset + 1,
                Duration::from_secs(30),
            )
            .await;
        match waited {
            crate::quorum::QuorumWait::Reached => return,
            crate::quorum::QuorumWait::NotLeading => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            other => panic!("{other:?}"),
        }
    }
}

async fn measure(relaxed: bool, in_flight: usize, writes: usize) {
    let (leader, _a) = broker(relaxed).await;
    let mut followers = Map::new();
    let mut dirs = Vec::new();
    for name in ["broker-b", "broker-c"] {
        let (follower, dir) = broker(relaxed).await;
        dirs.push(dir);
        followers.insert(
            name.to_string(),
            ReplicaHandler::new(follower, routed(name)),
        );
    }
    let (driver, marks) = super::counters::driver(
        Followers(followers),
        &leader,
        &routed("broker-a"),
        QuorumMarks::new(),
    );
    let payload = Bytes::from(vec![7u8; 1024]);
    let run = |count: usize| {
        let leader = Arc::clone(&leader);
        let marks = Arc::clone(&marks);
        let payload = payload.clone();
        async move {
            let mut next = 0u32;
            let mut flying = FuturesUnordered::new();
            let mut started = 0;
            while started < in_flight.min(count) {
                flying.push(quorum_write(
                    Arc::clone(&leader),
                    Arc::clone(&marks),
                    next % SHARDS,
                    payload.clone(),
                ));
                next += 1;
                started += 1;
            }
            while flying.next().await.is_some() {
                if started < count {
                    flying.push(quorum_write(
                        Arc::clone(&leader),
                        Arc::clone(&marks),
                        next % SHARDS,
                        payload.clone(),
                    ));
                    next += 1;
                    started += 1;
                }
            }
        }
    };
    run(200).await;
    let (began, spent) = (std::time::Instant::now(), cpu());
    run(writes).await;
    let (elapsed, spent) = (began.elapsed(), cpu() - spent);
    let records = (writes * PER_WRITE) as f64;
    println!(
        "hybrid relaxed {relaxed}, {in_flight} in flight: {:.2} us CPU per record; {:.0} records/s",
        spent.as_secs_f64() * 1e6 / records,
        records / elapsed.as_secs_f64(),
    );
    driver.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement"]
async fn cpu_per_record() {
    let relaxed = std::env::var("FELIX_CPU_RELAXED").map_or(true, |v| v == "1");
    let in_flight = std::env::var("FELIX_CPU_IN_FLIGHT").map_or(32, |v| v.parse().expect("n"));
    measure(relaxed, in_flight, 3_000).await;
}
