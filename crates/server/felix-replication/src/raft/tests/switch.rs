//! Members started from a switch's base on directories the hybrid wrote.

use felix_storage::disk_log::switch::{SwitchBase, digest};
use felix_storage::log::{AppendRecord, ReadRange, RecordMark};

use super::*;

/// The seal's generation: the hybrid used 1 to 5 before the switch.
const SEAL: u64 = 5;

fn hybrid() -> LogConfig {
    let config = LogConfig {
        raft_log: false,
        ..log_config()
    };
    config.generations.open_when(|| true);
    config
}

fn record(payload: &[u8], mark: RecordMark) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload),
        timestamp_micros: 1_700_000_000,
        mark,
        publisher: None,
        headers: None,
    }
}

async fn lead(log: &DiskLog, generation: u64) {
    log.accept_generation(generation, Some("a"))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
    log.lead_at(generation);
}

/// What the hybrid left in `dir` at the seal: `payloads`, the seal's
/// generation-start record, and `tail` above it, never acknowledged.
/// Returns the seal's offset.
async fn sealed_copy(dir: &Path, payloads: &[&[u8]], tail: &[&[u8]]) -> u64 {
    std::fs::create_dir_all(dir).expect("dir");
    let log = DiskLog::open(dir, "t/ns/orders/0", hybrid()).expect("open");
    lead(&log, 1).await;
    let records: Vec<_> = payloads
        .iter()
        .map(|p| record(p, RecordMark::None))
        .collect();
    log.append(&records).await.expect("append");
    lead(&log, SEAL).await;
    let seal = log
        .append(&[record(&SEAL.to_be_bytes(), RecordMark::GenerationStart)])
        .await
        .expect("seal")
        .first_offset;
    let tail: Vec<_> = tail.iter().map(|p| record(p, RecordMark::None)).collect();
    if !tail.is_empty() {
        log.append(&tail).await.expect("tail");
    }
    log.shutdown().await.expect("shutdown");
    seal
}

/// The client records from `from` on.
async fn payloads(log: &DiskLog, from: u64) -> Vec<(u64, Bytes)> {
    log.read_range(ReadRange {
        start: from,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .records
    .into_iter()
    .filter(|record| record.mark == RecordMark::None)
    .map(|record| (record.offset, record.payload))
    .collect()
}

/// Each broker's copy of shard 0 sealed at the same base, then started as a
/// voter of it.
async fn switched_cluster() -> (Cluster, SwitchBase) {
    let cluster = Cluster::new(true);
    let below: [&[u8]; 3] = [b"one", b"two", b"three"];
    let mut base = None;
    for broker in BROKERS {
        let dir = cluster.root.path().join(broker).join("0");
        // A copy may hold records above the base no majority took.
        let tail: &[&[u8]] = if broker == "a" { &[b"unacked"] } else { &[] };
        let n = sealed_copy(&dir, &below, tail).await;
        let base = match base {
            Some(base) => base,
            None => {
                let log = DiskLog::open(&dir, "t/ns/orders/0", hybrid()).expect("open");
                let digest = digest(&log, 0, n).await.expect("digest");
                log.shutdown().await.expect("shutdown");
                *base.insert(SwitchBase {
                    generation: SEAL,
                    offset: n,
                    start: 0,
                    digest,
                })
            }
        };
        crate::raft::switch::start_voter(
            &dir,
            "t/ns/orders/0",
            hybrid(),
            &base,
            &MemberId::new(broker, 1),
            &members(),
        )
        .await
        .expect("start")
        .expect("a voter");
    }
    (cluster, base.expect("base"))
}

/// Rules the switch adds to Raft's: the first election is above every
/// generation the hybrid used, the first entry follows the base, nothing
/// below it is rewritten or read as an entry, and it all holds across a
/// restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switched_members_elect_above_the_base_and_continue_its_offsets() {
    let (mut cluster, base) = switched_cluster().await;
    let base_term = base.term().expect("term");
    // Never `initialize`d: the membership is the base snapshot's.
    cluster.open(0).await;
    cluster.elected(0).await;
    for status in cluster.statuses(0) {
        assert!(status.term > base_term, "{status:?}");
    }

    let first = cluster.write(0, b"first").await;
    // The leader's blank entry takes the offset after the base.
    assert!(
        first >= base.offset + 2,
        "{first} after a base at {}",
        base.offset
    );
    cluster.committed_everywhere(0, first).await;

    for broker in BROKERS {
        let log = &cluster.logs[&(broker.to_string(), 0)];
        let read = payloads(log, 0).await;
        assert_eq!(
            read,
            [
                (0, Bytes::from_static(b"one")),
                (1, Bytes::from_static(b"two")),
                (2, Bytes::from_static(b"three")),
                (first, Bytes::from_static(b"first")),
            ],
            "{broker}"
        );
    }

    let term = cluster.statuses(0)[0].term;
    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;
    cluster.elected(0).await;
    let next = cluster.write(0, b"after the restart").await;
    assert!(next > first);
    cluster.committed_everywhere(0, next).await;
    assert!(cluster.statuses(0).iter().all(|status| status.term >= term));
    let log = &cluster.logs[&("b".to_string(), 0)];
    assert_eq!(payloads(log, 0).await.len(), 5);
    cluster.stop().await;
}
