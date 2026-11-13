use std::path::PathBuf;
use std::time::Duration;

use super::*;

const SEG: &str = "sim/ns/events/0/00000000000000000000.log";
const OTHER: &str = "sim/ns/events/1/00000000000000000000.log";

fn loss(node: usize, at: u64, torn: Option<&str>, repaired_before: usize) -> PowerLoss {
    loss_opened(node, at, torn, repaired_before, 0)
}

fn loss_opened(
    node: usize,
    at: u64,
    torn: Option<&str>,
    repaired_before: usize,
    opened_before: usize,
) -> PowerLoss {
    PowerLoss {
        node,
        at: Duration::from_secs(at),
        torn: torn.map(PathBuf::from),
        repaired_before,
        opened_before,
        restarted_at: Some(Duration::from_secs(at + 5)),
    }
}

fn paths(list: &[&str]) -> Vec<PathBuf> {
    list.iter().map(PathBuf::from).collect()
}

fn missed(missed: &[PowerLoss]) -> Vec<(usize, u64)> {
    missed
        .iter()
        .map(|loss| (loss.node, loss.at.as_secs()))
        .collect()
}

#[test]
fn a_torn_segment_repaired_after_the_cut_passes() {
    let losses = [loss(0, 10, Some(SEG), 0)];
    assert!(unrepaired(&losses, |_| paths(&[SEG]), |_| paths(&[SEG])).is_empty());
}

#[test]
fn a_repair_of_another_segment_does_not_count() {
    let losses = [loss(0, 10, Some(SEG), 0)];
    assert_eq!(
        missed(&unrepaired(&losses, |_| paths(&[OTHER]), |_| paths(&[SEG]))),
        [(0, 10)]
    );
}

#[test]
fn a_repair_from_before_the_cut_does_not_count() {
    let losses = [loss(0, 10, Some(SEG), 1)];
    assert_eq!(
        missed(&unrepaired(&losses, |_| paths(&[SEG]), |_| paths(&[SEG]))),
        [(0, 10)]
    );
}

#[test]
fn a_cut_with_no_append_in_flight_needs_no_repair() {
    let losses = [loss(0, 10, None, 0)];
    assert!(unrepaired(&losses, |_| Vec::new(), |_| paths(&[SEG])).is_empty());
}

#[test]
fn each_cut_is_held_to_the_window_before_the_next_one_on_its_broker() {
    // b1's first cut was repaired before its second; the second never was.
    // b2's repairs are its own.
    let losses = [
        loss(0, 10, Some(SEG), 0),
        loss(1, 20, Some(SEG), 0),
        loss(0, 30, Some(OTHER), 1),
    ];
    let repaired = |node: usize| {
        if node == 0 {
            paths(&[SEG])
        } else {
            paths(&[SEG, OTHER])
        }
    };
    assert_eq!(
        missed(&unrepaired(&losses, repaired, |_| paths(&[SEG, OTHER]))),
        [(0, 30)]
    );

    // A repair after the second cut does not cover the first.
    let losses = [
        loss(0, 10, Some(SEG), 0),
        loss_opened(0, 30, Some(OTHER), 0, 1),
    ];
    assert_eq!(
        missed(&unrepaired(
            &losses,
            |_| paths(&[OTHER, SEG]),
            |_| paths(&[SEG, OTHER])
        )),
        [(0, 10)]
    );
}

#[test]
fn a_torn_segment_never_opened_again_is_not_held_to_a_repair() {
    // The broker came back but no longer hosts that shard, or stayed down.
    let losses = [loss(0, 10, Some(SEG), 0)];
    assert!(unrepaired(&losses, |_| Vec::new(), |_| Vec::new()).is_empty());
    assert!(unrepaired(&losses, |_| Vec::new(), |_| paths(&[OTHER])).is_empty());
}

#[test]
fn an_open_from_before_the_cut_does_not_count() {
    let losses = [loss_opened(0, 10, Some(SEG), 0, 1)];
    assert!(unrepaired(&losses, |_| Vec::new(), |_| paths(&[SEG])).is_empty());
}

#[test]
fn an_open_after_the_next_cut_does_not_count_for_the_first() {
    let losses = [
        loss_opened(0, 10, Some(SEG), 0, 0),
        loss_opened(0, 30, None, 0, 0),
    ];
    assert!(unrepaired(&losses, |_| Vec::new(), |_| paths(&[SEG])).is_empty());
}

/// The broker opens a torn segment and nothing repairs it: the finding the
/// checker exists for. A read of the simulator's own does not count, the
/// same read made as the broker would does.
#[tokio::test(start_paused = true)]
async fn a_torn_segment_opened_without_a_repair_is_missed() {
    use felix_cluster::history::rng::Rng;
    use felix_storage::disk_log::inspect::{RecordsQuery, find_shards, read_records};
    use felix_storage::log::{FsyncMode, LogConfig};
    use tokio_util::sync::CancellationToken;

    use crate::client::{Recorder, SimClient};
    use crate::cluster::{SimCluster, SimConfig};

    let config = SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 2,
        cache_keys: 4,
        fsync_mode: FsyncMode::OnCommit,
        power_loss: true,
        ..SimConfig::for_seed(1)
    };
    let cluster = SimCluster::start(1, config).await.unwrap();
    let recorder = Recorder::new(cluster.config.shards);
    let stop = CancellationToken::new();
    let client = SimClient::new(0, cluster.clone(), recorder, Rng::new(1));
    let client = tokio::spawn(client.run(stop.clone()));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(cluster.power_loss(0).await, "no append to cut");
    stop.cancel();
    let _ = client.await;

    let losses = cluster.power_losses();
    let torn = losses[0].torn.clone().expect("a torn segment");
    let missed = || {
        unrepaired(
            &losses,
            |node| cluster.torn_tails_repaired(node),
            |node| cluster.segments_opened(node),
        )
    };
    cluster.disk_values();
    assert!(missed().is_empty(), "the simulator's read counted");

    // The inspector opens the segment and repairs nothing, as a recovery
    // that missed the tail would.
    let root = cluster.data_path(0);
    let query = RecordsQuery {
        offset: 0,
        around: 100,
        payload: false,
    };
    for shard in find_shards(root).unwrap() {
        let _ = read_records(&shard.path, &query, &LogConfig::default());
    }
    assert!(cluster.segments_opened(0)[losses[0].opened_before..].contains(&torn));
    assert_eq!(missed().len(), 1);
}
