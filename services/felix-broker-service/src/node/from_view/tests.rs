//! A directory starts from its view only when every rule holds. Each test
//! breaks one rule and nothing else.
use std::net::SocketAddr;

use felix_storage::StorageId;
use felix_storage::run_state::BootId;

use super::*;
use crate::cluster::controlplane_view::{Catalog, Node};

const ME: &str = "broker-a";

/// What a fleet that runs elections finalized, and this build reports.
fn election_fleet() -> Vec<String> {
    let mut fleet: Vec<String> =
        felix_common::fleet::prerequisites(felix_common::fleet::REPLICA_ELECTION.name())
            .iter()
            .map(|feature| feature.name().to_owned())
            .collect();
    fleet.push(felix_common::fleet::REPLICA_ELECTION.name().to_owned());
    fleet
}

fn reported() -> BTreeSet<String> {
    election_fleet().into_iter().collect()
}

fn view(storage_id: StorageId, fleet: Vec<String>) -> View {
    View {
        node_id: ME.to_owned(),
        storage_id: storage_id.to_hex(),
        incarnation: 4,
        fleet,
        routes: Vec::new(),
        nodes: vec![Node {
            node_id: "broker-b".to_owned(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 5], 7002)),
            region: "us-west-2".to_owned(),
        }],
        catalog: Catalog::default(),
    }
}

fn record(root: &Path, view: &View) {
    felix_storage::controlplane_view::record(root, &serde_json::to_vec(view).expect("encode"))
        .expect("record the view");
}

/// A directory with a storage id and a view registered under it.
fn qualifying() -> (tempfile::TempDir, StorageId) {
    let dir = tempfile::tempdir().expect("dir");
    let (id, _) =
        felix_storage::storage_identity::load_or_create(dir.path(), || [7; 16]).expect("id");
    record(dir.path(), &view(id, election_fleet()));
    (dir, id)
}

#[test]
fn a_directory_that_meets_every_rule_starts_from_its_view() {
    let (dir, id) = qualifying();
    let started = qualify(dir.path(), ME, &reported()).expect("qualifies");
    assert_eq!(started, view(id, election_fleet()));
    assert!(opened_under(&started, id));
}

/// A new, wiped or upgraded directory, or one whose node deregistered,
/// has nothing to start from.
#[test]
fn no_view_waits() {
    let (dir, _) = qualifying();
    felix_storage::controlplane_view::forget(dir.path()).expect("forget");
    assert_eq!(qualify(dir.path(), ME, &reported()), Err(Refusal::NoView));
}

#[test]
fn another_nodes_view_waits() {
    let (dir, _) = qualifying();
    assert_eq!(
        qualify(dir.path(), "broker-z", &reported()),
        Err(Refusal::NoView)
    );
}

/// No `registration` and no storage id: the directory was never this
/// node's registered one.
#[test]
fn a_directory_without_its_storage_id_waits() {
    let (dir, id) = qualifying();
    std::fs::remove_file(dir.path().join("storage-id")).expect("remove");
    assert_eq!(
        qualify(dir.path(), ME, &reported()),
        Err(Refusal::OtherDirectory {
            on_disk: None,
            registered: id.to_hex(),
        })
    );
    // Asking created no id.
    assert!(!dir.path().join("storage-id").exists());
}

/// A start that qualified, whose run then rotated the id (a run that crashed
/// in another boot under a relaxed fsync mode), is under an id the control
/// plane never registered.
#[test]
fn a_start_whose_run_rotated_the_id_does_not_go_on_from_the_view() {
    let (dir, id) = qualifying();
    let crashed_in = BootId::from_bytes([1; 16]);
    let this_boot = BootId::from_bytes([2; 16]);
    let first = felix_storage::run_state::begin(
        dir.path(),
        true,
        None,
        Some(crashed_in),
        || false,
        || [8; 16],
    )
    .expect("the run that crashes");
    assert_eq!(first.id, id);
    let begun = felix_storage::run_state::begin(
        dir.path(),
        true,
        None,
        Some(this_boot),
        || false,
        || [9; 16],
    )
    .expect("the start after a machine crash");
    assert!(begun.rotated);
    // That start began the run before it could tell, so it must not go on
    // from the view.
    let started = view(id, election_fleet());
    assert!(!opened_under(&started, begun.id));
}

/// The id rotated on one start that was then killed while it waited for the
/// control plane. The next start, in the same boot, keeps the rotated id, so
/// "this start did not rotate" would pass it; the registered id does not.
#[test]
fn an_id_rotated_on_an_earlier_start_that_never_registered_waits() {
    let (dir, id) = qualifying();
    let this_boot = BootId::from_bytes([2; 16]);
    felix_storage::run_state::begin(
        dir.path(),
        true,
        None,
        Some(BootId::from_bytes([1; 16])),
        || false,
        || [8; 16],
    )
    .expect("the run that crashes");
    let rotated = felix_storage::run_state::begin(
        dir.path(),
        true,
        None,
        Some(this_boot),
        || false,
        || [9; 16],
    )
    .expect("the start that is killed while it waits");
    assert!(rotated.rotated);
    // Killed: nothing recorded a stop. The next start asks first.
    assert_eq!(
        qualify(dir.path(), ME, &reported()),
        Err(Refusal::OtherDirectory {
            on_disk: Some(rotated.id.to_hex()),
            registered: id.to_hex(),
        })
    );
}

#[test]
fn a_fleet_without_replica_election_waits() {
    let (dir, id) = qualifying();
    let fleet: Vec<String> = election_fleet()
        .into_iter()
        .filter(|feature| feature != felix_common::fleet::REPLICA_ELECTION.name())
        .collect();
    record(dir.path(), &view(id, fleet));
    assert_eq!(
        qualify(dir.path(), ME, &reported()),
        Err(Refusal::NoElection)
    );
}

/// A rolled-back build, or one started with elections off, lacks a feature
/// the fleet finalized: the control plane would refuse it.
#[test]
fn a_finalized_feature_this_build_does_not_report_waits() {
    let (dir, _) = qualifying();
    let mut reported = reported();
    reported.remove(felix_common::fleet::REPLICA_ELECTION.name());
    assert_eq!(
        qualify(dir.path(), ME, &reported),
        Err(Refusal::Unreported(vec![
            felix_common::fleet::REPLICA_ELECTION.name().to_owned()
        ]))
    );
}

#[test]
fn the_views_nodes_are_routable_at_once() {
    let (_, id) = qualifying();
    let nodes = nodes(&view(id, election_fleet()));
    let b = nodes.get("broker-b").expect("broker-b");
    assert_eq!(b.advertise_addr, SocketAddr::from(([10, 0, 0, 5], 7002)));
    assert!(b.live);
}

/// A directory the control plane last registered under another id is marked
/// replaced. A start that stopped for it keeps the marker, so the next start
/// during an outage must not vote from the view either.
#[test]
fn a_directory_marked_replaced_waits() {
    let (dir, id) = qualifying();
    // Marked when the control plane's record was at the view's incarnation.
    // Marking removes the view; one written since at that incarnation is
    // still not from a registration after the replacement.
    felix_storage::run_state::record_replaced(dir.path(), id, 4).expect("mark");
    record(dir.path(), &view(id, election_fleet()));
    assert_eq!(qualify(dir.path(), ME, &reported()), Err(Refusal::Replaced));
}

/// The node was deleted, registered from another disk at incarnations 0 and
/// 1, and this disk came back: the marker says `after = 1`, below the view's
/// 4, though the view predates the replacement. Marking removes it, so the
/// start waits.
#[test]
fn a_view_from_before_a_replacement_waits_whatever_its_incarnation() {
    let (dir, id) = qualifying();
    felix_storage::run_state::record_replaced(dir.path(), id, 1).expect("mark");
    assert_eq!(qualify(dir.path(), ME, &reported()), Err(Refusal::NoView));
}

/// Once the replaced directory registered its current id, the view written
/// from that registration says which copies it holds, so later outages may
/// start from it.
#[test]
fn a_view_registered_after_the_replacement_starts() {
    let (dir, id) = qualifying();
    felix_storage::run_state::record_replaced(dir.path(), id, 3).expect("mark");
    // Written by the registration that followed.
    record(dir.path(), &view(id, election_fleet()));
    assert_eq!(
        qualify(dir.path(), ME, &reported()),
        Ok(view(id, election_fleet()))
    );
}

/// A marker written for another id, or by an older build that did not say
/// when, does not vouch for the view.
#[test]
fn a_marker_that_does_not_date_the_view_waits() {
    let (dir, id) = qualifying();
    felix_storage::run_state::record_replaced(dir.path(), StorageId::from_bytes([9; 16]), 1)
        .expect("mark");
    record(dir.path(), &view(id, election_fleet()));
    assert_eq!(qualify(dir.path(), ME, &reported()), Err(Refusal::Replaced));
    std::fs::write(
        dir.path()
            .join(felix_storage::run_state::REPLACED_FILE_NAME),
        b"replaced\n",
    )
    .expect("an older build's marker");
    assert_eq!(qualify(dir.path(), ME, &reported()), Err(Refusal::Replaced));
}
