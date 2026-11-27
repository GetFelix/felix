//! Member opens held until the broker knows the fleet's features (#3316).

use tokio::time::Instant;

use super::super::fleet_wait::{FLEET_WAIT, FleetWait, Hold};
use super::super::standalone::fleet_of_one;
use super::*;

const PERIODIC: FsyncMode = FsyncMode::Periodic {
    interval: Duration::from_secs(3600),
};

/// A relaxed broker whose run rotates on a crash, so its members run its
/// own mode once the fleet finalized `raft_written_commit`.
async fn relaxed(net: &Arc<Loopback>, fleet: Arc<FleetGate>) -> Member {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker_at(
        root.path(),
        3,
        INCARNATION,
        ConsistencyLevel::Quorum,
        PERIODIC,
    )
    .await;
    felix_storage::run_state::begin(
        root.path(),
        true,
        None,
        None,
        || false,
        || felix_common::random::uuid_v4().into_bytes(),
    )
    .expect("begin the run");
    Member::assemble(net, BROKERS[0], root, broker, fleet)
}

async fn mode(member: &Member) -> Option<FsyncMode> {
    let log = member.shards.log_of(&key("orders", 0)).await?;
    Some(log.config().fsync_mode)
}

async fn pass(member: &Member) {
    member.shards.take(&mut assignments([replicated()])).await;
}

/// **A member the pass reaches before registration answers opens in the
/// broker's own mode once it has.** Opened at once it would read the
/// feature as off and fsync on commit until it next opened.
#[tokio::test]
async fn a_member_reached_before_the_fleet_is_known_opens_in_the_configured_mode() {
    let net = Arc::new(Loopback::default());
    let gate = Arc::new(FleetGate::for_this_build());
    let member = relaxed(&net, Arc::clone(&gate)).await;
    pass(&member).await;
    assert_eq!(mode(&member).await, None, "opened on an unknown fleet");

    gate.restart([fleet::RAFT_WRITTEN_COMMIT.name()]);
    pass(&member).await;
    assert_eq!(mode(&member).await, Some(PERIODIC));
    assert!(EngineShards::shutdown(&*member.shards).await);
}

/// **A standalone broker is its whole fleet, so nothing waits.**
#[tokio::test]
async fn a_standalone_broker_opens_at_once() {
    let net = Arc::new(Loopback::default());
    let member = relaxed(&net, Arc::new(fleet_of_one())).await;
    pass(&member).await;
    assert_eq!(mode(&member).await, Some(PERIODIC));
    assert!(EngineShards::shutdown(&*member.shards).await);
}

/// **A start that votes before its restore check does not wait either**:
/// its members open while the control plane is down, on the strict mode.
#[tokio::test]
async fn a_start_that_votes_unchecked_opens_at_once_and_strict() {
    let net = Arc::new(Loopback::default());
    let member = relaxed(&net, Arc::new(FleetGate::for_this_build())).await;
    member
        .shards
        .stand_after(tokio_util::sync::CancellationToken::new());
    pass(&member).await;
    assert_eq!(mode(&member).await, Some(FsyncMode::OnCommit));
    assert!(EngineShards::shutdown(&*member.shards).await);
}

/// **A fleet seeded from the saved view with every feature the open reads
/// decided opens at once**; one that leaves a feature undecided waits.
#[test]
fn a_seeded_fleet_waits_only_for_what_it_cannot_decide() {
    use super::super::fleet_wait::decided;
    let reported = FleetGate::for_this_build().reported();
    let both = [fleet::RAFT_WRITTEN_COMMIT, fleet::RAFT_ALONE_REFORM].map(|f| f.name());
    assert!(decided(FleetGate::seeded(reported.clone(), both).view()));
    let one = [fleet::RAFT_WRITTEN_COMMIT.name()];
    assert!(!decided(FleetGate::seeded(reported, one).view()));
    // A feature this broker does not report is never enabled here.
    assert!(decided(FleetGate::new(Vec::<String>::new()).view()));
}

/// **The hold is bounded**: past [`FLEET_WAIT`] members open on what is
/// known, and the give-up is reported once.
#[test]
fn the_hold_gives_up_after_its_bound() {
    let start = Instant::now();
    let mut wait = FleetWait::default();
    assert_eq!(wait.check(false, start), Hold::Began);
    assert_eq!(wait.check(false, start + FLEET_WAIT / 2), Hold::Held);
    assert_eq!(wait.check(false, start + FLEET_WAIT), Hold::GaveUp);
    assert_eq!(wait.check(false, start + FLEET_WAIT * 2), Hold::Open);

    let mut known = FleetWait::default();
    assert_eq!(known.check(true, start), Hold::Open);
}
