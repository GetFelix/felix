use std::collections::HashMap as Map;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardKey, ShardKind};
use felix_wire::internal::Pong;

use super::state::State;
use super::*;
use crate::peer::PeerError;
use crate::suspicion::Suspects;

const A: &str = "broker-a";
const B: &str = "broker-b";
const C: &str = "broker-c";

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn set(nodes: &[&str]) -> HashSet<String> {
    nodes.iter().map(|node| node.to_string()).collect()
}

fn none() -> HashSet<String> {
    HashSet::default()
}

fn withdraw(peer: &str) -> Decision {
    Decision::Withdraw {
        peer: peer.to_string(),
    }
}

fn state(start: Instant) -> State {
    State::new(LivenessConfig::default(), start)
}

/// **The decided timers.** A change here is a change to the design.
#[test]
fn the_defaults_are_the_decided_timers() {
    let config = LivenessConfig::default();
    assert_eq!(config.interval, ms(500));
    assert_eq!(config.withdraw_after, ms(1500));
    assert_eq!(config.stall_limit, Duration::from_secs(5));
    assert_eq!(config.hold_for(), ms(3500));
}

/// **Support is withdrawn after 1.5 s of silence, once, and restored by the
/// next heartbeat.**
#[test]
fn support_is_withdrawn_after_the_silence_and_restored_when_heard() {
    let start = Instant::now();
    let mut state = state(start);
    let watched = set(&[B]);
    assert!(
        state
            .heard(B, 1, start)
            .iter()
            .all(|d| !matches!(d, Decision::Withdraw { .. }))
    );
    assert!(state.tick(&watched, &none(), start + ms(1499)).is_empty());
    assert_eq!(state.supports(B), Some(true));

    assert_eq!(
        state.tick(&watched, &none(), start + ms(1500)),
        vec![withdraw(B)]
    );
    assert_eq!(state.supports(B), Some(false));
    assert!(
        state.tick(&watched, &none(), start + ms(2000)).is_empty(),
        "a withdrawal is decided once"
    );

    assert_eq!(
        state.heard(B, 1, start + ms(2100)),
        vec![Decision::Restore {
            peer: B.to_string()
        }]
    );
    assert_eq!(state.supports(B), Some(true));
}

/// **Heartbeats an interval apart never withdraw support**, even when one is
/// lost.
#[test]
fn one_lost_heartbeat_withdraws_nothing() {
    let start = Instant::now();
    let mut state = state(start);
    let watched = set(&[B]);
    for beat in 0..20u64 {
        let now = start + ms(500 * beat);
        // The fifth is lost.
        if beat != 5 {
            state.heard(B, 1, now);
        }
        assert!(state.tick(&watched, &none(), now + ms(250)).is_empty());
    }
}

/// **A peer first watched gets a whole window, and one no longer watched is
/// forgotten**, epoch included.
#[test]
fn a_new_peer_gets_a_window_and_a_dropped_one_is_forgotten() {
    let start = Instant::now();
    let mut state = state(start);
    state.tick(&set(&[B]), &none(), start);
    let later = start + ms(10_000);
    assert_eq!(
        state.tick(&set(&[B, C]), &none(), later),
        vec![withdraw(B)],
        "broker-c was first watched now"
    );
    state.heard(C, 4, later);
    state.tick(&set(&[B]), &none(), later + ms(1));
    assert_eq!(state.supports(C), None);
    state.tick(&set(&[B, C]), &none(), later + ms(2));
    assert_eq!(state.epoch_of(C), None);
}

/// **A new epoch is a restart.** An older epoch from a supported peer is a
/// message from before the restart and changes nothing. From a peer that had
/// gone silent it is a store that lost its epoch file, and is taken.
#[test]
fn a_new_epoch_is_a_restart_and_a_stale_one_is_ignored() {
    let start = Instant::now();
    let mut state = state(start);
    let watched = set(&[B]);
    state.tick(&watched, &none(), start);
    assert!(
        !state
            .heard(B, 3, start)
            .iter()
            .any(|d| matches!(d, Decision::Restarted { .. })),
        "the first epoch seen is not a restart"
    );
    assert_eq!(
        state.heard(B, 4, start + ms(100)),
        vec![Decision::Restarted {
            peer: B.to_string(),
            epoch: 4
        }]
    );

    assert!(state.heard(B, 3, start + ms(200)).is_empty());
    assert_eq!(state.epoch_of(B), Some(4));
    assert_eq!(
        state.tick(&watched, &none(), start + ms(1600)),
        vec![withdraw(B)],
        "a stale heartbeat does not count as hearing the peer"
    );

    assert_eq!(
        state.heard(B, 1, start + ms(1700)),
        vec![
            Decision::Restarted {
                peer: B.to_string(),
                epoch: 1
            },
            Decision::Restore {
                peer: B.to_string()
            }
        ]
    );
}

/// **The stall limit.** Nothing is sent before the first probe finishes. A
/// probe that finished under 5 s ago keeps this broker sending, so a single
/// slow sync silences nobody. At 5 s it stops, and says so once.
#[test]
fn sending_stops_only_when_no_probe_finished_within_the_stall_limit() {
    let start = Instant::now();
    let mut state = state(start);
    assert!(!state.may_send(start));
    assert!(
        state.tick(&none(), &none(), start).is_empty(),
        "no probe yet is not a stall"
    );

    state.probe_finished(start);
    assert_eq!(state.tick(&none(), &none(), start), vec![Decision::Resumed]);
    // The next sync takes 4.9 s: well past the 1.5 s after which a peer
    // withdraws support for silence, and still not a stall.
    assert!(state.may_send(start + ms(4900)));
    assert!(state.tick(&none(), &none(), start + ms(4900)).is_empty());
    state.probe_finished(start + ms(4900));

    assert!(state.may_send(start + ms(9899)));
    assert!(!state.may_send(start + ms(9900)));
    assert_eq!(
        state.tick(&none(), &none(), start + ms(9900)),
        vec![Decision::Silenced]
    );
    assert!(state.tick(&none(), &none(), start + ms(12_000)).is_empty());

    state.probe_finished(start + ms(12_100));
    assert_eq!(
        state.tick(&none(), &none(), start + ms(12_100)),
        vec![Decision::Resumed]
    );
}

/// **The restart hold ends for a leader's groups when that leader is
/// heard**, and only for those.
#[test]
fn the_restart_hold_ends_at_first_leader_contact() {
    let start = Instant::now();
    let mut state = state(start);
    assert!(state.holds(B, start));
    assert!(state.holds(C, start));

    assert!(
        state
            .heard(B, 1, start + ms(600))
            .contains(&Decision::HoldReleased {
                leader: B.to_string()
            })
    );
    assert!(!state.holds(B, start + ms(600)));
    assert!(state.holds(C, start + ms(600)));
    assert!(
        !state
            .heard(B, 1, start + ms(1100))
            .iter()
            .any(|d| matches!(d, Decision::HoldReleased { .. })),
        "released once"
    );
}

/// **With no leader heard the hold ends after the suspect time plus one
/// election timeout**, naming the leaders that stayed silent.
#[test]
fn the_restart_hold_runs_out_after_the_suspect_time_and_an_election_timeout() {
    let start = Instant::now();
    let mut state = state(start);
    let watched = set(&[B, C]);
    state.heard(B, 1, start + ms(600));
    let expired = |decisions: &[Decision]| {
        decisions.iter().find_map(|d| match d {
            Decision::HoldExpired { unheard } => Some(unheard.clone()),
            _ => None,
        })
    };

    assert!(state.holds(C, start + ms(3499)));
    assert_eq!(
        expired(&state.tick(&watched, &watched, start + ms(3499))),
        None
    );
    assert!(!state.holds(C, start + ms(3500)));
    assert_eq!(
        expired(&state.tick(&watched, &watched, start + ms(3500))),
        Some(vec![C.to_string()])
    );
    assert_eq!(
        expired(&state.tick(&watched, &watched, start + ms(4000))),
        None
    );
    assert!(
        !state
            .heard(C, 1, start + ms(4100))
            .iter()
            .any(|d| matches!(d, Decision::HoldReleased { .. })),
        "nothing is held any more"
    );
}

/// A disk whose sync takes `delay_ms`, or fails.
#[derive(Default)]
struct Disk {
    delay_ms: AtomicU64,
    failing: AtomicBool,
    probes: AtomicU64,
}

#[async_trait::async_trait]
impl ProbeDisk for Disk {
    async fn probe(&self) -> std::io::Result<()> {
        self.probes.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(ms(self.delay_ms.load(Ordering::Relaxed))).await;
        if self.failing.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("injected"));
        }
        Ok(())
    }
}

/// The brokers by node id. A heartbeat is handed to the target as its
/// listener would, named by who sent it.
#[derive(Default)]
struct Net {
    brokers: Mutex<Map<String, Arc<Liveness>>>,
}

struct From {
    node_id: &'static str,
    net: Arc<Net>,
}

impl PeerRequester for From {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let InternalMessage::StoreLiveness(beat) = message else {
            panic!("store liveness sent {message:?}");
        };
        let target = self.net.brokers.lock().get(node_id).cloned();
        match target {
            Some(target) => {
                target.heard(self.node_id, beat.epoch);
                Ok(InternalMessage::Pong(Pong { correlation_id: 0 }))
            }
            None => Err(PeerError::ShuttingDown),
        }
    }

    async fn capabilities(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> Result<PeerCapabilities, PeerError> {
        match self.net.brokers.lock().contains_key(node_id) {
            true => Ok(PeerCapabilities::STORE_LIVENESS),
            false => Err(PeerError::ShuttingDown),
        }
    }
}

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

/// `local`'s router: shard 0 led by broker-a and followed by broker-b, and
/// shard 1 between broker-c and a broker with no address.
fn router(local: &str) -> Arc<ShardRouter> {
    let nodes: Map<String, NodeRef> = [(A, 7001), (B, 7002), (C, 7003)]
        .into_iter()
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let router = ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    );
    let key = |shard| ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    };
    let table = RoutingTable::build(
        [
            (key(0), A.to_string(), vec![A.to_string(), B.to_string()], 1),
            (
                key(1),
                C.to_string(),
                vec![C.to_string(), "broker-d".to_string()],
                1,
            ),
        ],
        &nodes,
    );
    router.publish(table, &nodes);
    Arc::new(router)
}

struct Running {
    liveness: Arc<Liveness>,
    disk: Arc<Disk>,
    shutdown: CancellationToken,
}

fn run(net: &Arc<Net>, node_id: &'static str, epoch: u64) -> Running {
    let liveness = Liveness::new(epoch, LivenessConfig::default());
    let disk = Arc::new(Disk::default());
    let shutdown = CancellationToken::new();
    net.brokers
        .lock()
        .insert(node_id.to_string(), Arc::clone(&liveness));
    liveness.start(
        Arc::new(From {
            node_id,
            net: Arc::clone(net),
        }),
        router(node_id),
        Arc::clone(&disk) as Arc<dyn ProbeDisk>,
        shutdown.clone(),
    );
    Running {
        liveness,
        disk,
        shutdown,
    }
}

fn decided(running: &Running, decision: &Decision) -> bool {
    running.liveness.shadow().decisions.contains(decision)
}

/// **Only brokers that share a shard are peers**, and one with no address is
/// left out.
#[test]
fn peers_are_the_brokers_sharing_a_shard() {
    assert_eq!(
        sharing(&router(B)),
        vec![SharedPeer {
            node_id: A.to_string(),
            addr: "10.0.0.1:7001".parse().expect("addr"),
            leads: true,
        }]
    );
    assert_eq!(
        sharing(&router(A)),
        vec![SharedPeer {
            node_id: B.to_string(),
            addr: "10.0.0.1:7002".parse().expect("addr"),
            leads: false,
        }]
    );
    assert!(sharing(&router(C)).is_empty());
}

/// **Two healthy brokers support each other and learn each other's epoch.**
#[tokio::test(start_paused = true)]
async fn healthy_brokers_support_each_other() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 7);
    let b = run(&net, B, 3);
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(a.liveness.supports(B), Some(true));
    assert_eq!(b.liveness.supports(A), Some(true));
    assert_eq!(a.liveness.epoch_of(B), Some(3));
    assert_eq!(b.liveness.epoch_of(A), Some(7));
    for running in [&a, &b] {
        let shadow = running.liveness.shadow();
        assert!(shadow.sending);
        assert!(
            !shadow.decisions.iter().any(|d| matches!(
                d,
                Decision::Withdraw { .. } | Decision::Silenced | Decision::Restarted { .. }
            )),
            "{:?}",
            shadow.decisions
        );
    }
    assert!(
        decided(
            &b,
            &Decision::HoldReleased {
                leader: A.to_string()
            }
        ),
        "broker-b follows a shard broker-a leads"
    );
}

/// **A stalled disk with a working network silences the broker**, and its
/// peer withdraws support. When the disk answers again, so does the broker.
#[tokio::test(start_paused = true)]
async fn a_stalled_disk_silences_a_broker_whose_network_is_fine() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(3)).await;

    a.disk.delay_ms.store(20_000, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        a.liveness.shadow().sending,
        "under the stall limit a slow sync silences nobody"
    );
    assert_eq!(b.liveness.supports(A), Some(true));

    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!a.liveness.shadow().sending);
    assert!(decided(&a, &Decision::Silenced));
    assert!(decided(&b, &withdraw(A)));
    assert_eq!(
        a.liveness.supports(B),
        Some(true),
        "broker-b's heartbeats still arrive"
    );

    a.disk.delay_ms.store(0, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(a.liveness.shadow().sending);
    assert!(decided(
        &b,
        &Decision::Restore {
            peer: A.to_string()
        }
    ));
    assert_eq!(b.liveness.epoch_of(A), Some(1), "not a restart");
}

/// **One slow sync withdraws nothing.** A 3 s `F_FULLFSYNC` is twice the
/// silence after which a peer withdraws support, and nobody notices.
#[tokio::test(start_paused = true)]
async fn one_slow_sync_withdraws_nothing() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    a.disk.delay_ms.store(3000, Ordering::Relaxed);
    tokio::time::sleep(ms(600)).await;
    a.disk.delay_ms.store(0, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(10)).await;

    for running in [&a, &b] {
        let decisions = running.liveness.shadow().decisions;
        assert!(
            !decisions
                .iter()
                .any(|d| matches!(d, Decision::Withdraw { .. } | Decision::Silenced)),
            "{decisions:?}"
        );
    }
}

/// **A probe that fails counts as one that never finished.**
#[tokio::test(start_paused = true)]
async fn a_failing_disk_silences_the_broker() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    a.disk.failing.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(decided(&a, &Decision::Silenced));
    assert!(decided(&b, &withdraw(A)));
}

/// **Probes never overlap.** A sync that overruns the interval is waited
/// for, so a stalled device has one sync queued on it, not one per interval.
#[tokio::test(start_paused = true)]
async fn a_slow_probe_is_not_doubled() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    a.disk.delay_ms.store(60_000, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(a.disk.probes.load(Ordering::Relaxed), 1);
}

/// **A restart shows a new epoch to peers.**
#[tokio::test(start_paused = true)]
async fn a_restart_shows_a_new_epoch() {
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(2)).await;

    a.shutdown.cancel();
    net.brokers.lock().remove(A);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _a = run(&net, A, 2);
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(decided(
        &b,
        &Decision::Restarted {
            peer: A.to_string(),
            epoch: 2
        }
    ));
    assert_eq!(b.liveness.supports(A), Some(true));
}

/// **A peer that does not run store liveness is not watched**, so its
/// silence decides nothing.
#[tokio::test(start_paused = true)]
async fn a_peer_without_store_liveness_is_not_watched() {
    let net = Arc::new(Net::default());
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(b.liveness.supports(A), None);
    assert!(!decided(&b, &withdraw(A)));
}

/// **Shadow mode acts on nothing.** The hybrid fails over on `Suspects`.
/// Store liveness is never handed them, so a withdrawal leaves them empty.
#[tokio::test(start_paused = true)]
async fn a_withdrawal_names_no_suspect() {
    let suspects = Arc::new(Suspects::default());
    let changes = suspects.subscribe();
    let net = Arc::new(Net::default());
    let a = run(&net, A, 1);
    let b = run(&net, B, 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    a.shutdown.cancel();
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(decided(&b, &withdraw(A)));
    assert!(suspects.current().is_empty());
    assert!(!changes.has_changed().expect("open"));
}

/// **Off by default.** A broker offers `STORE_LIVENESS` only when told to
/// run it, so by default no peer ever sends it a heartbeat.
#[test]
fn the_capability_is_offered_only_when_turned_on() {
    let config = crate::peer::PeerTransportConfig::default();
    assert!(!config.store_liveness);
    assert!(
        !config
            .capabilities()
            .contains(PeerCapabilities::STORE_LIVENESS)
    );
    assert!(
        !config
            .outgoing_capabilities()
            .contains(PeerCapabilities::STORE_LIVENESS)
    );
    let on = crate::peer::PeerTransportConfig {
        store_liveness: true,
        ..config
    };
    assert!(on.capabilities().contains(PeerCapabilities::STORE_LIVENESS));
}
