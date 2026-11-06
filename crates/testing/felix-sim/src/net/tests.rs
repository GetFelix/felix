use std::sync::atomic::{AtomicUsize, Ordering};

use felix_wire::internal::{Fence, Hello, Ping, Pong, ReplicaLog, ReplicateOk, ShardRef};
use tokio::time::Instant;

use super::*;

const ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

/// Answers every request, pings included, with a learner's ack; counts what
/// it handled, and remembers who asked last.
#[derive(Default)]
struct Echo {
    handled: AtomicUsize,
    asked_by: Mutex<Option<Arc<str>>>,
    /// How long each answer takes.
    takes: Duration,
}

#[async_trait::async_trait]
impl PeerRequestHandler for Echo {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.handle_from(None, request).await
    }

    async fn handle_from(
        &self,
        peer: Option<Arc<str>>,
        request: InternalMessage,
    ) -> InternalMessage {
        tokio::time::sleep(self.takes).await;
        self.handled.fetch_add(1, Ordering::SeqCst);
        *self.asked_by.lock() = peer;
        ack(request.correlation_id(), true)
    }
}

fn config() -> PeerTransportConfig {
    PeerTransportConfig {
        request_timeout: Duration::from_secs(5),
        ..PeerTransportConfig::default()
    }
}

fn node(node_id: &str, capabilities: PeerCapabilities, handler: &Arc<Echo>) -> SimNode {
    SimNode {
        node_id: node_id.to_string(),
        capabilities,
        identity: PeerIdentity::default(),
        handler: Arc::clone(handler) as Arc<dyn PeerRequestHandler>,
    }
}

fn ack(correlation_id: u64, learner: bool) -> InternalMessage {
    InternalMessage::ReplicateOk(ReplicateOk {
        correlation_id,
        durable_offset: 0,
        learner,
    })
}

/// A request any peer handles, whatever it offered.
fn request() -> InternalMessage {
    InternalMessage::Hello(Hello {
        correlation_id: 7,
        node_id: "a".to_string(),
        capabilities: None,
    })
}

/// Two nodes, `a` and `b`, with `b` answering through `echo`.
fn pair(net: &SimNet, echo: &Arc<Echo>) -> (SimPeer, SimPeer) {
    let a = net.start(node("a", PeerCapabilities::NONE, &Arc::default()));
    let b = net.start(node("b", PeerCapabilities::NONE, echo));
    (a, b)
}

#[tokio::test(start_paused = true)]
async fn a_request_reaches_the_target_and_its_answer_comes_back() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);

    let answer = a.request("b", ADDR, request()).await.unwrap();

    // a never offered learner acks, so the learner's ack reads as a plain
    // one, as the listener rewrites it.
    assert_eq!(answer, ack(7, false));
    assert_eq!(echo.handled.load(Ordering::SeqCst), 1);
    assert_eq!(a.recorded_capabilities("b"), Some(PeerCapabilities::NONE));
}

#[tokio::test(start_paused = true)]
async fn a_partition_refuses_at_once_and_only_in_its_direction() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, b) = pair(&net, &echo);
    net.set_link(
        "a",
        "b",
        Link {
            partitioned: true,
            ..Link::default()
        },
    );

    let started = Instant::now();
    let refused = a.request("b", ADDR, request()).await.unwrap_err();
    assert!(
        matches!(refused, PeerError::Unavailable { .. }),
        "{refused:?}"
    );
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(echo.handled.load(Ordering::SeqCst), 0);
    // b to a is a separate link and still up, so b's request lands. a's
    // answer crosses the cut link and is lost.
    let unanswered = b.request("a", ADDR, request()).await.unwrap_err();
    assert!(
        matches!(unanswered, PeerError::Timeout { .. }),
        "{unanswered:?}"
    );

    net.heal();
    a.request("b", ADDR, request()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_lost_request_times_out_without_reaching_the_handler() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    net.set_link(
        "a",
        "b",
        Link {
            drop: 1.0,
            ..Link::default()
        },
    );

    let started = Instant::now();
    let lost = a.request("b", ADDR, request()).await.unwrap_err();

    assert!(matches!(lost, PeerError::Timeout { .. }), "{lost:?}");
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(echo.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_lost_answer_times_out_after_the_handler_ran() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    net.set_link(
        "b",
        "a",
        Link {
            drop: 1.0,
            ..Link::default()
        },
    );

    let lost = a.request("b", ADDR, request()).await.unwrap_err();

    // Applied and unacknowledged: the case a retry must not duplicate.
    assert!(matches!(lost, PeerError::Timeout { .. }), "{lost:?}");
    assert_eq!(echo.handled.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_delay_holds_each_direction_and_past_the_timeout_the_handler_still_runs() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    let slow = Link {
        delay: Duration::from_secs(2),
        ..Link::default()
    };
    net.set_link("a", "b", slow);
    net.set_link("b", "a", slow);

    let started = Instant::now();
    a.request("b", ADDR, request()).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(4));

    net.set_link(
        "a",
        "b",
        Link {
            delay: Duration::from_secs(6),
            ..Link::default()
        },
    );
    let late = a.request("b", ADDR, request()).await.unwrap_err();
    assert!(matches!(late, PeerError::Timeout { .. }), "{late:?}");
    // The request was still in flight when the caller gave up, and lands.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(echo.handled.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn an_ended_incarnation_is_refused_and_its_late_answers_are_lost() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo {
        takes: Duration::from_secs(1),
        ..Echo::default()
    });
    let (a, b) = pair(&net, &echo);

    // b dies while answering: the caller loses its connection.
    let sent = tokio::spawn(async move { a.request("b", ADDR, request()).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    net.crash("b");
    let cut = sent.await.unwrap().unwrap_err();
    assert!(matches!(cut, PeerError::Disconnected { .. }), "{cut:?}");

    // A dead node cannot be reached, and its own requester is refused.
    let a = net.start(node("a", PeerCapabilities::NONE, &Arc::default()));
    let down = a.request("b", ADDR, request()).await.unwrap_err();
    assert!(matches!(down, PeerError::Handshake { .. }), "{down:?}");
    let refused = b.request("a", ADDR, request()).await.unwrap_err();
    assert!(matches!(refused, PeerError::ShuttingDown), "{refused:?}");

    // Restarting b does not bring the old requester back.
    let b2 = net.start(node("b", PeerCapabilities::NONE, &echo));
    a.request("b", ADDR, request()).await.unwrap();
    let refused = b.request("a", ADDR, request()).await.unwrap_err();
    assert!(matches!(refused, PeerError::ShuttingDown), "{refused:?}");
    assert_eq!(b2.node_id(), "b");
}

#[tokio::test(start_paused = true)]
async fn a_request_the_target_never_offered_is_not_sent() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    let fence = InternalMessage::Fence(Fence {
        correlation_id: 1,
        shard: ShardRef {
            tenant_id: "t".to_string(),
            namespace: "n".to_string(),
            stream: "s".to_string(),
            shard: 0,
            generation: 1,
            incarnation: None,
        },
        log: ReplicaLog::Stream,
        election: None,
    });

    let refused = a.request("b", ADDR, fence).await.unwrap_err();

    assert!(
        matches!(refused, PeerError::Unsupported { .. }),
        "{refused:?}"
    );
    assert_eq!(echo.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn the_handler_hears_who_asked_only_where_it_keeps_ballots() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    a.request("b", ADDR, request()).await.unwrap();
    assert_eq!(*echo.asked_by.lock(), None);

    let _b = net.start(node("b", PeerCapabilities::BALLOTS, &echo));
    a.request("b", ADDR, request()).await.unwrap();
    assert_eq!(echo.asked_by.lock().as_deref(), Some("a"));
}

#[tokio::test(start_paused = true)]
async fn a_seed_replays_the_same_drops() {
    async fn outcomes(seed: u64) -> Vec<bool> {
        let net = SimNet::new(config(), seed);
        let (a, _b) = pair(&net, &Arc::default());
        net.set_link(
            "a",
            "b",
            Link {
                drop: 0.5,
                ..Link::default()
            },
        );
        let mut outcomes = Vec::new();
        for _ in 0..32 {
            outcomes.push(a.request("b", ADDR, request()).await.is_ok());
        }
        outcomes
    }

    let first = outcomes(42).await;
    assert_eq!(first, outcomes(42).await);
    assert!(first.contains(&true) && first.contains(&false), "{first:?}");
}

#[tokio::test(start_paused = true)]
async fn offers_follow_the_capabilities_it_started_with() {
    let net = SimNet::new(config(), 1);
    let plain = net.start(node("a", PeerCapabilities::NONE, &Arc::default()));
    assert!(!plain.offers_election() && !plain.offers_fence());

    let full = net.start(node(
        "a",
        felix_replication::promotion::REQUIRED.union(PeerCapabilities::ELECTION),
        &Arc::default(),
    ));
    assert!(full.offers_election() && full.offers_fence());
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_offered_learner_acks_reads_them() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let a = net.start(node("a", PeerCapabilities::LEARNER_ACKS, &Arc::default()));
    let _b = net.start(node("b", PeerCapabilities::NONE, &echo));

    assert_eq!(a.request("b", ADDR, request()).await.unwrap(), ack(7, true));
}

#[tokio::test(start_paused = true)]
async fn a_ping_is_answered_without_the_handler() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);

    let answer = a
        .request("b", ADDR, InternalMessage::Ping(Ping { correlation_id: 3 }))
        .await
        .unwrap();

    assert_eq!(answer, InternalMessage::Pong(Pong { correlation_id: 3 }));
    assert_eq!(echo.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_restart_while_a_request_crosses_loses_the_connection() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    net.set_link(
        "a",
        "b",
        Link {
            delay: Duration::from_millis(100),
            ..Link::default()
        },
    );

    let sent = tokio::spawn(async move { a.request("b", ADDR, request()).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    net.crash("b");
    let _b2 = net.start(node("b", PeerCapabilities::NONE, &echo));

    // The new incarnation never sees a request sent to the old one.
    let cut = sent.await.unwrap().unwrap_err();
    assert!(matches!(cut, PeerError::Disconnected { .. }), "{cut:?}");
    assert_eq!(echo.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_partition_that_lands_while_the_answer_crosses_loses_it() {
    let net = SimNet::new(config(), 1);
    let echo = Arc::new(Echo::default());
    let (a, _b) = pair(&net, &echo);
    net.set_link(
        "b",
        "a",
        Link {
            delay: Duration::from_secs(1),
            ..Link::default()
        },
    );

    let sent = tokio::spawn(async move { a.request("b", ADDR, request()).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    net.partition("a", "b");

    let lost = sent.await.unwrap().unwrap_err();
    assert!(matches!(lost, PeerError::Timeout { .. }), "{lost:?}");
    assert_eq!(echo.handled.load(Ordering::SeqCst), 1);
}

#[test]
fn nearby_seeds_draw_different_drops() {
    assert_ne!(splitmix(2), splitmix(3));
    assert_ne!(splitmix(0), 0);
}
