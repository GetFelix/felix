//! The proxies on their own, over loopback sockets, with no broker involved.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::owners::Resolver;
use super::rules::Rules;
use super::tcp::TcpProxy;
use super::udp::UdpProxy;
use crate::fault::Endpoint;

const QUIET: Duration = Duration::from_millis(300);

fn a() -> Endpoint {
    Endpoint::node("a")
}

fn b() -> Endpoint {
    Endpoint::node("b")
}

fn server() -> Endpoint {
    Endpoint::node("server")
}

/// A UDP server that echoes every datagram and counts them.
async fn udp_echo() -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = socket.local_addr().expect("echo addr");
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&seen);
    tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        while let Ok((len, from)) = socket.recv_from(&mut buf).await {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = socket.send_to(&buf[..len], from).await;
        }
    });
    (addr, seen)
}

/// Client sockets named by port, standing in for the process lookup.
fn resolver(names: HashMap<u16, Endpoint>) -> Resolver {
    Arc::new(move |source: SocketAddr| names.get(&source.port()).cloned())
}

async fn round_trip(socket: &UdpSocket, to: SocketAddr, budget: Duration) -> Option<Duration> {
    let started = Instant::now();
    socket.send_to(b"ping", to).await.expect("send");
    let mut buf = [0u8; 16];
    tokio::time::timeout(budget, socket.recv_from(&mut buf))
        .await
        .ok()
        .map(|_| started.elapsed())
}

struct UdpFixture {
    proxy: UdpProxy,
    rules: Arc<Rules>,
    seen: Arc<std::sync::atomic::AtomicUsize>,
    from_a: UdpSocket,
    from_b: UdpSocket,
    _shutdown: tokio_util::sync::DropGuard,
}

async fn udp_fixture() -> UdpFixture {
    let (upstream, seen) = udp_echo().await;
    let from_a = UdpSocket::bind("127.0.0.1:0").await.expect("bind a");
    let from_b = UdpSocket::bind("127.0.0.1:0").await.expect("bind b");
    let names = HashMap::from([
        (from_a.local_addr().expect("a addr").port(), a()),
        (from_b.local_addr().expect("b addr").port(), b()),
    ]);
    let rules = Arc::new(Rules::new());
    let shutdown = CancellationToken::new();
    let proxy = UdpProxy::start(
        &Handle::current(),
        server(),
        upstream,
        Arc::clone(&rules),
        resolver(names),
        shutdown.clone(),
    )
    .expect("start proxy");
    UdpFixture {
        proxy,
        rules,
        seen,
        from_a,
        from_b,
        _shutdown: shutdown.drop_guard(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_forwards_both_ways_when_nothing_is_faulted() {
    let fixture = udp_fixture().await;
    let to = fixture.proxy.addr();
    assert!(round_trip(&fixture.from_a, to, QUIET * 3).await.is_some());
    assert!(round_trip(&fixture.from_b, to, QUIET * 3).await.is_some());
}

/// Dropping `a -> server` loses a's datagrams and nobody else's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_drops_one_source_towards_the_target() {
    let fixture = udp_fixture().await;
    let to = fixture.proxy.addr();
    fixture.rules.set_dropped(&a(), &server(), true);

    assert!(round_trip(&fixture.from_a, to, QUIET).await.is_none());
    assert_eq!(fixture.seen.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(round_trip(&fixture.from_b, to, QUIET * 3).await.is_some());

    fixture.rules.set_dropped(&a(), &server(), false);
    assert!(round_trip(&fixture.from_a, to, QUIET * 3).await.is_some());
}

/// The asymmetric case: `server -> a` dropped means a's request arrives and
/// the reply does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_drops_replies_without_dropping_requests() {
    let fixture = udp_fixture().await;
    let to = fixture.proxy.addr();
    fixture.rules.set_dropped(&server(), &a(), true);

    assert!(round_trip(&fixture.from_a, to, QUIET).await.is_none());
    assert_eq!(
        fixture.seen.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the request should have reached the server",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_delays_one_direction() {
    let fixture = udp_fixture().await;
    let to = fixture.proxy.addr();
    let delay = Duration::from_millis(200);
    fixture.rules.set_delay(&a(), &server(), delay);

    let slow = round_trip(&fixture.from_a, to, QUIET * 3)
        .await
        .expect("a delayed datagram still arrives");
    assert!(slow >= delay, "round trip {slow:?} was not delayed");
    let fast = round_trip(&fixture.from_b, to, QUIET * 3)
        .await
        .expect("b is not delayed");
    assert!(fast < delay, "b's round trip {fast:?} was delayed too");
}

/// A broker started again listens somewhere new; the proxy follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_follows_a_moved_upstream() {
    let fixture = udp_fixture().await;
    let (moved, seen) = udp_echo().await;
    fixture.proxy.set_upstream(moved);
    assert!(
        round_trip(&fixture.from_a, fixture.proxy.addr(), QUIET * 3)
            .await
            .is_some()
    );
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// A TCP server that echoes and reports each chunk it read.
async fn tcp_echo() -> (SocketAddr, tokio::sync::mpsc::UnboundedReceiver<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = listener.local_addr().expect("echo addr");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                while let Ok(len) = stream.read(&mut buf).await {
                    if len == 0 {
                        return;
                    }
                    let _ = tx.send(len);
                    if stream.write_all(&buf[..len]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, rx)
}

async fn tcp_fixture() -> (
    TcpProxy,
    Arc<Rules>,
    tokio::sync::mpsc::UnboundedReceiver<usize>,
    tokio_util::sync::DropGuard,
) {
    let (upstream, reads) = tcp_echo().await;
    let rules = Arc::new(Rules::new());
    let shutdown = CancellationToken::new();
    let proxy = TcpProxy::start(
        &Handle::current(),
        a(),
        Endpoint::ControlPlane,
        upstream,
        Arc::clone(&rules),
        shutdown.clone(),
    )
    .expect("start proxy");
    (proxy, rules, reads, shutdown.drop_guard())
}

async fn echo_within(stream: &mut TcpStream, budget: Duration) -> Option<Duration> {
    let started = Instant::now();
    stream.write_all(b"ping").await.ok()?;
    let mut buf = [0u8; 4];
    match tokio::time::timeout(budget, stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) => Some(started.elapsed()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_black_holes_the_request_direction_then_closes_on_heal() {
    let (proxy, rules, mut reads, _guard) = tcp_fixture().await;
    let mut stream = TcpStream::connect(proxy.addr()).await.expect("connect");
    assert!(echo_within(&mut stream, QUIET * 3).await.is_some());
    let _ = reads.recv().await;

    rules.set_dropped(&a(), &Endpoint::ControlPlane, true);
    assert!(
        echo_within(&mut stream, QUIET).await.is_none(),
        "a black-holed request was answered",
    );
    assert!(reads.try_recv().is_err(), "the server saw dropped bytes");

    // Bytes were lost, so the stream cannot be trusted again: healing closes
    // it rather than resuming mid-stream.
    rules.set_dropped(&a(), &Endpoint::ControlPlane, false);
    let mut buf = [0u8; 4];
    let closed = tokio::time::timeout(QUIET * 3, stream.read(&mut buf)).await;
    assert!(
        matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
        "a connection that lost bytes stayed open after the heal",
    );

    let mut fresh = TcpStream::connect(proxy.addr()).await.expect("reconnect");
    assert!(echo_within(&mut fresh, QUIET * 3).await.is_some());
}

/// Replies lost, requests delivered: the server acts on what it never
/// answers, which is the partition a heartbeat can fall into.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_drops_replies_without_dropping_requests() {
    let (proxy, rules, mut reads, _guard) = tcp_fixture().await;
    rules.set_dropped(&Endpoint::ControlPlane, &a(), true);
    let mut stream = TcpStream::connect(proxy.addr()).await.expect("connect");

    assert!(echo_within(&mut stream, QUIET).await.is_none());
    assert_eq!(
        tokio::time::timeout(QUIET, reads.recv())
            .await
            .ok()
            .flatten(),
        Some(4),
        "the request should have reached the server",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_delays_one_direction() {
    let (proxy, rules, _reads, _guard) = tcp_fixture().await;
    let delay = Duration::from_millis(200);
    rules.set_delay(&Endpoint::ControlPlane, &a(), delay);
    let mut stream = TcpStream::connect(proxy.addr()).await.expect("connect");

    let took = echo_within(&mut stream, QUIET * 3)
        .await
        .expect("a delayed reply still arrives");
    assert!(took >= delay, "round trip {took:?} was not delayed");
}

/// Rules are per direction and per pair, and a cleared rule is gone.
#[test]
fn rules_are_directed() {
    use super::rules::Verdict;
    let rules = Rules::new();
    rules.set_dropped(&a(), &b(), true);
    assert_eq!(rules.verdict(Some(&a()), Some(&b())), Verdict::Drop);
    assert_eq!(
        rules.verdict(Some(&b()), Some(&a())),
        Verdict::Deliver(Duration::ZERO)
    );
    assert_eq!(
        rules.verdict(None, Some(&b())),
        Verdict::Deliver(Duration::ZERO),
        "an unattributed source is on no faulted link",
    );
    assert_eq!(rules.unattributed(), 1, "and it is counted");
    assert!(rules.severed(&b(), &a()));

    rules.set_dropped(&a(), &b(), false);
    assert!(!rules.severed(&a(), &b()));
}

/// The lookup the peer proxy relies on: a UDP port maps back to the process
/// holding it, and a port nobody tracked holds maps to nobody.
#[test]
fn a_source_port_resolves_to_the_process_holding_it() {
    let processes = super::owners::Processes::default();
    let held = std::net::UdpSocket::bind("0.0.0.0:0").expect("bind");
    let port = held.local_addr().expect("addr").port();
    let source: SocketAddr = ([127, 0, 0, 1], port).into();

    assert_eq!(
        processes.owner_of_udp(source),
        None,
        "nobody is tracked yet"
    );
    processes.set("me", Some(std::process::id()));
    assert_eq!(processes.owner_of_udp(source), Some(Endpoint::node("me")));

    drop(held);
    assert_eq!(processes.owner_of_udp(source), None);
}
