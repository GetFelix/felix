use super::*;

/// An upstream that echoes every datagram back to whoever sent it.
async fn echo() -> SocketAddr {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = socket.local_addr().expect("echo addr");
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        while let Ok((len, from)) = socket.recv_from(&mut buf).await {
            let _ = socket.send_to(&buf[..len], from).await;
        }
    });
    addr
}

async fn client() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").await.expect("bind client")
}

/// Send `payload` through the interposer and wait up to `wait` for the echo.
async fn round_trip(socket: &UdpSocket, to: SocketAddr, payload: &[u8], wait: Duration) -> bool {
    socket.send_to(payload, to).await.expect("send");
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let deadline = Instant::now() + wait;
    loop {
        match tokio::time::timeout_at(deadline, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) if &buf[..len] == payload => return true,
            // An echo of something sent earlier, released late.
            Ok(Ok(_)) => continue,
            _ => return false,
        }
    }
}

const QUICK: Duration = Duration::from_millis(300);

#[tokio::test]
async fn forwards_both_ways_when_healthy() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    assert!(round_trip(&socket, link.addr(), b"hello", QUICK).await);
}

#[tokio::test]
async fn a_drop_discards_until_it_heals() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    assert!(round_trip(&socket, link.addr(), b"before", QUICK).await);

    link.inject(LinkFault::Drop, Duration::from_secs(60));
    assert!(!round_trip(&socket, link.addr(), b"during", QUICK).await);
    // A new flow does not get through a drop either.
    assert!(!round_trip(&client().await, link.addr(), b"fresh", QUICK).await);

    link.heal();
    assert!(round_trip(&socket, link.addr(), b"after", QUICK).await);
}

#[tokio::test]
async fn a_drop_heals_on_its_own_after_the_hold() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    link.inject(LinkFault::Drop, Duration::from_millis(200));
    assert!(!round_trip(&socket, link.addr(), b"during", Duration::from_millis(100)).await);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(round_trip(&socket, link.addr(), b"after", QUICK).await);
}

#[tokio::test]
async fn a_stall_delivers_late_and_in_order() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    assert!(round_trip(&socket, link.addr(), b"warm", QUICK).await);

    let hold = Duration::from_millis(400);
    link.inject(LinkFault::Stall, hold);
    let sent = Instant::now();
    for index in 0u8..5 {
        socket.send_to(&[index], link.addr()).await.expect("send");
    }
    let mut buf = vec![0u8; MAX_DATAGRAM];
    for index in 0u8..5 {
        let (len, _) = tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
            .await
            .expect("stalled datagram never arrived")
            .expect("recv");
        assert_eq!(&buf[..len], &[index], "a stall reordered datagrams");
    }
    assert!(
        sent.elapsed() >= hold,
        "a stall let a datagram through early"
    );
}

#[tokio::test]
async fn a_reset_kills_open_flows_and_lets_new_ones_through() {
    let link = Interposer::start(echo().await).await.expect("start");
    let old = client().await;
    assert!(round_trip(&old, link.addr(), b"before", QUICK).await);

    link.inject(LinkFault::Reset, Duration::ZERO);
    assert!(!round_trip(&old, link.addr(), b"after", QUICK).await);
    // Healing ends drops and stalls, not a reset.
    link.heal();
    assert!(!round_trip(&old, link.addr(), b"healed", QUICK).await);

    let fresh = client().await;
    assert!(round_trip(&fresh, link.addr(), b"fresh", QUICK).await);
}

/// A QUIC v1 Initial header with `dcid`, padded to look like a real packet.
fn initial(dcid: &[u8]) -> Vec<u8> {
    let mut packet = vec![0xc0, 0, 0, 0, 1, dcid.len() as u8];
    packet.extend_from_slice(dcid);
    packet.extend_from_slice(b"payload");
    packet
}

#[tokio::test]
async fn a_new_initial_on_a_reset_source_port_gets_through() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    let first = initial(b"old-dcid");
    assert!(round_trip(&socket, link.addr(), &first, QUICK).await);

    link.inject(LinkFault::Reset, Duration::ZERO);
    // The killed connection stays dead, including a retransmitted Initial and
    // anything that is not an Initial.
    assert!(!round_trip(&socket, link.addr(), &first, QUICK).await);
    assert!(!round_trip(&socket, link.addr(), b"short header", QUICK).await);

    // A new connection on the same port starts a fresh flow.
    let second = initial(b"new-dcid");
    assert!(round_trip(&socket, link.addr(), &second, QUICK).await);
    assert!(round_trip(&socket, link.addr(), b"short header", QUICK).await);
}

#[tokio::test]
async fn a_reset_still_kills_a_live_flow_with_a_new_initial_during_a_drop() {
    let link = Interposer::start(echo().await).await.expect("start");
    let socket = client().await;
    assert!(round_trip(&socket, link.addr(), &initial(b"one"), QUICK).await);
    link.inject(LinkFault::Reset, Duration::ZERO);
    link.inject(LinkFault::Drop, Duration::from_secs(60));
    // The fresh flow is still subject to the drop.
    assert!(!round_trip(&socket, link.addr(), &initial(b"two"), QUICK).await);
}
