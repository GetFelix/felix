use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use super::paced::gap;
use super::subscribe::{Delivery, parse_publish, read_packet, subscribe_packet};
use super::{
    Common, connect_packet, mqtt_publish, mqtt_subscribe, publish_packet, remaining_length,
};

#[test]
fn remaining_length_uses_the_continuation_bit() {
    for (n, bytes) in [
        (0, vec![0]),
        (127, vec![127]),
        (128, vec![0x80, 1]),
        (16_384, vec![0x80, 0x80, 1]),
    ] {
        let mut out = Vec::new();
        remaining_length(n, &mut out);
        assert_eq!(out, bytes, "{n}");
    }
}

#[test]
fn connect_is_a_clean_311_session() {
    let p = connect_packet("a", None);
    assert_eq!(p[0], 0x10);
    assert_eq!(&p[2..10], b"\0\x04MQTT\x04\x02");
    assert_eq!(p[1] as usize, p.len() - 2);
}

#[test]
fn publish_is_qos1_with_the_packet_id_after_the_topic() {
    let mut out = Vec::new();
    publish_packet("t/1", 7, b"xy", &mut out);
    assert_eq!(out, [0x32, 9, 0, 3, b't', b'/', b'1', 0, 7, b'x', b'y']);
}

#[test]
fn connect_with_credentials_sends_username_then_password() {
    let p = connect_packet("a", Some(("t", "jwt")));
    assert_eq!(p[9], 0xc2);
    assert_eq!(p[1] as usize, p.len() - 2);
    assert!(p.ends_with(b"\0\x01a\0\x01t\0\x03jwt"));
}

#[test]
fn subscribe_is_qos1_on_one_filter() {
    assert_eq!(
        subscribe_packet(1, "b/#"),
        [0x82, 8, 0, 1, 0, 3, b'b', b'/', b'#', 1]
    );
}

#[test]
fn a_qos1_delivery_carries_the_id_to_acknowledge() {
    let mut out = Vec::new();
    publish_packet("t/1", 7, b"xyz", &mut out);
    // Skip the fixed header: first byte and one length byte.
    let d = parse_publish(out[0], &out[2..]).unwrap();
    assert_eq!(
        d,
        Delivery {
            ack_id: Some(7),
            payload_len: 3
        }
    );
}

#[test]
fn a_qos0_delivery_has_nothing_to_acknowledge() {
    let body = [0, 1, b't', b'a', b'b'];
    let d = parse_publish(0x30, &body).unwrap();
    assert_eq!(
        d,
        Delivery {
            ack_id: None,
            payload_len: 2
        }
    );
    assert!(parse_publish(0x32, &body[..3]).is_err());
}

#[test]
fn the_pacing_gap_is_the_connections_share_of_the_rate() {
    assert_eq!(gap(250.0), Duration::from_millis(4));
}

fn common(addr: &str) -> Common {
    let mut c = Common::new(vec![], "t".into(), "default".into(), "jwt".into());
    c.prose = false;
    c.payload_bytes = 64;
    let _ = addr;
    c
}

/// A broker that accepts everyone, acknowledges every publish, and after a
/// SUBSCRIBE sends `deliveries` QoS 1 messages (or refuses it).
async fn fake_broker(deliveries: usize, refuse: bool) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (first, _) = read_packet(&mut s).await.unwrap();
                assert_eq!(first, 0x10);
                s.write_all(&[0x20, 2, 0, 0]).await.unwrap();
                while let Ok((first, body)) = read_packet(&mut s).await {
                    match first >> 4 {
                        3 => {
                            let id = parse_publish(first, &body).unwrap().ack_id.unwrap();
                            s.write_all(&[0x40, 2, (id >> 8) as u8, id as u8])
                                .await
                                .unwrap();
                        }
                        8 => {
                            let rc = if refuse { 0x80 } else { 1 };
                            s.write_all(&[0x90, 3, body[0], body[1], rc]).await.unwrap();
                            for id in 1..=deliveries {
                                send(&mut s, id as u16).await;
                            }
                        }
                        _ => {}
                    }
                }
            });
        }
    });
    addr
}

async fn send(s: &mut TcpStream, id: u16) {
    let mut out = Vec::new();
    publish_packet("bench/c0", id, &[7u8; 64], &mut out);
    s.write_all(&out).await.unwrap();
}

#[tokio::test]
async fn a_paced_publisher_holds_its_rate_and_reports_it() {
    let addr = fake_broker(0, false).await;
    let mut c = common(&addr);
    c.concurrency = 2;
    let json = mqtt_publish(
        &c,
        &[addr],
        "bench/",
        4,
        Duration::from_secs(1),
        Some(400),
        None,
    )
    .await
    .unwrap();
    let acked = json["acked_records"].as_u64().unwrap();
    assert!((380..=420).contains(&acked), "acked {acked}");
    assert_eq!(json["target_rate_msg_s"], 400);
    assert!(json["throughput_msg_s"].as_f64().unwrap() > 380.0);
}

#[tokio::test]
async fn subscribers_count_every_delivery_in_the_window() {
    let addr = fake_broker(50, false).await;
    let mut c = common(&addr);
    c.fanout = 2;
    let json = mqtt_subscribe(&c, &[addr], "bench/", Duration::from_secs(1), None)
        .await
        .unwrap();
    assert_eq!(json["received"], 100);
    assert_eq!(json["delivered_throughput_msg_s"], 100.0);
}

#[tokio::test]
async fn a_refused_subscribe_fails_the_run() {
    let addr = fake_broker(0, true).await;
    let c = common(&addr);
    let err = mqtt_subscribe(&c, &[addr], "bench/", Duration::from_secs(1), None)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("refused"), "{err:#}");
}
