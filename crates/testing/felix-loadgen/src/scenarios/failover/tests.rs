use super::{Outage, analyze};

fn steady(seq0: u64, from_ms: f64, to_ms: f64) -> Vec<(u64, f64)> {
    let mut v = Vec::new();
    let (mut s, mut t) = (seq0, from_ms);
    while t <= to_ms {
        v.push((s, t));
        s += 1;
        t += 1.0;
    }
    v
}

#[test]
fn finds_the_longest_silence_and_the_catch_up() {
    let mut acks = steady(1, 0.0, 100.0);
    acks.extend(steady(102, 600.0, 700.0)); // 500 ms silence after the ack at 100
    let mut deliveries = steady(1, 1.0, 100.0);
    // the subscriber reconnects and sees the first post-outage record at 900
    deliveries.push((102, 900.0));
    let got = analyze(&acks, &deliveries).expect("outage");
    assert_eq!(
        got,
        Outage {
            gap_start_ms: 100.0,
            first_commit_ms: 500.0,
            caught_up_ms: Some(800.0),
        }
    );
}

#[test]
fn a_subscriber_that_never_recovers_is_none() {
    let mut acks = steady(1, 0.0, 10.0);
    acks.extend(steady(50, 200.0, 210.0));
    let deliveries = steady(1, 1.0, 10.0);
    assert_eq!(
        analyze(&acks, &deliveries).expect("outage").caught_up_ms,
        None
    );
}

#[test]
fn needs_two_acks() {
    assert!(analyze(&[(1, 0.0)], &[]).is_none());
}
