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
fn measures_from_the_kill_and_not_the_last_ack() {
    let mut acks = steady(1, 0.0, 100.0);
    acks.extend(steady(102, 600.0, 700.0));
    let mut deliveries = steady(1, 1.0, 100.0);
    deliveries.push((102, 900.0));
    let got = analyze(&acks, &deliveries, 120.0).expect("outage");
    assert_eq!(
        got,
        Outage {
            kill_ms: 120.0,
            first_commit_ms: 480.0,
            caught_up_ms: Some(780.0),
        }
    );
}

#[test]
fn a_stall_before_the_kill_is_not_the_outage() {
    // 2 s silence at 100 ms (a natural stall), then steady, then the kill at
    // 3000 ms with a 300 ms outage.
    let mut acks = steady(1, 0.0, 100.0);
    acks.extend(steady(102, 2100.0, 3000.0));
    acks.extend(steady(1100, 3300.0, 3400.0));
    let got = analyze(&acks, &[], 3000.5).expect("outage");
    assert_eq!(got.first_commit_ms, 299.5);
}

#[test]
fn a_subscriber_that_never_recovers_is_none() {
    let mut acks = steady(1, 0.0, 10.0);
    acks.extend(steady(50, 200.0, 210.0));
    let deliveries = steady(1, 1.0, 10.0);
    assert_eq!(
        analyze(&acks, &deliveries, 10.5)
            .expect("outage")
            .caught_up_ms,
        None
    );
}

#[test]
fn no_ack_after_the_kill_is_none() {
    assert!(analyze(&steady(1, 0.0, 10.0), &[], 50.0).is_none());
}
