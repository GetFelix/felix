use super::*;

const SEC: Duration = Duration::from_secs(1);

#[test]
fn a_lease_clock_runs_at_its_rate_and_keeps_its_drift_across_a_change() {
    let mut clock = NodeClock::new(0, PPM / 2);
    assert_eq!(clock.lease(10 * SEC), 5 * SEC);
    clock.set_rate(10 * SEC, PPM * 2);
    // Five seconds of drift kept, then twice as fast.
    assert_eq!(clock.lease(10 * SEC), 5 * SEC);
    assert_eq!(clock.lease(13 * SEC), 11 * SEC);
}

#[test]
fn a_lease_clock_never_goes_backwards() {
    let mut clock = NodeClock::new(-5_000, PPM / 4);
    let mut last = Duration::ZERO;
    for millis in 0..20_000u64 {
        let base = Duration::from_millis(millis);
        if millis % 1_000 == 0 {
            clock.set_rate(base, (millis as u32 % 7) * PPM / 3);
            clock.step_wall(-(millis as i64));
        }
        let now = clock.lease(base);
        assert!(now >= last, "{now:?} < {last:?} at {base:?}");
        last = now;
    }
}

#[test]
fn a_wall_clock_steps_either_way_and_does_not_drift() {
    let mut clock = NodeClock::new(1_500, PPM * 3);
    assert_eq!(clock.wall(10 * SEC), Duration::from_millis(11_500));
    clock.step_wall(-2_000);
    assert_eq!(clock.wall(10 * SEC), 8 * SEC);
    // Clamped at the shared zero rather than negative.
    assert_eq!(clock.wall(SEC), Duration::ZERO);
    // The wall reading is the lease reading plus what the seam adds to it.
    let base = 30 * SEC;
    let wall = clock.lease(base).as_nanos() as i128 + clock.wall_past_lease(base);
    assert_eq!(wall, clock.wall(base).as_nanos() as i128);
}

#[test]
fn a_node_id_names_its_broker() {
    assert_eq!(Owner::of_node("b1"), Some(Owner::Broker(0)));
    assert_eq!(Owner::of_node("b5"), Some(Owner::Broker(4)));
    assert_eq!(Owner::of_node("b0"), None);
    assert_eq!(Owner::of_node("cp"), None);
}

fn skewed() -> Clocks {
    Clocks {
        controlplane: NodeClock::new(-700, PPM),
        brokers: vec![NodeClock::new(300, PPM * 2), NodeClock::default()],
    }
}

/// What `felix_common::clock` reads now, as (lease, wall) from the shared zero.
fn readings() -> (Duration, Duration) {
    let wall = felix_common::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH + felix_common::clock::sim::EPOCH)
        .unwrap();
    (felix_common::clock::boottime(), wall)
}

#[tokio::test(start_paused = true)]
async fn each_owner_reads_its_own_clock_through_felix_common() {
    install(skewed());
    tokio::time::advance(10 * SEC).await;
    assert_eq!(
        readings(),
        (10 * SEC, 10 * SEC),
        "the harness reads the shared clock"
    );
    assert_eq!(
        as_owner(Owner::Broker(0), readings),
        (20 * SEC, Duration::from_millis(10_300))
    );
    assert_eq!(
        as_owner(Owner::ControlPlane, readings),
        (10 * SEC, Duration::from_millis(9_300))
    );
    assert_eq!(
        on(Owner::Broker(1), async { readings() }).await,
        (10 * SEC, 10 * SEC)
    );
    // Back to the harness once the scope ends.
    assert_eq!(readings(), (10 * SEC, 10 * SEC));

    let changed = change(Owner::Broker(0), |clock, base| {
        clock.set_rate(base, PPM);
        clock.step_wall(-1_000);
    });
    assert_eq!(changed.rate_ppm(), PPM);
    tokio::time::advance(10 * SEC).await;
    assert_eq!(as_owner(Owner::Broker(0), readings), (30 * SEC, 19 * SEC));
}

/// A task spawned by a broker's code reads the broker's clock, as do the
/// tasks it spawns in turn, while a task the harness spawns does not.
#[cfg(tokio_unstable)]
#[test]
fn spawned_tasks_read_the_clock_of_the_code_that_spawned_them() {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    attribute_tasks(&mut builder);
    let runtime = builder.build().unwrap();
    runtime.block_on(async {
        install(skewed());
        tokio::time::advance(10 * SEC).await;
        let nested = as_owner(Owner::Broker(0), || {
            tokio::spawn(async {
                tokio::time::sleep(SEC).await;
                tokio::spawn(async { readings() }).await.unwrap()
            })
        });
        let harness = tokio::spawn(async { readings() });
        assert_eq!(
            nested.await.unwrap(),
            (22 * SEC, Duration::from_millis(11_300))
        );
        assert_eq!(harness.await.unwrap(), (10 * SEC, 10 * SEC));
    });
}
