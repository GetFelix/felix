use super::*;

const WINDOW: Duration = Duration::from_secs(90);

#[test]
fn the_clock_plan_is_a_function_of_the_seed() {
    let plan = |seed| ClockPlan::generate(seed, ClockSkew::Bounded, 4, WINDOW);
    assert_eq!(plan(7), plan(7));
    assert_ne!(plan(7), plan(8));
}

#[test]
fn off_skews_nothing() {
    let plan = ClockPlan::generate(7, ClockSkew::Off, 4, WINDOW);
    assert!(plan.steps.is_empty());
    assert_eq!(plan.initial.brokers, vec![NodeClock::default(); 4]);
    assert_eq!(plan.initial.controlplane, NodeClock::default());
}

fn rates_and_offsets(plan: &ClockPlan) -> (Vec<u32>, Vec<i64>) {
    let mut rates = Vec::new();
    let mut offsets = Vec::new();
    for clock in std::iter::once(&plan.initial.controlplane).chain(&plan.initial.brokers) {
        rates.push(clock.rate_ppm());
        offsets.push(clock.wall_offset_ms());
    }
    for step in &plan.steps {
        match step.change {
            ClockChange::Rate { ppm } => rates.push(ppm),
            ClockChange::StepWall { to_ms } => offsets.push(to_ms),
        }
    }
    (rates, offsets)
}

#[test]
fn bounded_skew_stays_inside_the_documented_bounds() {
    for seed in 0..100 {
        for brokers in 3..=5 {
            let plan = ClockPlan::generate(seed, ClockSkew::Bounded, brokers, WINDOW);
            assert!(!plan.steps.is_empty());
            let mut last = Duration::ZERO;
            for step in &plan.steps {
                assert!(step.at < WINDOW && step.at >= last, "{step:?}");
                last = step.at;
                if let Owner::Broker(index) = step.owner {
                    assert!(index < brokers);
                }
            }
            let (rates, offsets) = rates_and_offsets(&plan);
            for rate in rates {
                assert!(rate.abs_diff(PPM) <= BOUNDED_RATE_PPM, "{rate}");
            }
            for offset in offsets {
                assert!(offset.unsigned_abs() <= BOUNDED_WALL_MS, "{offset}");
            }
        }
    }
}

#[test]
fn unbounded_skew_goes_past_the_bounds() {
    let (rates, offsets) =
        rates_and_offsets(&ClockPlan::generate(3, ClockSkew::Unbounded, 5, WINDOW));
    for rate in &rates {
        assert!(rate.abs_diff(PPM) > PPM / 4, "{rate}");
    }
    assert!(rates.iter().any(|rate| *rate < PPM) && rates.iter().any(|rate| *rate > PPM));
    assert!(
        offsets
            .iter()
            .any(|offset| offset.unsigned_abs() > BOUNDED_WALL_MS)
    );
}

#[test]
fn modes_parse_by_name() {
    for skew in [ClockSkew::Off, ClockSkew::Bounded, ClockSkew::Unbounded] {
        assert_eq!(ClockSkew::parse(skew.name()), Some(skew));
    }
    assert_eq!(ClockSkew::parse("sideways"), None);
}
