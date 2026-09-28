use super::*;

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

const ROUTING: FleetFeature = FleetFeature::new("routing");
const FENCE: FleetFeature = FleetFeature::new("fence");

#[test]
fn the_minimum_is_what_every_report_includes() {
    let reports = [
        set(&["a", "b", "c"]),
        set(&["b", "c"]),
        set(&["c", "b", "d"]),
    ];
    assert_eq!(minimum(&reports), set(&["b", "c"]));
}

#[test]
fn a_broker_that_reports_nothing_holds_every_feature_back() {
    // An older broker sends no set at all, which reads as the empty one.
    let reports = [set(&["a", "b"]), set(&[])];
    assert!(minimum(&reports).is_empty());
}

#[test]
fn no_reports_means_no_features() {
    assert!(minimum(std::iter::empty::<&BTreeSet<String>>()).is_empty());
}

#[test]
fn leaving_can_only_raise_the_minimum_and_joining_only_lower_it() {
    let old = set(&["a"]);
    let new = set(&["a", "b"]);
    let with_old = minimum([&new, &new, &old]);
    let without_old = minimum([&new, &new]);
    assert_eq!(with_old, set(&["a"]));
    assert_eq!(without_old, set(&["a", "b"]));
    assert!(with_old.is_subset(&without_old));
}

#[test]
fn missing_names_what_a_joiner_lacks() {
    let required = set(&["a", "b", "c"]);
    assert_eq!(missing(&required, &set(&["b"])), vec!["a", "c"]);
    assert!(missing(&required, &set(&["a", "b", "c", "d"])).is_empty());
}

#[test]
fn a_gate_enables_only_what_the_fleet_has_in_common() {
    let gate = FleetGate::new(["routing", "fence"]);
    assert!(!gate.supports(ROUTING));
    assert_eq!(gate.observe(["routing"]), vec!["routing"]);
    assert!(gate.supports(ROUTING));
    assert!(!gate.supports(FENCE));
}

#[test]
fn a_gate_never_enables_a_feature_this_broker_did_not_report() {
    // Nothing is enabled unless every serving broker reported it, so an
    // answer that includes it is wrong, and trusting it would turn on behaviour
    // this build does not have.
    let gate = FleetGate::new(["routing"]);
    assert!(gate.observe(["fence", "routing"]).contains(&"routing"));
    assert!(!gate.supports(FENCE));
}

#[test]
fn a_lower_heartbeat_answer_does_not_turn_a_feature_off() {
    let gate = FleetGate::new(["routing"]);
    gate.observe(["routing"]);
    assert!(gate.observe(std::iter::empty()).is_empty());
    assert!(gate.supports(ROUTING));
}

#[test]
fn registering_again_starts_from_that_answer() {
    let gate = FleetGate::new(["routing", "fence"]);
    gate.observe(["routing", "fence"]);
    assert_eq!(gate.restart(["fence"]), vec!["routing"]);
    assert!(!gate.supports(ROUTING));
    assert!(gate.supports(FENCE));
}

#[test]
fn this_build_reports_what_it_implements() {
    let gate = FleetGate::for_this_build();
    let names: BTreeSet<String> = IMPLEMENTED.iter().map(|f| f.name().to_string()).collect();
    assert_eq!(gate.reported(), names);
}

#[test]
fn feature_names_are_snake_case() {
    for feature in IMPLEMENTED {
        assert!(
            feature
                .name()
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "{feature}"
        );
    }
}
