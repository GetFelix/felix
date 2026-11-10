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
fn record_generations_is_implemented_and_needs_nothing_first() {
    assert!(IMPLEMENTED.contains(&RECORD_GENERATIONS));
    assert!(prerequisites(RECORD_GENERATIONS.name()).is_empty());
}

#[test]
fn record_headers_is_implemented() {
    assert!(IMPLEMENTED.contains(&RECORD_HEADERS));
    assert!(
        FleetGate::for_this_build()
            .reported()
            .contains("record_headers")
    );
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

#[test]
fn prerequisites_come_first_and_carry_their_own() {
    for (at, feature) in IMPLEMENTED.iter().enumerate() {
        for needed in prerequisites(feature.name()) {
            let before = IMPLEMENTED.iter().position(|f| f == needed);
            assert!(
                before.is_some_and(|before| before < at),
                "{needed} is listed after {feature}, which needs it"
            );
            for transitive in prerequisites(needed.name()) {
                assert!(
                    prerequisites(feature.name()).contains(transitive),
                    "{feature} needs {needed}, so it also needs {transitive}"
                );
            }
        }
    }
    assert_eq!(prerequisites(MAJORITY_ACK.name()), &[GENERATION_START]);
    assert_eq!(
        prerequisites(LEASE_FREE_READS.name()),
        &[GENERATION_START, MAJORITY_ACK]
    );
}

#[test]
fn a_view_is_adopted_only_after_a_registration_answer() {
    let gate = FleetGate::new([MAJORITY_ACK.name()]);
    gate.observe([MAJORITY_ACK.name()]);
    let view = gate.view();
    assert!(view.supports(MAJORITY_ACK));
    assert!(!view.adopted(), "a heartbeat is not a registration");

    gate.restart([MAJORITY_ACK.name()]);
    let view = gate.view();
    assert!(view.adopted());
    assert!(view.supports(MAJORITY_ACK));
    assert!(!view.supports(GENERATION_START));
}

/// Declared so the control plane knows their prerequisites, but no build
/// reports them yet: a finalize finds no broker that has them and is refused.
#[test]
fn election_features_are_declared_and_not_implemented() {
    for feature in [
        JOINT_FENCE,
        REPLICA_ELECTION,
        PEER_ELECTION,
        START_HOLDERS,
        NON_VOTING,
    ] {
        assert!(!IMPLEMENTED.contains(&feature), "{feature}");
        assert!(
            !FleetGate::for_this_build()
                .reported()
                .contains(feature.name())
        );
    }
    assert_eq!(
        prerequisites(JOINT_FENCE.name()),
        &[GENERATION_START, STREAM_INCARNATIONS, CACHE_INCARNATIONS]
    );
    assert_eq!(
        prerequisites(REPLICA_ELECTION.name()),
        &[
            GENERATION_START,
            MAJORITY_ACK,
            LEASE_FREE_READS,
            STREAM_INCARNATIONS,
            CACHE_INCARNATIONS,
            JOINT_FENCE
        ]
    );
}

#[test]
fn a_seeded_gate_knows_what_it_was_seeded_with_and_nothing_else() {
    let reported = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        HEAD_TRIM.name(),
    ];
    // `fenced_caches` is persisted but not reported: a gate only enables what
    // this broker reports.
    let gate = FleetGate::seeded(reported, [GENERATION_START.name(), FENCED_CACHES.name()]);
    let view = gate.view();
    assert!(!view.adopted(), "a seed is not a registration");
    assert_eq!(view.decides(GENERATION_START), Some(true));
    // Not persisted: it may have been finalized since, so it is undecided,
    // though a plain `supports` reads it as off.
    assert_eq!(view.decides(MAJORITY_ACK), None);
    assert!(!view.supports(MAJORITY_ACK));
    // Not reported: the fleet never enables it here.
    assert_eq!(view.decides(FENCED_CACHES), Some(false));
    assert_eq!(gate.enabled(), vec![GENERATION_START.name()]);

    // A registration's answer is exact and decides everything.
    gate.restart([MAJORITY_ACK.name()]);
    let view = gate.view();
    assert!(view.adopted());
    assert_eq!(view.decides(MAJORITY_ACK), Some(true));
    assert_eq!(view.decides(GENERATION_START), Some(false));
    assert_eq!(view.decides(HEAD_TRIM), Some(false));
}

#[test]
fn an_unseeded_gate_decides_only_once_adopted() {
    let gate = FleetGate::new([MAJORITY_ACK.name()]);
    assert_eq!(gate.view().decides(MAJORITY_ACK), None);
    gate.restart(std::iter::empty());
    assert_eq!(gate.view().decides(MAJORITY_ACK), Some(false));
}

/// How a broker that seeded its gate but has not registered may read each
/// feature it was not seeded with as off (see [`IMPLEMENTED`]). A feature
/// added without a place here fails this test.
#[test]
fn every_feature_is_safe_to_read_as_off_before_registration() {
    enum Off {
        /// Seeded whenever `replica_election` is, which a pass needs before
        /// it decides on a seeded gate (`Serving::decided`).
        ElectionPrerequisite,
        /// Off refuses, or keeps a format every broker still reads.
        FailsClosed,
        /// Read per shard: `Serving::decided` for `fenced_caches`.
        CheckedPerShard,
        /// Read by the control plane only, or a report marker no gate reads.
        NotRead,
    }
    let class = |feature: FleetFeature| match feature.name() {
        "generation_start"
        | "majority_ack"
        | "lease_free_reads"
        | "joint_fence"
        | "stream_incarnations"
        | "cache_incarnations" => Off::ElectionPrerequisite,
        "head_trim"
        | "atomic_commit"
        | "publisher_principal"
        | "group_cursor_dependencies"
        // Off, a clustered broker refuses records with headers.
        | "record_headers"
        // Off, a log stays on the format every broker reads.
        | "record_generations"
        // Off, a broker does not stand or re-send kept sets.
        | "replica_election" => Off::FailsClosed,
        "jump_hash_routing" | "peer_election" | "start_holders" | "non_voting" => Off::NotRead,
        "fenced_caches" => Off::CheckedPerShard,
        other => panic!("classify `{other}` before a broker can read it as off unregistered"),
    };
    // Everything a broker reports, so everything a gate can enable.
    for feature in IMPLEMENTED
        .iter()
        .chain(ELECTION_REPORTED)
        .chain([&START_HOLDERS, &NON_VOTING])
        .copied()
    {
        if matches!(class(feature), Off::ElectionPrerequisite) {
            assert!(
                prerequisites(REPLICA_ELECTION.name()).contains(&feature),
                "{feature} is not a prerequisite of replica_election",
            );
        }
    }
    for feature in prerequisites(REPLICA_ELECTION.name()) {
        assert!(matches!(class(*feature), Off::ElectionPrerequisite));
    }
}
