//! That a typo is named, and an unrelated variable is not guessed at.
use super::*;

#[test]
fn a_misspelling_is_matched_to_what_was_probably_meant() {
    assert_eq!(suggestions("FELIX_QUIC_BINDD"), vec!["FELIX_QUIC_BIND"]);
    assert_eq!(
        suggestions("FELIX_BROKER_METRICS_BNID"),
        vec!["FELIX_BROKER_METRICS_BIND"],
    );
}

/// The mistake worth catching most, because it is the one someone makes
/// without noticing: a plausible shorter name that does not exist.
///
/// `FELIX_METRICS_BIND` reads like the obvious name for the metrics listener
/// and is not it — the broker's is `FELIX_BROKER_METRICS_BIND`. Seven edits
/// apart, so distance alone will not find it; the segments are in order, so
/// the subsequence rule does.
#[test]
fn a_name_missing_a_segment_is_matched_to_the_full_one() {
    // Both, because the guess is genuinely ambiguous and picking one would be
    // inventing certainty. Falling through to edit distance instead answered
    // `FELIX_QUIC_BIND`, which is a different listener.
    assert_eq!(
        suggestions("FELIX_METRICS_BIND"),
        vec![
            "FELIX_BROKER_METRICS_BIND",
            "FELIX_CONTROLPLANE_METRICS_BIND"
        ],
    );
}

#[test]
fn a_name_nothing_resembles_suggests_nothing() {
    // Suggesting the nearest arbitrary string is worse than suggesting
    // nothing: it sends the reader after a setting that has no bearing on
    // what they were doing.
    assert!(suggestions("FELIX_SOMETHING_ENTIRELY_UNRELATED").is_empty());
}

#[test]
fn a_real_name_is_not_reported() {
    for name in KNOWN_VARS {
        assert!(
            !unrecognised().iter().any(|(found, _)| found == name),
            "{name} is read by the code and was reported as unknown",
        );
    }
}

#[test]
fn the_registry_has_no_duplicates_and_is_sorted() {
    // Sorted so a diff adding one is readable, and unique so a rename that
    // leaves the old name behind shows up as a conflict rather than a silent
    // second entry.
    let mut sorted = KNOWN_VARS.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        KNOWN_VARS.len(),
        "the registry has duplicate entries",
    );
    assert_eq!(sorted, KNOWN_VARS, "the registry is not sorted");
}

#[test]
fn every_name_looks_like_one() {
    for name in KNOWN_VARS {
        assert!(
            name.starts_with("FELIX_"),
            "{name} is not a FELIX_ variable"
        );
        assert!(
            name.chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
            "{name} is not shaped like an environment variable",
        );
    }
}

/// A retired name is neither known nor unknown: it is reported as retired,
/// so an operator learns it does nothing instead of being told of a typo.
#[test]
fn a_retired_name_is_reported_as_retired() {
    let mut sorted = RETIRED_VARS.to_vec();
    sorted.sort();
    assert_eq!(sorted, RETIRED_VARS, "the retired list is not sorted");
    for name in RETIRED_VARS {
        assert!(
            !KNOWN_VARS.contains(name),
            "{name} is both known and retired"
        );
    }
    // Set by no other test in this crate, and read only through the registry.
    unsafe {
        std::env::set_var("FELIX_SHARD_SUSPECT_AFTER_MS", "500");
    }
    let warnings = unrecognised_warnings();
    unsafe {
        std::env::remove_var("FELIX_SHARD_SUSPECT_AFTER_MS");
    }
    assert!(
        warnings.contains(
            &"FELIX_SHARD_SUSPECT_AFTER_MS is retired with the hybrid shard engine and has no \
              effect; unset it"
                .to_string()
        ),
        "{warnings:?}"
    );
    assert!(
        !unrecognised()
            .iter()
            .any(|(name, _)| name == "FELIX_SHARD_SUSPECT_AFTER_MS")
    );
}
