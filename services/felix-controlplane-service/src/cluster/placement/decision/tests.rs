//! The reasons `felixctl placement plan` prints for a shard placement left
//! where it is.

use std::collections::HashSet;

use super::*;

fn every_blocked() -> Vec<Blocked> {
    let node = || "broker-7".to_string();
    vec![
        Blocked::DestinationCatchingUp { successor: node() },
        Blocked::LeaderStopping,
        Blocked::LeaderDeparted,
        Blocked::MoveLimit,
        Blocked::NodeMoveLimit { node: node() },
        Blocked::NoDestination,
        Blocked::DestinationHalted {
            node: node(),
            reason: "disk full".to_string(),
        },
        Blocked::Paused,
        Blocked::Recovering,
        Blocked::OverFactor,
        Blocked::BackingOff {
            given_up: 3,
            until_millis: 1234,
        },
        Blocked::SetChangePending,
        Blocked::NoReplacement {
            node: node(),
            why: Leaving::Draining,
        },
        Blocked::MemberDown {
            node: node(),
            why: Leaving::Lost,
        },
        Blocked::TargetUnsettled,
        Blocked::Unformed,
        Blocked::Unapplied,
        Blocked::MoveStranded { node: node() },
    ]
}

/// **Every reason a move waits reads differently**, so an operator can
/// tell from `placement plan` alone which one holds a shard.
#[test]
fn every_blocked_reason_has_its_own_text() {
    let all = every_blocked();
    let texts: HashSet<String> = all.iter().map(ToString::to_string).collect();
    assert_eq!(texts.len(), all.len(), "{texts:?}");
    assert!(texts.iter().all(|text| !text.is_empty()));
}

/// **A reason that names a broker, a count or a time says which**, so the
/// operator knows where to look without reading the control plane's state.
#[test]
fn a_blocked_reason_names_what_it_waits_on() {
    for blocked in every_blocked() {
        let text = blocked.to_string();
        match &blocked {
            Blocked::DestinationCatchingUp { successor: node }
            | Blocked::NodeMoveLimit { node }
            | Blocked::MoveStranded { node } => assert!(text.contains(node.as_str()), "{text}"),
            Blocked::DestinationHalted { node, reason } => {
                assert!(
                    text.contains(node.as_str()) && text.contains(reason.as_str()),
                    "{text}"
                );
            }
            Blocked::NoReplacement { node, why } | Blocked::MemberDown { node, why } => {
                assert!(text.contains(node.as_str()), "{text}");
                assert!(text.contains(&why.to_string()), "{text}");
            }
            Blocked::BackingOff {
                given_up,
                until_millis,
            } => {
                assert!(text.contains(&given_up.to_string()), "{text}");
                assert!(text.contains(&until_millis.to_string()), "{text}");
            }
            _ => {}
        }
    }
}

/// **Why a member leaves is one short phrase per cause**, read inside the
/// longer reasons above.
#[test]
fn leaving_reads_as_its_cause() {
    assert_eq!(Leaving::Internal.to_string(), "an internal node");
    assert_eq!(Leaving::Draining.to_string(), "draining");
    assert_eq!(Leaving::Lost.to_string(), "lost");
}

/// **An unplaceable shard says why, naming the leader or region it waits
/// for**, and no two causes read alike.
#[test]
fn every_unplaceable_cause_is_distinct_and_names_its_subject() {
    let all = [
        Unplaceable::NoEligibleNode,
        Unplaceable::AllNodesAtCapacity,
        Unplaceable::NoCaughtUpReplica,
        Unplaceable::OwnerUnavailable {
            leader: "broker-3".to_string(),
        },
        Unplaceable::NoNodeInRegion {
            region: "eu-west-1".to_string(),
        },
    ];
    let texts: HashSet<String> = all.iter().map(ToString::to_string).collect();
    assert_eq!(texts.len(), all.len());
    assert!(
        Unplaceable::OwnerUnavailable {
            leader: "broker-3".to_string()
        }
        .to_string()
        .contains("broker-3")
    );
    assert!(
        Unplaceable::NoNodeInRegion {
            region: "eu-west-1".to_string()
        }
        .to_string()
        .contains("eu-west-1")
    );
}
