use felix_client::{InspectedFence, InspectedLease, InspectedReplica};

use super::*;

fn target() -> Target {
    Target {
        tenant: "acme".to_string(),
        namespace: "default".to_string(),
        name: "orders".to_string(),
        cache: false,
    }
}

fn assignment() -> InspectedAssignment {
    InspectedAssignment {
        generation: 42,
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
        draining: false,
        successor: Some("broker-c".to_string()),
    }
}

fn view(node: &str, phase: &str) -> ShardInspection {
    ShardInspection {
        node_id: node.to_string(),
        shards: 4,
        role: "follower".to_string(),
        phase: phase.to_string(),
        serving: false,
        reason: Some("not_assigned_here".to_string()),
        detail: None,
        generation: Some(42),
        assignment: Some(assignment()),
        fence: None,
        lease: None,
        tail: Some(1_048_510),
        committed: None,
        accepted_generation: Some(42),
        replicas: vec![],
    }
}

/// The leader from the design: promoted, and neither replica has taken the
/// fence.
fn fencing_leader() -> ShardInspection {
    let fenced = |node: &str| InspectedReplica {
        node_id: node.to_string(),
        role: if node == "broker-c" {
            "learner"
        } else {
            "follower"
        }
        .to_string(),
        next_offset: None,
        lag: None,
        fence: Some(false),
        state: "fencing".to_string(),
        halted: None,
    };
    ShardInspection {
        role: "leader".to_string(),
        reason: Some("fencing".to_string()),
        detail: Some("0 of 2 replicas took the fence".to_string()),
        fence: Some(InspectedFence {
            took: vec![],
            pending: vec!["broker-b".to_string(), "broker-c".to_string()],
            attempts: 4,
            retry_in_ms: Some(2000),
        }),
        lease: Some(InspectedLease {
            held: true,
            remaining_ms: 7100,
        }),
        tail: Some(1_048_576),
        committed: Some(1_048_510),
        replicas: vec![fenced("broker-b"), fenced("broker-c")],
        ..view("broker-a", "fencing")
    }
}

fn views(list: Vec<ShardInspection>) -> BTreeMap<String, ShardInspection> {
    list.into_iter()
        .map(|view| (view.node_id.clone(), view))
        .collect()
}

#[test]
fn a_target_is_a_name_or_a_full_path() {
    let tenant = || Ok("t1".to_string());
    assert_eq!(
        Target::parse("orders", false, tenant, "default").unwrap(),
        Target {
            tenant: "t1".to_string(),
            namespace: "default".to_string(),
            name: "orders".to_string(),
            cache: false,
        }
    );
    let full = Target::parse("acme/payments/orders", true, tenant, "default").unwrap();
    assert_eq!(
        (full.tenant.as_str(), full.namespace.as_str(), full.cache),
        ("acme", "payments", true)
    );
    for bad in ["", "a/b", "a//c", "a/b/c/d"] {
        let err = Target::parse(bad, false, tenant, "default").unwrap_err();
        assert_eq!(crate::error::exit_for(&err), Exit::Usage, "{bad:?}");
    }
}

#[test]
fn a_fencing_leader_reads_as_in_the_design() {
    let report = report(
        &target(),
        3,
        Some(&assignment()),
        &views(vec![
            fencing_leader(),
            view("broker-b", "closed"),
            view("broker-c", "closed"),
        ]),
        &[],
    );
    assert_eq!(
        render(&report),
        "\
shard       acme/default/orders/3 (stream)
generation  42, move to broker-c staged
leader      broker-a  not serving: fencing, 0 of 2 replicas took the fence (4 attempts, next in 2s)
lease       held, 7.1s left
offsets     tail 1048576  committed 1048510

REPLICA   ROLE      NEXT OFFSET  LAG  FENCE  STATE
broker-a  leader    1048576      -    -      fencing
broker-b  follower  -            -    no     fencing
broker-c  learner   -            -    no     fencing"
    );
    assert_eq!(report["leader"]["reason"], "fencing");
    assert_eq!(report["leader"]["fence"]["attempts"], 4);
    assert_eq!(report["assignment"]["move"]["to"], "broker-c");
    assert_eq!(report["replicas"][0]["own"]["phase"], "closed");
    assert_eq!(report["unreachable"], json!([]));
}

#[test]
fn a_shipping_leader_shows_each_replicas_lag() {
    let mut leader = fencing_leader();
    leader.serving = true;
    leader.phase = "active".to_string();
    leader.reason = None;
    leader.detail = None;
    leader.fence = None;
    leader.replicas = vec![
        InspectedReplica {
            node_id: "broker-b".to_string(),
            role: "follower".to_string(),
            next_offset: Some(1_048_510),
            lag: Some(66),
            fence: None,
            state: "shipping".to_string(),
            halted: None,
        },
        InspectedReplica {
            node_id: "broker-c".to_string(),
            role: "learner".to_string(),
            next_offset: Some(900),
            lag: Some(1_047_676),
            fence: None,
            state: "halted".to_string(),
            halted: Some("diverged".to_string()),
        },
    ];
    let rendered = render(&report(
        &target(),
        3,
        Some(&assignment()),
        &views(vec![leader]),
        &["broker-b".to_string(), "broker-c".to_string()],
    ));
    assert!(rendered.contains("broker-a  serving"), "{rendered}");
    assert!(
        rendered.contains("unreachable  broker-b, broker-c"),
        "{rendered}"
    );
    assert!(
        rendered.contains("broker-b  follower  1048510      66"),
        "{rendered}"
    );
    assert!(rendered.contains("halted (diverged)"), "{rendered}");
}

/// The leader did not answer: its replicas' own views are shown, and nothing
/// is said on its behalf.
#[test]
fn an_unreachable_leader_is_not_described() {
    let report = report(
        &target(),
        0,
        Some(&assignment()),
        &views(vec![view("broker-b", "closed")]),
        &["broker-a".to_string(), "broker-c".to_string()],
    );
    assert!(report["leader"].is_null());
    let rendered = render(&report);
    assert!(
        rendered.contains("leader       broker-a  unreachable"),
        "{rendered}"
    );
    assert!(
        rendered.contains("broker-b  follower  1048510      -    -      closed"),
        "{rendered}"
    );
    assert!(rendered.contains("unreachable"), "{rendered}");
}
