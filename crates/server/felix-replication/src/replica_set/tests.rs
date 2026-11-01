use std::collections::BTreeMap;
use std::sync::Arc;

use felix_router::NodeRef;

use super::*;

fn node(id: &str) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: "127.0.0.1:1".parse().expect("addr"),
        region: "r".to_string(),
        live: true,
    }
}

fn route() -> Route {
    Route {
        leader: node("a"),
        replicas: vec![node("b"), node("c")],
        generation: 7,
        draining: false,
        successor: None,
        incarnation: 0,
        fence_storage_ids: Some(Arc::new(BTreeMap::from([
            (
                "a".to_string(),
                "0101010101010101010101010101010f".to_string(),
            ),
            ("b".to_string(), String::new()),
        ]))),
        member_storage_ids: None,
        pending: None,
    }
}

/// The set names the registered ids on a route that carries them, whatever
/// the failover's ids say, and falls back to the failover's only without any.
#[test]
fn the_set_names_each_members_registered_storage_id() {
    let mut route = route();
    route.member_storage_ids = Some(Arc::new(BTreeMap::from([
        (
            "a".to_string(),
            "02020202020202020202020202020202".to_string(),
        ),
        (
            "b".to_string(),
            "03030303030303030303030303030303".to_string(),
        ),
        (
            "c".to_string(),
            "04040404040404040404040404040404".to_string(),
        ),
    ])));
    let ids: Vec<_> = view(&route, None)
        .members
        .iter()
        .map(|member| member.storage_id)
        .collect();
    assert_eq!(ids, vec![Some([2; 16]), Some([3; 16]), Some([4; 16])]);

    let set = felix_router::PlacedSet {
        leader: "a".to_string(),
        replicas: vec!["b".to_string()],
        successor: None,
        storage_ids: BTreeMap::new(),
        member_storage_ids: BTreeMap::from([(
            "b".to_string(),
            "03030303030303030303030303030303".to_string(),
        )]),
    };
    let kept = placed(&set, 3);
    assert_eq!(
        kept.members[1].storage,
        Some(StorageId::from_bytes([3; 16]))
    );
}

#[test]
fn a_routes_set_names_the_leader_and_its_replicas() {
    let view = view(&route(), None);
    assert_eq!(view.generation, 7);
    let members: Vec<_> = view
        .members
        .iter()
        .map(|member| (member.node_id.as_str(), member.storage_id))
        .collect();
    let mut id = [1u8; 16];
    id[15] = 0x0f;
    assert_eq!(members, vec![("a", Some(id)), ("b", None), ("c", None)]);
}

#[test]
fn a_named_learner_is_a_learner_in_the_set_and_a_bare_successor_is_not() {
    let mut staged = route();
    staged.successor = Some("c".to_string());
    let bare = view(&staged, None);
    assert!(bare.members.iter().all(|member| !member.learner));
    let view = view(&staged, Some("c"));
    let learners: Vec<_> = view
        .members
        .iter()
        .map(|member| (member.node_id.as_str(), member.learner))
        .collect();
    assert_eq!(learners, vec![("a", false), ("b", false), ("c", true)]);
    assert_eq!(kept(&staged, Some("c")).current, membership(&view));
}

#[test]
fn the_leaders_and_a_followers_sets_for_one_route_are_equal() {
    let mut staged = route();
    staged.successor = Some("c".to_string());
    // The leader keeps `kept(route)`, a replica learns from the view.
    let leaders = kept(&staged, None);
    let followers = membership(&view(&staged, None));
    assert_eq!(leaders.current, followers);
    assert_eq!(leaders.current.members.len(), 3);
}
