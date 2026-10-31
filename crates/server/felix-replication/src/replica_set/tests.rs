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
    }
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
