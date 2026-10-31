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
    let view = view(&route());
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
fn a_staged_copy_is_left_out_of_the_kept_set() {
    let kept = kept(&route(), Some("c"));
    let nodes: Vec<_> = kept
        .current
        .members
        .iter()
        .map(|member| member.node.as_str())
        .collect();
    assert_eq!(nodes, vec!["a", "b"]);
    assert_eq!(membership(&view(&route())).members.len(), 3);
}
