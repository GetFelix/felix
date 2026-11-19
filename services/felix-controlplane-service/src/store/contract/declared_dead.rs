//! Declaring a broker's data directory dead.
use std::collections::BTreeSet;
use std::sync::Arc;

use super::nodes::node;
use crate::model::{DeclaredDead, Node, NodeLifecycle};
use crate::store::{ControlPlaneStore, StoreError};

fn on_disk(node_id: &str, port: u16, storage: char) -> Node {
    let mut node = node(node_id, port);
    node.status.storage_id = Some(storage.to_string().repeat(32));
    node
}

fn dead(node_id: &str, storage: char) -> DeclaredDead {
    DeclaredDead {
        node_id: node_id.to_owned(),
        storage_id: storage.to_string().repeat(32),
    }
}

async fn down(store: &Arc<dyn ControlPlaneStore>, node_id: &str) {
    store
        .set_node_lifecycle(node_id, NodeLifecycle::Down)
        .await
        .expect("down");
}

fn names(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|node| (*node).to_owned()).collect()
}

/// **A dead directory never comes back as itself.** Only a node that is not
/// serving can be declared dead, under the storage id it is registered with.
/// It registers again only under another storage id, then leaves the dead
/// set; a node delete keeps the record, and a registration without a
/// storage id is refused once any of the node's directories was declared.
pub(crate) async fn run_declared_dead_contract(store: Arc<dyn ControlPlaneStore>) {
    assert!(matches!(
        store.declare_node_dead("dead-a").await,
        Err(StoreError::NotFound(_))
    ));
    store
        .register_node(on_disk("dead-a", 7101, 'a'))
        .await
        .expect("register a");
    store
        .register_node(node("dead-b", 7102))
        .await
        .expect("register b");
    // Serving, or no storage id to name the directory by.
    assert!(matches!(
        store.declare_node_dead("dead-a").await,
        Err(StoreError::Conflict(_))
    ));
    down(&store, "dead-b").await;
    assert!(matches!(
        store.declare_node_dead("dead-b").await,
        Err(StoreError::Conflict(_))
    ));

    down(&store, "dead-a").await;
    assert_eq!(
        store.declare_node_dead("dead-a").await.expect("declare"),
        dead("dead-a", 'a')
    );
    assert_eq!(
        store.declare_node_dead("dead-a").await.expect("again"),
        dead("dead-a", 'a')
    );
    assert_eq!(
        store.declared_dead().await.expect("list"),
        vec![dead("dead-a", 'a')]
    );
    assert_eq!(store.dead_nodes().await.expect("dead"), names(&["dead-a"]));

    // The same directory, or one that names none, is refused.
    for refused in [on_disk("dead-a", 7101, 'a'), node("dead-a", 7101)] {
        assert!(matches!(
            store.register_node(refused).await,
            Err(StoreError::Conflict(_))
        ));
    }
    assert_eq!(store.dead_nodes().await.expect("dead"), names(&["dead-a"]));

    // A new directory is a live broker again; the record stays.
    store
        .register_node(on_disk("dead-a", 7101, 'b'))
        .await
        .expect("new storage id");
    assert!(store.dead_nodes().await.expect("dead").is_empty());
    assert_eq!(
        store.declared_dead().await.expect("list"),
        vec![dead("dead-a", 'a')]
    );

    // Declared again under the new id, then deleted: still dead, and the
    // old directory still cannot register.
    down(&store, "dead-a").await;
    store.declare_node_dead("dead-a").await.expect("declare b");
    store.delete_node("dead-a").await.expect("delete");
    assert_eq!(store.dead_nodes().await.expect("dead"), names(&["dead-a"]));
    assert!(matches!(
        store.register_node(on_disk("dead-a", 7101, 'a')).await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store.declared_dead().await.expect("list"),
        vec![dead("dead-a", 'a'), dead("dead-a", 'b')]
    );
}
