//! A table of what every broker and the control plane believe about one
//! shard, for a failure to print.

use std::fmt;
use std::time::Duration;

use felix_broker_service::sim::{ShardKey, ShardSnapshot};

use super::{SimCluster, describe};

/// What one broker reports about one shard.
#[derive(Debug, Default)]
pub struct NodeView {
    pub node_id: String,
    /// `up`, `starting` or `down`.
    pub state: &'static str,
    /// Cut off from the control plane.
    pub cut: bool,
    /// The catalog has been applied, so the broker registers.
    pub seeded: bool,
    /// The node incarnation the control plane registered it under, if it has.
    pub incarnation: Option<u64>,
    pub lease_remaining: Option<Duration>,
    pub snapshot: Option<ShardSnapshot>,
}

/// One shard across the cluster.
#[derive(Debug)]
pub struct Census {
    pub shard: String,
    pub nodes: Vec<NodeView>,
    /// The control plane's assignment for the shard, as text.
    pub assignment: String,
}

pub(super) async fn take(cluster: &SimCluster, key: &ShardKey) -> Census {
    let mut nodes = Vec::new();
    for node in cluster.nodes() {
        nodes.push(node.view(key).await);
    }
    let assignment = assignment_of(cluster, key).await;
    Census {
        shard: describe(key),
        nodes,
        assignment,
    }
}

async fn assignment_of(cluster: &SimCluster, key: &ShardKey) -> String {
    let fault = cluster.cp().fault();
    let read = tokio::time::timeout(Duration::from_secs(1), cluster.cp().assignments()).await;
    let snapshot = match read {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(err)) => return format!("unreadable ({fault:?}): {err:#}"),
        Err(_) => return format!("unreadable ({fault:?}): timed out"),
    };
    let found = snapshot.items.iter().find(|item| {
        let assigned = &item.assigned.assignment;
        assigned.key.stream == key.stream
            && assigned.key.shard == key.shard
            && assigned.key.tenant_id == key.tenant_id
            && assigned.key.namespace == key.namespace
            && format!("{:?}", assigned.key.kind).to_lowercase()
                == format!("{:?}", key.kind).to_lowercase()
    });
    match found {
        Some(item) => {
            let a = &item.assigned.assignment;
            format!(
                "leader {} replicas {:?} generation {} state {:?}{}{}",
                a.leader,
                a.replicas,
                a.generation,
                a.state,
                a.successor
                    .as_ref()
                    .map_or(String::new(), |s| format!(" successor {s}")),
                a.joining
                    .as_ref()
                    .map_or(String::new(), |j| format!(" joining {j}")),
            )
        }
        None => "none".to_string(),
    }
}

impl fmt::Display for Census {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "shard {}  control plane: {}",
            self.shard, self.assignment
        )?;
        writeln!(
            f,
            "  {:<5} {:<9} {:<5} {:<6} {:<4} {:<8} {:<14} {:<10} {:<8} {:<8} {:<8}",
            "node",
            "state",
            "cut",
            "seeded",
            "inc",
            "lease",
            "routed(leader,g)",
            "phase",
            "fence",
            "tail",
            "quorum"
        )?;
        for node in &self.nodes {
            let lease = node.lease_remaining.map_or("-".to_string(), |left| {
                format!("{:.1}s", left.as_secs_f64())
            });
            let (routed, phase, fence, tail, quorum) = match &node.snapshot {
                Some(snapshot) => (
                    snapshot
                        .routed_leader
                        .as_ref()
                        .map_or("-".to_string(), |(leader, g)| format!("{leader},{g}")),
                    snapshot.phase.unwrap_or("busy").to_string(),
                    match (snapshot.serving, snapshot.generation) {
                        (true, Some(g)) => format!("open@{g}"),
                        (false, Some(g)) => format!("shut@{g}"),
                        _ => "-".to_string(),
                    },
                    snapshot.log_tail.map_or("-".to_string(), |t| t.to_string()),
                    snapshot
                        .quorum_mark
                        .map_or("-".to_string(), |m| m.to_string()),
                ),
                None => Default::default(),
            };
            writeln!(
                f,
                "  {:<5} {:<9} {:<5} {:<6} {:<4} {:<8} {:<14} {:<10} {:<8} {:<8} {:<8}",
                node.node_id,
                node.state,
                if node.cut { "yes" } else { "no" },
                if node.seeded { "yes" } else { "no" },
                node.incarnation
                    .map_or("-".to_string(), |at| at.to_string()),
                lease,
                routed,
                phase,
                fence,
                tail,
                quorum,
            )?;
        }
        Ok(())
    }
}
