//! What the cluster looked like when a campaign failed: who leads each of the
//! workload's shards and how far each replica had got, from the control plane,
//! and how each broker says it is doing, from its metrics.
//!
//! Brokers do not label their replication metrics by shard (the label set
//! would grow with every stream), so the per-replica positions come from the
//! last replica report each shard's leader sent the control plane.

use std::fmt::Write as _;
use std::time::Duration;

use felix_controlplane_service::model::{ReplicaReport, ShardAssignment, ShardKind};
use felix_controlplane_service::store::ControlPlaneStore;

use super::campaign::Campaign;
use crate::Cluster;

/// How long one metric scrape may take. A paused broker never answers.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);

/// The broker metrics the dump prints, and what to call each.
const BROKER_METRICS: [(&str, &str); 6] = [
    ("felix_broker_lease_held", "lease held"),
    ("felix_broker_heartbeat_age_seconds", "heartbeat age s"),
    ("felix_broker_shard_watch_checkpoint", "watch checkpoint"),
    (
        "felix_broker_replication_lag_records",
        "slowest follower lag",
    ),
    ("felix_broker_replication_halted", "halted followers"),
    (
        "felix_broker_quorum_marks_withheld_total",
        "quorum marks withheld",
    ),
];

/// The dump, as the lines a failing run prints.
pub(super) async fn state(cluster: &Cluster, campaign: &Campaign) -> String {
    let mut out = String::from("cluster state:\n");
    let Some(control_plane) = cluster.control_plane.as_ref() else {
        out.push_str("  the control plane is gone\n");
        return out;
    };
    let store = &control_plane.store;

    let lifecycles: Vec<(String, String)> = match store.list_nodes().await {
        Ok(nodes) => nodes
            .into_iter()
            .map(|node| {
                let lifecycle = format!("{:?}", node.status.lifecycle).to_lowercase();
                (node.node_id, lifecycle)
            })
            .collect(),
        Err(err) => {
            let _ = writeln!(out, "  could not list nodes: {err}");
            Vec::new()
        }
    };
    for id in cluster.node_ids() {
        let running = cluster.node(&id).is_some_and(|node| node.is_running());
        let lifecycle = lifecycles
            .iter()
            .find(|(node, _)| *node == id)
            .map_or("unregistered", |(_, lifecycle)| lifecycle.as_str());
        let _ = write!(
            out,
            "  {id}: {}, {lifecycle}",
            if running { "running" } else { "not running" }
        );
        if running {
            for (name, label) in BROKER_METRICS {
                match tokio::time::timeout(SCRAPE_TIMEOUT, cluster.metric(&id, name)).await {
                    Ok(Ok(Some(value))) => {
                        let _ = write!(out, ", {label} {value}");
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(_)) | Err(_) => {
                        out.push_str(", metrics did not answer");
                        break;
                    }
                }
            }
        }
        out.push('\n');
    }

    let assignments = match store.list_shard_assignments().await {
        Ok(assignments) => assignments,
        Err(err) => {
            let _ = writeln!(out, "  could not list shard assignments: {err}");
            return out;
        }
    };
    let reports = store.list_replica_reports().await.unwrap_or_default();
    let now = now_millis();
    for (kind, name) in campaign.shard_names() {
        let mut shards: Vec<&ShardAssignment> = assignments
            .iter()
            .filter(|a| {
                kind_name(a.key.kind) == kind
                    && a.key.tenant_id == cluster.tenant_id
                    && a.key.namespace == cluster.namespace
                    && a.key.stream == name
            })
            .collect();
        if shards.is_empty() {
            let _ = writeln!(out, "  {kind} {name}: no assignment");
        }
        shards.sort_by_key(|a| a.key.shard);
        for assignment in shards {
            describe(&mut out, kind, assignment);
            match reports.iter().find(|r| r.key == assignment.key) {
                Some(report) => describe_report(&mut out, assignment, report, now),
                None => out.push_str("    no replica report\n"),
            }
        }
    }
    out
}

fn describe(out: &mut String, kind: &str, a: &ShardAssignment) {
    let _ = write!(
        out,
        "  {kind} {}/{}: leader {}, replicas [{}], generation {}, {}",
        a.key.stream,
        a.key.shard,
        a.leader,
        a.replicas.join(", "),
        a.generation,
        format!("{:?}", a.state).to_lowercase(),
    );
    if let Some(successor) = &a.successor {
        let _ = write!(out, ", moving to {successor}");
    }
    if let Some(joining) = &a.joining {
        let _ = write!(out, ", {joining} joining");
    }
    if let Some(reason) = a.move_reason {
        let _ = write!(out, " ({})", reason.as_str());
    }
    out.push('\n');
}

fn describe_report(out: &mut String, a: &ShardAssignment, report: &ReplicaReport, now: u64) {
    let tail = report
        .leader_offset
        .map_or_else(|| "unknown".to_string(), |offset| offset.to_string());
    let replicas: Vec<String> = report
        .offsets
        .iter()
        .map(|(node, offset)| {
            let caught_up = if report.caught_up.contains(node) {
                " caught up"
            } else {
                ""
            };
            format!("{node} at {offset}{caught_up}")
        })
        .collect();
    let _ = writeln!(
        out,
        "    report at generation {} ({}ms old{}): leader log end {tail}, majority at {}; {}",
        report.generation,
        now.saturating_sub(report.reported_at_millis),
        if report.drained { ", drained" } else { "" },
        majority_mark(a, report).map_or_else(|| "unknown".to_string(), |mark| mark.to_string()),
        if replicas.is_empty() {
            "no replica positions".to_string()
        } else {
            replicas.join(", ")
        },
    );
}

/// The offset a majority of the shard's copies had reached, by the report:
/// what the leader could have acknowledged up to. Brokers keep their own
/// quorum marks to themselves, so this is the control plane's view of it.
fn majority_mark(a: &ShardAssignment, report: &ReplicaReport) -> Option<u64> {
    let copies = a.replicas.len() + 1;
    let mut positions: Vec<u64> = report
        .leader_offset
        .into_iter()
        .chain(report.offsets.values().copied())
        .collect();
    positions.sort_unstable_by(|x, y| y.cmp(x));
    positions.get(copies / 2).copied()
}

fn kind_name(kind: ShardKind) -> &'static str {
    match kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
