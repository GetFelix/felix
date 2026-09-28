//! The node catalog: registration, heartbeats, expiry and lifecycle moves.
use anyhow::anyhow;
use sqlx::FromRow;

use super::{PostgresStore, begin_consistent_read, is_unique_violation, page_fetch};
use crate::model::{
    Node, NodeCapacity, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest, NodeSpec,
    NodeStatus, NodeValidationError,
};
use crate::store::{ChangeSet, Page, PageRequest, Snapshot, StoreError, StoreResult};

const NODE_SELECT_ALL: &str = r#"SELECT node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features FROM nodes ORDER BY node_id"#;

const NODE_SELECT_FIRST_PAGE: &str = r#"SELECT node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features FROM nodes ORDER BY node_id LIMIT $1"#;

const NODE_SELECT_PAGE_AFTER: &str = r#"SELECT node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features FROM nodes WHERE node_id > $1 ORDER BY node_id LIMIT $2"#;

const NODE_SELECT_BY_ID: &str = r#"SELECT node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features FROM nodes WHERE node_id = $1"#;

/// `FOR UPDATE` so a concurrent register or patch of the same node waits rather
/// than reading the row this transaction is about to replace.
const NODE_SELECT_BY_ID_FOR_UPDATE: &str = r#"SELECT node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features FROM nodes WHERE node_id = $1 FOR UPDATE"#;

/// The advisory lock registrations and finalizes serialize on ("felixreg" in
/// ASCII).
const REGISTRATION_LOCK: i64 = 0x6665_6c69_7872_6567;

/// Row shape for the `nodes` table.
#[derive(Debug, Clone, FromRow)]
struct DbNode {
    node_id: String,
    advertise_addr: String,
    client_addr: Option<String>,
    kafka_addr: Option<String>,
    region: String,
    zone: Option<String>,
    labels: serde_json::Value,
    capacity_max_shards: Option<i32>,
    capacity_weight: i32,
    lifecycle: String,
    last_heartbeat_at_millis: i64,
    registered_at_millis: i64,
    incarnation: i64,
    features: serde_json::Value,
}

#[derive(Debug, Clone, FromRow)]
struct NodeChangeRow {
    seq: i64,
    op: String,
    node_id: String,
    payload: Option<serde_json::Value>,
}

pub(super) async fn register_node(store: &PostgresStore, node: Node) -> StoreResult<Node> {
    node.validate().map_err(invalid_node)?;
    let mut tx = store.pool.begin().await?;

    // Registration is the only way a node starts serving, so taking turns
    // with finalize is enough to keep a node lacking a feature from
    // registering while that feature is being enabled.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(REGISTRATION_LOCK)
        .execute(&mut *tx)
        .await?;
    let enabled = enabled_in(&mut tx).await?;
    crate::cluster::fleet::admit(&enabled, &node)?;

    let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
        .bind(&node.node_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(node_from_db)
        .transpose()?;

    let stored = match existing {
        Some(existing) => {
            if !existing
                .status
                .lifecycle
                .can_transition_to(node.status.lifecycle)
            {
                return Err(invalid_node_transition(
                    existing.status.lifecycle,
                    node.status.lifecycle,
                ));
            }
            Node {
                status: NodeStatus {
                    // The identity outlives the process, so its first
                    // registration is what dates it.
                    registered_at_millis: existing.status.registered_at_millis,
                    incarnation: existing.status.incarnation + 1,
                    ..node.status
                },
                ..node
            }
        }
        None => Node {
            status: NodeStatus {
                incarnation: 0,
                ..node.status
            },
            ..node
        },
    };

    let upsert = sqlx::query(
        r#"INSERT INTO nodes (node_id, advertise_addr, region, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, client_addr, kafka_addr, zone, features)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (node_id) DO UPDATE SET
                 advertise_addr = EXCLUDED.advertise_addr,
                 client_addr = EXCLUDED.client_addr,
                 kafka_addr = EXCLUDED.kafka_addr,
                 region = EXCLUDED.region,
                 zone = EXCLUDED.zone,
                 labels = EXCLUDED.labels,
                 capacity_max_shards = EXCLUDED.capacity_max_shards,
                 capacity_weight = EXCLUDED.capacity_weight,
                 lifecycle = EXCLUDED.lifecycle,
                 last_heartbeat_at_millis = EXCLUDED.last_heartbeat_at_millis,
                 registered_at_millis = EXCLUDED.registered_at_millis,
                 incarnation = EXCLUDED.incarnation,
                 features = EXCLUDED.features,
                 updated_at = now()"#,
    );
    let upsert = bind_node(upsert, &stored).execute(&mut *tx).await;
    if let Err(err) = upsert {
        if is_unique_violation(&err) {
            return Err(StoreError::Conflict(format!(
                "advertise_addr {} is already registered to another node",
                stored.spec.advertise_addr
            )));
        }
        return Err(StoreError::Unexpected(err.into()));
    }

    record_node_change(
        &mut tx,
        NodeChangeOp::Registered,
        &stored.node_id,
        Some(&stored),
    )
    .await?;
    tx.commit().await?;
    crate::cluster::membership::metrics::record_registration(if stored.status.incarnation == 0 {
        "new"
    } else {
        "restart"
    });
    metrics::counter!("felix_node_changes_total", "op" => "registered").increment(1);
    Ok(stored)
}

/// The supported set, computed in the database: each feature some serving
/// node reported, kept when every serving node reported it.
pub(super) async fn supported_fleet_features(
    store: &PostgresStore,
) -> StoreResult<std::collections::BTreeSet<String>> {
    let names: Vec<String> = sqlx::query_scalar(
        r#"SELECT feature FROM nodes, jsonb_array_elements_text(features) AS feature
               WHERE lifecycle IN ('live', 'draining')
               GROUP BY feature
               HAVING count(DISTINCT node_id) =
                   (SELECT count(*) FROM nodes WHERE lifecycle IN ('live', 'draining'))"#,
    )
    .fetch_all(&store.pool)
    .await?;
    Ok(names.into_iter().collect())
}

pub(super) async fn enabled_fleet_features(
    store: &PostgresStore,
) -> StoreResult<std::collections::BTreeSet<String>> {
    let names: Vec<String> = sqlx::query_scalar("SELECT feature FROM fleet_features")
        .fetch_all(&store.pool)
        .await?;
    Ok(names.into_iter().collect())
}

pub(super) async fn finalize_fleet_feature(
    store: &PostgresStore,
    feature: &str,
) -> StoreResult<std::collections::BTreeSet<String>> {
    let mut tx = store.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(REGISTRATION_LOCK)
        .execute(&mut *tx)
        .await?;
    let mut enabled = enabled_in(&mut tx).await?;
    if !enabled.contains(feature) {
        let nodes = sqlx::query_as::<_, DbNode>(NODE_SELECT_ALL)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(node_from_db)
            .collect::<StoreResult<Vec<_>>>()?;
        crate::cluster::fleet::check_finalize(&nodes, feature)?;
        sqlx::query("INSERT INTO fleet_features (feature) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(feature)
            .execute(&mut *tx)
            .await?;
        enabled.insert(feature.to_string());
    }
    tx.commit().await?;
    Ok(enabled)
}

async fn enabled_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> StoreResult<std::collections::BTreeSet<String>> {
    let names: Vec<String> = sqlx::query_scalar("SELECT feature FROM fleet_features")
        .fetch_all(&mut **tx)
        .await?;
    Ok(names.into_iter().collect())
}

pub(super) async fn get_node(store: &PostgresStore, node_id: &str) -> StoreResult<Node> {
    sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID)
        .bind(node_id)
        .fetch_optional(&store.pool)
        .await?
        .map(node_from_db)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound("node".into()))
}

pub(super) async fn list_nodes(store: &PostgresStore) -> StoreResult<Vec<Node>> {
    let rows = sqlx::query_as::<_, DbNode>(NODE_SELECT_ALL)
        .fetch_all(&store.pool)
        .await?;
    rows.into_iter().map(node_from_db).collect()
}

pub(super) async fn list_nodes_page(
    store: &PostgresStore,
    page: PageRequest<String>,
) -> StoreResult<Page<Node>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbNode>(NODE_SELECT_FIRST_PAGE).bind(fetch),
        Some(after) => sqlx::query_as::<_, DbNode>(NODE_SELECT_PAGE_AFTER)
            .bind(after)
            .bind(fetch),
    }
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(node_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn patch_node(
    store: &PostgresStore,
    node_id: &str,
    patch: NodePatchRequest,
) -> StoreResult<Node> {
    let mut tx = store.pool.begin().await?;
    let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(node_from_db)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound("node".into()))?;

    let patched = patch.apply(&existing).map_err(invalid_node)?;

    let update = sqlx::query(
        r#"UPDATE nodes SET advertise_addr = $2, region = $3, labels = $4,
                 capacity_max_shards = $5, capacity_weight = $6, lifecycle = $7,
                 last_heartbeat_at_millis = $8, registered_at_millis = $9, incarnation = $10,
                 client_addr = $11, kafka_addr = $12, zone = $13, features = $14,
                 updated_at = now()
               WHERE node_id = $1"#,
    );
    let update = bind_node(update, &patched).execute(&mut *tx).await;
    if let Err(err) = update {
        if is_unique_violation(&err) {
            return Err(StoreError::Conflict(format!(
                "advertise_addr {} is already registered to another node",
                patched.spec.advertise_addr
            )));
        }
        return Err(StoreError::Unexpected(err.into()));
    }

    record_node_change(&mut tx, NodeChangeOp::Updated, node_id, Some(&patched)).await?;
    tx.commit().await?;
    metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
    Ok(patched)
}

pub(super) async fn delete_node(store: &PostgresStore, node_id: &str) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;

    // Lock the node row before counting. An assignment write takes it
    // `FOR SHARE`, so the two serialize: either the write commits first and
    // the count below (a fresh statement under READ COMMITTED) sees it, or
    // the write waits and then finds the node gone.
    let locked: Option<String> =
        sqlx::query_scalar("SELECT node_id FROM nodes WHERE node_id = $1 FOR UPDATE")
            .bind(node_id)
            .fetch_optional(&mut *tx)
            .await?;
    if locked.is_none() {
        return Err(StoreError::NotFound("node".into()));
    }

    // Refused rather than cascaded: deleting the assignment would erase the
    // only record of where that shard's data lives. Deliberately not a
    // foreign key, because a cascade is the behaviour being avoided.
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shard_assignments WHERE leader = $1 OR replicas ? $1",
    )
    .bind(node_id)
    .fetch_one(&mut *tx)
    .await?;
    if held > 0 {
        return Err(StoreError::Conflict(format!(
            "node {node_id} still holds {held} shard(s); reassign them first"
        )));
    }

    let deleted = sqlx::query("DELETE FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .execute(&mut *tx)
        .await?;
    if deleted.rows_affected() == 0 {
        return Err(StoreError::NotFound("node".into()));
    }
    record_node_change(&mut tx, NodeChangeOp::Deregistered, node_id, None).await?;
    tx.commit().await?;
    metrics::counter!("felix_node_changes_total", "op" => "deregistered").increment(1);
    Ok(())
}

pub(super) async fn record_node_heartbeat(
    store: &PostgresStore,
    node_id: &str,
    incarnation: u64,
    at_millis: u64,
) -> StoreResult<Node> {
    let mut tx = store.pool.begin().await?;
    let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(node_from_db)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound("node".into()))?;

    if incarnation < existing.status.incarnation {
        return Err(StoreError::Conflict(format!(
            "heartbeat for incarnation {incarnation} of {node_id}, which is now at {}",
            existing.status.incarnation
        )));
    }
    // The answer to a node that has left grants no lease, and its stamp is
    // what placement waits out before handing its shards on.
    if existing.status.lifecycle == NodeLifecycle::Left {
        tx.commit().await?;
        return Ok(existing);
    }

    // GREATEST, not assignment: heartbeats from two connections can arrive
    // out of order, and the newest observation is the one that matters.
    // No change is emitted -- see the trait for why.
    sqlx::query(
        r#"UPDATE nodes
               SET last_heartbeat_at_millis = GREATEST(last_heartbeat_at_millis, $2),
                   updated_at = now()
               WHERE node_id = $1"#,
    )
    .bind(node_id)
    .bind(at_millis as i64)
    .execute(&mut *tx)
    .await?;

    let mut updated = existing;
    updated.status.last_heartbeat_at_millis =
        updated.status.last_heartbeat_at_millis.max(at_millis);
    tx.commit().await?;
    Ok(updated)
}

/// The database's clock, so every instance judges expiry by the same one.
///
/// `clock_timestamp()` rather than `now()`: `now()` is the transaction's
/// start time and would be identical for every call inside one, which is
/// not what a clock means.
pub(super) async fn now_millis(store: &PostgresStore) -> StoreResult<u64> {
    let millis: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&store.pool)
            .await?;
    Ok(millis.max(0) as u64)
}

pub(super) async fn clamp_future_heartbeats(
    store: &PostgresStore,
    now_millis: u64,
) -> StoreResult<u64> {
    let result = sqlx::query(
        r#"UPDATE nodes SET last_heartbeat_at_millis = $1
               WHERE last_heartbeat_at_millis > $1"#,
    )
    .bind(now_millis as i64)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn expire_stale_nodes(
    store: &PostgresStore,
    expiry_before_millis: u64,
) -> StoreResult<Vec<Node>> {
    let mut tx = store.pool.begin().await?;

    // The UPDATE both selects and claims: it takes a row lock and re-checks
    // the predicate, so a second control-plane instance running the same
    // sweep concurrently matches zero rows and publishes nothing.
    let rows = sqlx::query_as::<_, DbNode>(
        r#"UPDATE nodes SET lifecycle = 'down', updated_at = now()
               WHERE lifecycle IN ('live', 'draining') AND last_heartbeat_at_millis < $1
               RETURNING node_id, advertise_addr, client_addr, kafka_addr, region, zone, labels, capacity_max_shards, capacity_weight, lifecycle, last_heartbeat_at_millis, registered_at_millis, incarnation, features"#,
    )
    .bind(expiry_before_millis as i64)
    .fetch_all(&mut *tx)
    .await?;

    let mut expired = Vec::with_capacity(rows.len());
    for row in rows {
        let node = node_from_db(row)?;
        // The row already reads `down`; the move it made is what the counter
        // is for, and the sweep only returns rows it actually claimed.
        crate::cluster::membership::metrics::record_transition(
            NodeLifecycle::Live,
            NodeLifecycle::Down,
        );
        record_node_change(&mut tx, NodeChangeOp::Updated, &node.node_id, Some(&node)).await?;
        expired.push(node);
    }
    tx.commit().await?;

    for _ in &expired {
        metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
    }
    expired.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    Ok(expired)
}

pub(super) async fn set_node_lifecycle(
    store: &PostgresStore,
    node_id: &str,
    lifecycle: NodeLifecycle,
) -> StoreResult<Option<Node>> {
    let mut tx = store.pool.begin().await?;
    let existing = sqlx::query_as::<_, DbNode>(NODE_SELECT_BY_ID_FOR_UPDATE)
        .bind(node_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(node_from_db)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound("node".into()))?;

    if existing.status.lifecycle == lifecycle {
        return Ok(None);
    }
    if !existing.status.lifecycle.can_transition_to(lifecycle) {
        return Err(invalid_node_transition(
            existing.status.lifecycle,
            lifecycle,
        ));
    }

    let mut updated = existing;
    crate::cluster::membership::metrics::record_transition(updated.status.lifecycle, lifecycle);
    updated.status.lifecycle = lifecycle;
    sqlx::query("UPDATE nodes SET lifecycle = $2, updated_at = now() WHERE node_id = $1")
        .bind(node_id)
        .bind(node_lifecycle_to_str(lifecycle))
        .execute(&mut *tx)
        .await?;

    record_node_change(&mut tx, NodeChangeOp::Updated, node_id, Some(&updated)).await?;
    tx.commit().await?;
    metrics::counter!("felix_node_changes_total", "op" => "updated").increment(1);
    Ok(Some(updated))
}

pub(super) async fn node_snapshot(store: &PostgresStore) -> StoreResult<Snapshot<Node>> {
    // REPEATABLE READ, not just one transaction: under the default READ
    // COMMITTED the two statements below see different snapshots, so a node
    // committed between them is absent from `items` while its seq is
    // already counted in `next_seq` -- lost to every consumer that resumes
    // there. Together with seq being handed out in commit order (see
    // 0005_nodes.sql), this makes snapshot-then-poll exactly-once.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, DbNode>(NODE_SELECT_ALL)
        .fetch_all(&mut *tx)
        .await?;
    let items = rows
        .into_iter()
        .map(node_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM node_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn node_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<NodeChange>> {
    // Consistent for the same reason as `node_snapshot`: the rows and
    // `next_seq` have to describe one instant.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, NodeChangeRow>(
        r#"SELECT seq, op, node_id, payload FROM node_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&mut *tx)
    .await?;
    let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM node_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(NodeChange {
            seq: row.seq as u64,
            op: parse_node_change_op(&row.op)?,
            node_id: row.node_id,
            node: row.payload.map(serde_json::from_value).transpose()?,
        });
    }
    Ok(ChangeSet { items, next_seq })
}

fn node_from_db(row: DbNode) -> StoreResult<Node> {
    Ok(Node {
        node_id: row.node_id,
        spec: NodeSpec {
            advertise_addr: row.advertise_addr,
            client_addr: row.client_addr,
            kafka_addr: row.kafka_addr,
            region: row.region,
            zone: row.zone,
            labels: serde_json::from_value(row.labels)?,
            capacity: NodeCapacity {
                max_shards: row.capacity_max_shards.map(|v| v as u32),
                weight: row.capacity_weight as u32,
            },
        },
        status: NodeStatus {
            lifecycle: parse_node_lifecycle(&row.lifecycle)?,
            last_heartbeat_at_millis: row.last_heartbeat_at_millis as u64,
            registered_at_millis: row.registered_at_millis as u64,
            incarnation: row.incarnation as u64,
            features: serde_json::from_value(row.features)?,
        },
    })
}

/// Bind a node in the column order the insert and the update both use.
fn bind_node<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    node: &'q Node,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(&node.node_id)
        .bind(&node.spec.advertise_addr)
        .bind(&node.spec.region)
        .bind(serde_json::to_value(&node.spec.labels).unwrap_or_default())
        .bind(node.spec.capacity.max_shards.map(|v| v as i32))
        .bind(node.spec.capacity.weight as i32)
        .bind(node_lifecycle_to_str(node.status.lifecycle))
        .bind(node.status.last_heartbeat_at_millis as i64)
        .bind(node.status.registered_at_millis as i64)
        .bind(node.status.incarnation as i64)
        // Last, so both statements above can name them as $11 to $14.
        .bind(node.spec.client_addr.as_deref())
        .bind(node.spec.kafka_addr.as_deref())
        .bind(node.spec.zone.as_deref())
        .bind(serde_json::to_value(&node.status.features).unwrap_or_default())
}

/// Append a change, taking its `seq` from the locked counter.
///
/// The `UPDATE ... RETURNING` holds a row lock until this transaction commits,
/// so a concurrent writer blocks and takes the next number only afterwards.
/// That is what makes `seq` order equal commit order, which is what lets a
/// consumer resume at `next_seq` without missing anything. See 0005_nodes.sql.
async fn record_node_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: NodeChangeOp,
    node_id: &str,
    node: Option<&Node>,
) -> StoreResult<()> {
    let seq = sqlx::query_scalar::<_, i64>(
        "UPDATE node_change_seq SET next_seq = next_seq + 1 RETURNING next_seq - 1",
    )
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query("INSERT INTO node_changes (seq, op, node_id, payload) VALUES ($1, $2, $3, $4)")
        .bind(seq)
        .bind(node_change_op_to_str(&op))
        .bind(node_id)
        .bind(node.map(serde_json::to_value).transpose()?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn node_lifecycle_to_str(lifecycle: NodeLifecycle) -> &'static str {
    match lifecycle {
        NodeLifecycle::Live => "live",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Down => "down",
        NodeLifecycle::Left => "left",
    }
}

fn parse_node_lifecycle(value: &str) -> StoreResult<NodeLifecycle> {
    match value {
        "live" => Ok(NodeLifecycle::Live),
        "draining" => Ok(NodeLifecycle::Draining),
        "down" => Ok(NodeLifecycle::Down),
        "left" => Ok(NodeLifecycle::Left),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown node lifecycle: {other}"
        ))),
    }
}

fn node_change_op_to_str(op: &NodeChangeOp) -> &'static str {
    match op {
        NodeChangeOp::Registered => "Registered",
        NodeChangeOp::Updated => "Updated",
        NodeChangeOp::Deregistered => "Deregistered",
    }
}

fn parse_node_change_op(value: &str) -> StoreResult<NodeChangeOp> {
    match value {
        "Registered" => Ok(NodeChangeOp::Registered),
        "Updated" => Ok(NodeChangeOp::Updated),
        "Deregistered" => Ok(NodeChangeOp::Deregistered),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown node change op: {other}"
        ))),
    }
}

/// A model rejection is the caller's fault, so it surfaces as a conflict rather
/// than an internal error.
fn invalid_node(err: NodeValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

fn invalid_node_transition(from: NodeLifecycle, to: NodeLifecycle) -> StoreError {
    invalid_node(NodeValidationError::UnsupportedTransition { from, to })
}
