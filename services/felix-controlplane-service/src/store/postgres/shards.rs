//! Shard assignments, and what leaders report about their replicas.
use anyhow::anyhow;
use sqlx::FromRow;

use super::{PostgresStore, begin_consistent_read, page_fetch};
use crate::model::{
    MoveReason, ReplicaReport, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp,
    ShardKey, ShardKind, ShardState, ShardValidationError,
};
use crate::store::{
    AssignmentWrite, ChangeSet, Page, PageRequest, PlacementLease, ReportWrite, Snapshot,
    StoreError, StoreResult,
};

#[derive(Debug, Clone, FromRow)]
struct DbShardAssignment {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    leader: String,
    replicas: serde_json::Value,
    generation: i64,
    state: String,
    successor: Option<String>,
    joining: Option<String>,
    move_started_at_millis: Option<i64>,
    move_reason: Option<String>,
    incarnation: i64,
}

#[derive(Debug, Clone, FromRow)]
struct ShardAssignmentChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, FromRow)]
struct DbReplicaReport {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: i32,
    kind: String,
    generation: i64,
    caught_up: serde_json::Value,
    offsets: serde_json::Value,
    reported_at_millis: i64,
    drained: bool,
    leader_offset: Option<i64>,
    halted: serde_json::Value,
    counter_offsets: serde_json::Value,
    by_round: bool,
    leader_incarnation: Option<i64>,
    by_round_at: Option<i64>,
    follower_storage: Option<serde_json::Value>,
    follower_storage_at: Option<i64>,
    leader: Option<String>,
    node_incarnations: Option<serde_json::Value>,
    stamped_at: Option<i64>,
}

pub(super) async fn put_shard_assignment(
    store: &PostgresStore,
    assignment: ShardAssignment,
) -> StoreResult<ShardAssignment> {
    match write_shard_assignment(store, assignment, None, None).await? {
        AssignmentWrite::Written(stored) => Ok(stored),
        AssignmentWrite::Stale { .. } | AssignmentWrite::Fenced { .. } => {
            unreachable!("an unconditional write is never stale")
        }
    }
}

pub(super) async fn put_shard_assignment_if(
    store: &PostgresStore,
    assignment: ShardAssignment,
    expected_generation: Option<u64>,
    fence: u64,
) -> StoreResult<AssignmentWrite> {
    write_shard_assignment(store, assignment, Some(expected_generation), Some(fence)).await
}

/// `expected`: `None` writes unconditionally, `Some(generation)` only over
/// that generation (`Some(None)`: only where there is no assignment).
/// `fence`: the placement token the write must find.
async fn write_shard_assignment(
    store: &PostgresStore,
    assignment: ShardAssignment,
    expected: Option<Option<u64>>,
    fence: Option<u64>,
) -> StoreResult<AssignmentWrite> {
    assignment.validate().map_err(invalid_shard)?;
    let mut tx = store.pool.begin().await?;

    // First, so fenced writes queue on this one row and in one order, and
    // the token only moves if the whole transaction commits.
    if let Some(fence) = fence {
        let advanced: Option<i64> = sqlx::query_scalar(
            "UPDATE placement_lease SET token = token + 1 WHERE id = 1 AND token = $1 RETURNING token",
        )
        .bind(fence as i64)
        .fetch_optional(&mut *tx)
        .await?;
        if advanced.is_none() {
            drop(tx);
            return Ok(AssignmentWrite::Fenced {
                token: placement_token(store).await?,
            });
        }
    }

    // `FOR KEY SHARE` holds the stream or cache row until this commits, so a
    // delete waits for the assignment to land (and then removes it with the
    // row), and a stream created again after the delete is read with its own
    // incarnation: never the old one stamped on an assignment of the new.
    // The shard bound comes from whichever of the two the key names, chosen
    // by the kind rather than by trying one and falling back to the other:
    // a cache and a stream may share a name, and a fallback would let a
    // shard of the wrong one validate against the other's count.
    let (table, missing) = match assignment.key.kind {
        ShardKind::Stream => (
            "SELECT shards, incarnation FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 FOR KEY SHARE",
            "stream",
        ),
        ShardKind::Cache => (
            "SELECT shards, 0::BIGINT AS incarnation FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3 FOR KEY SHARE",
            "cache",
        ),
    };
    let bounds: Option<(i32, i64)> = sqlx::query_as(table)
        .bind(&assignment.key.tenant_id)
        .bind(&assignment.key.namespace)
        .bind(&assignment.key.stream)
        .fetch_optional(&mut *tx)
        .await?;
    let (shards, incarnation) = bounds.ok_or_else(|| StoreError::NotFound(missing.into()))?;
    let shards = shards as u32;
    assignment.validate_within(shards).map_err(invalid_shard)?;

    // Checked here rather than by a foreign key: the node reference has none
    // deliberately, so deleting a node cannot cascade an assignment away.
    // `FOR SHARE` holds each node until this commits, so a concurrent
    // `delete_node` (which locks the row `FOR UPDATE`) either waits and then
    // sees this assignment, or has already deleted the node and this finds
    // no row.
    for node_id in assignment.nodes() {
        let exists: Option<String> =
            sqlx::query_scalar("SELECT node_id FROM nodes WHERE node_id = $1 FOR SHARE")
                .bind(node_id)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_none() {
            return Err(StoreError::NotFound(format!("node {node_id}")));
        }
    }

    // `FOR UPDATE` so a concurrent write to the same shard waits rather than
    // reading the row this transaction is about to replace.
    let existing = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5
               FOR UPDATE"#,
    )
    .bind(&assignment.key.tenant_id)
    .bind(&assignment.key.namespace)
    .bind(&assignment.key.stream)
    .bind(assignment.key.shard as i32)
    .bind(assignment.key.kind.as_str())
    .fetch_optional(&mut *tx)
    .await?
    .map(shard_from_db)
    .transpose()?;

    // The lock makes this check-then-write safe for a row that exists. One
    // that does not exist has no row to lock; the insert below covers that.
    let current = existing.as_ref().map(|a| a.generation);
    if let Some(expected) = expected
        && expected != current
    {
        return Ok(AssignmentWrite::Stale { current });
    }

    let mut change_seq = None;
    let (op, generation) = match &existing {
        Some(existing) => {
            if !existing.state.can_transition_to(assignment.state) {
                return Err(invalid_shard(ShardValidationError::UnsupportedTransition {
                    from: existing.state,
                    to: assignment.state,
                }));
            }
            (
                ShardAssignmentChangeOp::Updated,
                existing.generation.saturating_add(1),
            )
        }
        // The change's sequence number is taken here, before the write, so
        // a new stream's first generation can be it; see
        // `model::first_generation`.
        None => {
            let seq = take_change_seq(&mut tx).await?;
            change_seq = Some(seq);
            (
                ShardAssignmentChangeOp::Assigned,
                crate::model::first_generation(incarnation as u64, seq as u64),
            )
        }
    };

    // Store-owned, so a caller cannot pin a generation and make its own
    // stale report look current.
    let stored = ShardAssignment {
        generation,
        incarnation: incarnation as u64,
        ..assignment
    };

    // A write that expects no assignment must not replace one another writer
    // inserted since the read above.
    let insert = if expected == Some(None) {
        r#"INSERT INTO shard_assignments (tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor, joining, move_started_at_millis, move_reason, incarnation)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO NOTHING"#
    } else {
        r#"INSERT INTO shard_assignments (tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor, joining, move_started_at_millis, move_reason, incarnation)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO UPDATE SET
                 leader = EXCLUDED.leader,
                 replicas = EXCLUDED.replicas,
                 generation = EXCLUDED.generation,
                 state = EXCLUDED.state,
                 successor = EXCLUDED.successor,
                 joining = EXCLUDED.joining,
                 move_started_at_millis = EXCLUDED.move_started_at_millis,
                 move_reason = EXCLUDED.move_reason,
                 incarnation = EXCLUDED.incarnation,
                 updated_at = now()"#
    };
    let written = sqlx::query(insert)
        .bind(&stored.key.tenant_id)
        .bind(&stored.key.namespace)
        .bind(&stored.key.stream)
        .bind(stored.key.shard as i32)
        .bind(stored.key.kind.as_str())
        .bind(&stored.leader)
        .bind(serde_json::to_value(&stored.replicas)?)
        .bind(stored.generation as i64)
        .bind(shard_state_to_str(stored.state))
        .bind(&stored.successor)
        .bind(&stored.joining)
        .bind(stored.move_started_at_millis.map(|millis| millis as i64))
        .bind(stored.move_reason.map(MoveReason::as_str))
        .bind(stored.incarnation as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if written == 0 {
        drop(tx);
        let current = get_shard_assignment(store, &stored.key)
            .await
            .map(|a| Some(a.generation))
            .or_else(|err| match err {
                StoreError::NotFound(_) => Ok(None),
                err => Err(err),
            })?;
        return Ok(AssignmentWrite::Stale { current });
    }

    match change_seq {
        Some(seq) => insert_shard_change(&mut tx, seq, op, &stored.key, Some(&stored)).await?,
        None => record_shard_change(&mut tx, op, &stored.key, Some(&stored)).await?,
    }
    tx.commit().await?;
    metrics::counter!("felix_shard_assignment_changes_total", "op" => shard_op_to_str(op))
        .increment(1);
    Ok(AssignmentWrite::Written(stored))
}

pub(super) async fn get_shard_assignment(
    store: &PostgresStore,
    key: &ShardKey,
) -> StoreResult<ShardAssignment> {
    sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .fetch_optional(&store.pool)
    .await?
    .map(shard_from_db)
    .transpose()?
    .ok_or_else(|| StoreError::NotFound("shard assignment".into()))
}

pub(super) async fn list_shard_assignments(
    store: &PostgresStore,
) -> StoreResult<Vec<ShardAssignment>> {
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(shard_from_db).collect()
}

/// Pages in primary-key order, so each is an index range scan; the leader
/// filter, when there is one, applies within it.
pub(super) async fn list_shard_assignments_page(
    store: &PostgresStore,
    leader: Option<&str>,
    page: PageRequest<ShardKey>,
) -> StoreResult<Page<ShardAssignment>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments WHERE ($1::text IS NULL OR leader = $1)
               ORDER BY tenant_id, namespace, kind, stream, shard LIMIT $2"#,
        )
        .bind(leader)
        .bind(fetch),
        Some(after) => sqlx::query_as::<_, DbShardAssignment>(
            r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments WHERE ($1::text IS NULL OR leader = $1)
                 AND (tenant_id, namespace, kind, stream, shard) > ($2, $3, $4, $5, $6)
               ORDER BY tenant_id, namespace, kind, stream, shard LIMIT $7"#,
        )
        .bind(leader)
        .bind(&after.tenant_id)
        .bind(&after.namespace)
        .bind(after.kind.as_str())
        .bind(&after.stream)
        // Past every real shard when out of range, which is the right answer
        // for a cursor beyond the last one.
        .bind(i32::try_from(after.shard).unwrap_or(i32::MAX))
        .bind(fetch),
    }
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(shard_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn list_shard_assignments_for_node(
    store: &PostgresStore,
    node_id: &str,
) -> StoreResult<Vec<ShardAssignment>> {
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments WHERE leader = $1
               ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .bind(node_id)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(shard_from_db).collect()
}

pub(super) async fn delete_shard_assignment(
    store: &PostgresStore,
    key: &ShardKey,
) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;
    let deleted = sqlx::query(
        r#"DELETE FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .execute(&mut *tx)
    .await?;
    if deleted.rows_affected() == 0 {
        return Err(StoreError::NotFound("shard assignment".into()));
    }
    record_shard_change(&mut tx, ShardAssignmentChangeOp::Unassigned, key, None).await?;
    tx.commit().await?;
    metrics::counter!("felix_shard_assignment_changes_total", "op" => "unassigned").increment(1);
    Ok(())
}

pub(super) async fn shard_assignment_snapshot(
    store: &PostgresStore,
) -> StoreResult<Snapshot<ShardAssignment>> {
    // REPEATABLE READ for the same reason as `node_snapshot`: under READ
    // COMMITTED the two reads below see different snapshots, so an
    // assignment committed between them is missing from `items` while
    // already counted in `next_seq`.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, DbShardAssignment>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, leader, replicas, generation, state, successor,
                      joining, move_started_at_millis, move_reason, incarnation
               FROM shard_assignments ORDER BY tenant_id, namespace, stream, shard, kind"#,
    )
    .fetch_all(&mut *tx)
    .await?;
    let items = rows
        .into_iter()
        .map(shard_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    let next_seq = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn shard_assignment_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<ShardAssignmentChange>> {
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, ShardAssignmentChangeRow>(
        r#"SELECT seq, op, tenant_id, namespace, stream, shard, kind, payload
               FROM shard_assignment_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&mut *tx)
    .await?;
    let head = sqlx::query_scalar::<_, i64>("SELECT next_seq FROM shard_assignment_change_seq")
        .fetch_one(&mut *tx)
        .await? as u64;
    tx.commit().await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(ShardAssignmentChange {
            seq: row.seq as u64,
            op: parse_shard_op(&row.op)?,
            key: ShardKey {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                stream: row.stream,
                shard: row.shard as u32,
                kind: parse_shard_kind(&row.kind)?,
            },
            assignment: row.payload.map(serde_json::from_value).transpose()?,
        });
    }
    Ok(ChangeSet::page(
        items,
        store.changes_limit(),
        head,
        |change| change.seq,
    ))
}

pub(super) async fn record_replica_report(
    store: &PostgresStore,
    report: ReplicaReport,
    leader: &str,
) -> StoreResult<ReportWrite> {
    let mut tx = store.pool.begin().await?;
    // `FOR SHARE` holds the assignment row until commit, so a promotion
    // either committed before this read and is seen, or waits for the report.
    let assignment: Option<(String, i64, i64)> = sqlx::query_as(
        r#"SELECT leader, generation, incarnation FROM shard_assignments
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5
               FOR SHARE"#,
    )
    .bind(&report.key.tenant_id)
    .bind(&report.key.namespace)
    .bind(&report.key.stream)
    .bind(report.key.shard as i32)
    .bind(report.key.kind.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let Some((current_leader, generation, incarnation)) = assignment else {
        return Err(StoreError::NotFound("shard assignment".into()));
    };
    if current_leader != leader {
        return Ok(ReportWrite::NotLeader);
    }
    if generation as u64 != report.generation {
        return Ok(ReportWrite::Stale);
    }
    // Under the same row lock: a recreate cannot land between this and the write.
    if report
        .stream_incarnation
        .is_some_and(|reported| reported != incarnation as u64)
    {
        return Ok(ReportWrite::Stale);
    }
    // Locked, so a concurrent report cannot carry halts from a row this one
    // is about to replace.
    let held: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"SELECT halted FROM replica_reports
               WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 AND shard = $4 AND kind = $5
               FOR UPDATE"#,
    )
    .bind(&report.key.tenant_id)
    .bind(&report.key.namespace)
    .bind(&report.key.stream)
    .bind(report.key.shard as i32)
    .bind(report.key.kind.as_str())
    .fetch_optional(&mut *tx)
    .await?;
    let mut report = report;
    let held = match held {
        Some((halted,)) => serde_json::from_value(halted)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode halted: {err}")))?,
        None => Default::default(),
    };
    report.carry_halts(&held);
    // The WHERE is `ReplicaReport::supersedes` in SQL: a report that does not
    // supersede the held one updates no row, which is how it is told apart.
    // The foreign key is what removes the report when the assignment goes.
    let result = sqlx::query(
        r#"INSERT INTO replica_reports
                   (tenant_id, namespace, kind, stream, shard, generation, caught_up, offsets,
                    reported_at_millis, drained, leader_offset, halted, counter_offsets,
                    by_round, leader_incarnation, by_round_at,
                    follower_storage, follower_storage_at,
                    leader, node_incarnations, stamped_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $9,
                       $16, $9, $17, $18, $9)
               ON CONFLICT (tenant_id, namespace, kind, stream, shard) DO UPDATE
               SET generation = EXCLUDED.generation,
                   caught_up = EXCLUDED.caught_up,
                   offsets = EXCLUDED.offsets,
                   reported_at_millis = EXCLUDED.reported_at_millis,
                   drained = EXCLUDED.drained,
                   leader_offset = EXCLUDED.leader_offset,
                   halted = EXCLUDED.halted,
                   counter_offsets = EXCLUDED.counter_offsets,
                   by_round = EXCLUDED.by_round,
                   leader_incarnation = EXCLUDED.leader_incarnation,
                   by_round_at = EXCLUDED.by_round_at,
                   follower_storage = EXCLUDED.follower_storage,
                   follower_storage_at = EXCLUDED.follower_storage_at,
                   leader = EXCLUDED.leader,
                   node_incarnations = EXCLUDED.node_incarnations,
                   stamped_at = EXCLUDED.stamped_at
               WHERE EXCLUDED.generation > replica_reports.generation
                  OR (EXCLUDED.generation = replica_reports.generation
                      AND (EXCLUDED.leader_offset IS NULL
                           OR replica_reports.leader_offset IS NULL
                           OR EXCLUDED.leader_offset >= replica_reports.leader_offset))"#,
    )
    .bind(&report.key.tenant_id)
    .bind(&report.key.namespace)
    .bind(shard_kind_to_str(report.key.kind))
    .bind(&report.key.stream)
    .bind(report.key.shard as i32)
    .bind(report.generation as i64)
    .bind(serde_json::to_value(&report.caught_up).expect("a set of strings serializes"))
    .bind(serde_json::to_value(&report.offsets).expect("a map of integers serializes"))
    .bind(report.reported_at_millis as i64)
    .bind(report.drained)
    .bind(report.leader_offset.map(|offset| offset as i64))
    .bind(serde_json::to_value(&report.halted).expect("a map of halts serializes"))
    .bind(serde_json::to_value(&report.counter_offsets).expect("a map of integers serializes"))
    .bind(report.by_round)
    .bind(
        report
            .leader_incarnation
            .map(|incarnation| incarnation as i64),
    )
    .bind(serde_json::to_value(&report.follower_storage).expect("a map of strings serializes"))
    .bind(&report.leader)
    .bind(serde_json::to_value(&report.node_incarnations).expect("a map of integers serializes"))
    .execute(&mut *tx)
    .await;
    match result {
        Ok(done) if done.rows_affected() == 0 => Ok(ReportWrite::Stale),
        Ok(_) => {
            tx.commit().await?;
            Ok(ReportWrite::Stored)
        }
        Err(sqlx::Error::Database(err)) if err.is_foreign_key_violation() => {
            Err(StoreError::NotFound("shard assignment".into()))
        }
        Err(err) => Err(err.into()),
    }
}

pub(super) async fn list_replica_reports(store: &PostgresStore) -> StoreResult<Vec<ReplicaReport>> {
    sqlx::query_as::<_, DbReplicaReport>(
        r#"SELECT tenant_id, namespace, stream, shard, kind, generation, caught_up, offsets,
                      reported_at_millis, drained, leader_offset, halted, counter_offsets,
                      by_round, leader_incarnation, by_round_at,
                      follower_storage, follower_storage_at,
                      leader, node_incarnations, stamped_at
               FROM replica_reports
               ORDER BY tenant_id, namespace, kind, stream, shard"#,
    )
    .fetch_all(&store.pool)
    .await?
    .into_iter()
    .map(replica_report_from_db)
    .collect()
}

fn shard_from_db(row: DbShardAssignment) -> StoreResult<ShardAssignment> {
    Ok(ShardAssignment {
        key: ShardKey {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            stream: row.stream,
            shard: row.shard as u32,
            kind: parse_shard_kind(&row.kind)?,
        },
        leader: row.leader,
        replicas: serde_json::from_value(row.replicas)?,
        generation: row.generation as u64,
        state: parse_shard_state(&row.state)?,
        successor: row.successor,
        joining: row.joining,
        move_started_at_millis: row.move_started_at_millis.map(|millis| millis as u64),
        move_reason: row
            .move_reason
            .as_deref()
            .map(|reason| {
                MoveReason::parse(reason).ok_or_else(|| {
                    StoreError::Unexpected(anyhow!("unknown move reason {reason:?}"))
                })
            })
            .transpose()?,
        incarnation: row.incarnation as u64,
    })
}

fn replica_report_from_db(row: DbReplicaReport) -> StoreResult<ReplicaReport> {
    // Only the report that set the flag speaks for it: a member that predates
    // the columns updates the row and leaves them behind (migration 0029).
    let by_round = row.by_round && row.by_round_at == Some(row.reported_at_millis);
    // The same for the follower identities (migration 0030): left behind by
    // an older member's update, they would vouch for a report they never
    // came with.
    let follower_storage = match row.follower_storage {
        Some(ids) if row.follower_storage_at == Some(row.reported_at_millis) => {
            serde_json::from_value::<Option<_>>(ids)
                .map_err(|err| StoreError::Unexpected(anyhow!("decode follower_storage: {err}")))?
                .unwrap_or_default()
        }
        _ => Default::default(),
    };
    // And for who made it and the incarnations it was taken at (0031).
    let stamped = row.stamped_at == Some(row.reported_at_millis);
    let node_incarnations = match row.node_incarnations {
        Some(incarnations) if stamped => serde_json::from_value::<Option<_>>(incarnations)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode node_incarnations: {err}")))?
            .unwrap_or_default(),
        _ => Default::default(),
    };
    Ok(ReplicaReport {
        key: ShardKey {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            stream: row.stream,
            shard: row.shard as u32,
            kind: parse_shard_kind(&row.kind)?,
        },
        generation: row.generation as u64,
        caught_up: serde_json::from_value(row.caught_up)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode caught_up: {err}")))?,
        offsets: serde_json::from_value(row.offsets)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode offsets: {err}")))?,
        reported_at_millis: row.reported_at_millis as u64,
        drained: row.drained,
        leader_offset: row.leader_offset.map(|offset| offset as u64),
        halted: serde_json::from_value(row.halted)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode halted: {err}")))?,
        counter_offsets: serde_json::from_value(row.counter_offsets)
            .map_err(|err| StoreError::Unexpected(anyhow!("decode counter_offsets: {err}")))?,
        stream_incarnation: None,
        by_round,
        leader_incarnation: row
            .leader_incarnation
            .filter(|_| by_round)
            .map(|incarnation| incarnation as u64),
        follower_storage,
        leader: row.leader.filter(|_| stamped),
        node_incarnations,
    })
}

/// Append a shard change, taking its `seq` from the locked counter.
///
/// Same construction as `record_node_change`: the row lock makes seq order equal
/// commit order, which is what lets a consumer resume at `next_seq` without
/// skipping a change. See 0006_shard_assignments.sql.
async fn record_shard_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: ShardAssignmentChangeOp,
    key: &ShardKey,
    assignment: Option<&ShardAssignment>,
) -> StoreResult<()> {
    let seq = take_change_seq(tx).await?;
    insert_shard_change(tx, seq, op, key, assignment).await
}

/// The next shard change's `seq`, taken from the locked counter.
async fn take_change_seq(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> StoreResult<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "UPDATE shard_assignment_change_seq SET next_seq = next_seq + 1 RETURNING next_seq - 1",
    )
    .fetch_one(&mut **tx)
    .await?)
}

/// Append a shard change at a `seq` already taken with [`take_change_seq`].
async fn insert_shard_change(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    seq: i64,
    op: ShardAssignmentChangeOp,
    key: &ShardKey,
    assignment: Option<&ShardAssignment>,
) -> StoreResult<()> {
    sqlx::query(
        r#"INSERT INTO shard_assignment_changes (seq, op, tenant_id, namespace, stream, shard, kind, payload)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
    )
    .bind(seq)
    .bind(shard_op_to_str(op))
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(key.shard as i32)
    .bind(key.kind.as_str())
    .bind(assignment.map(serde_json::to_value).transpose()?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn shard_state_to_str(state: ShardState) -> &'static str {
    match state {
        ShardState::Assigning => "assigning",
        ShardState::Active => "active",
        ShardState::Draining => "draining",
    }
}

fn parse_shard_state(value: &str) -> StoreResult<ShardState> {
    match value {
        "assigning" => Ok(ShardState::Assigning),
        "active" => Ok(ShardState::Active),
        "draining" => Ok(ShardState::Draining),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard state: {other}"
        ))),
    }
}

fn shard_kind_to_str(kind: ShardKind) -> &'static str {
    match kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    }
}

fn parse_shard_kind(value: &str) -> StoreResult<ShardKind> {
    match value {
        "stream" => Ok(ShardKind::Stream),
        "cache" => Ok(ShardKind::Cache),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard kind: {other}"
        ))),
    }
}

fn shard_op_to_str(op: ShardAssignmentChangeOp) -> &'static str {
    match op {
        ShardAssignmentChangeOp::Assigned => "assigned",
        ShardAssignmentChangeOp::Updated => "updated",
        ShardAssignmentChangeOp::Unassigned => "unassigned",
    }
}

fn parse_shard_op(value: &str) -> StoreResult<ShardAssignmentChangeOp> {
    match value {
        "assigned" => Ok(ShardAssignmentChangeOp::Assigned),
        "updated" => Ok(ShardAssignmentChangeOp::Updated),
        "unassigned" => Ok(ShardAssignmentChangeOp::Unassigned),
        other => Err(StoreError::Unexpected(anyhow!(
            "unknown shard change op: {other}"
        ))),
    }
}

fn invalid_shard(err: ShardValidationError) -> StoreError {
    StoreError::Conflict(err.to_string())
}

/// A missing row reads as not paused: the migration inserts it, and a
/// database restored without it should place as it always did.
pub(super) async fn moves_paused(store: &PostgresStore) -> StoreResult<bool> {
    let paused: Option<bool> =
        sqlx::query_scalar("SELECT moves_paused FROM placement_settings WHERE id = 1")
            .fetch_optional(&store.pool)
            .await?;
    Ok(paused.unwrap_or(false))
}

pub(super) async fn set_moves_paused(store: &PostgresStore, paused: bool) -> StoreResult<()> {
    sqlx::query(
        r#"INSERT INTO placement_settings (id, moves_paused) VALUES (1, $1)
           ON CONFLICT (id) DO UPDATE SET moves_paused = EXCLUDED.moves_paused, updated_at = now()"#,
    )
    .bind(paused)
    .execute(&store.pool)
    .await?;
    Ok(())
}

/// A missing row reads as zero. The migration inserts it, and acquiring the
/// lease puts it back.
pub(super) async fn placement_token(store: &PostgresStore) -> StoreResult<u64> {
    let token: Option<i64> = sqlx::query_scalar("SELECT token FROM placement_lease WHERE id = 1")
        .fetch_optional(&store.pool)
        .await?;
    Ok(token.unwrap_or(0) as u64)
}

pub(super) async fn acquire_placement_lease(
    store: &PostgresStore,
    holder: &str,
    ttl_millis: u64,
) -> StoreResult<Option<PlacementLease>> {
    sqlx::query("INSERT INTO placement_lease (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
        .execute(&store.pool)
        .await?;
    let mut tx = store.pool.begin().await?;
    // Expiry is judged by the database's clock, the one every instance
    // shares, never by the instance's own.
    let (current, lapsed, token): (Option<String>, bool, i64) = sqlx::query_as(
        r#"SELECT holder, expires_at IS NULL OR expires_at <= clock_timestamp(), token
           FROM placement_lease WHERE id = 1 FOR UPDATE"#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let ours = current.as_deref() == Some(holder);
    if !ours && current.is_some() && !lapsed {
        return Ok(None);
    }
    let token = if ours { token } else { token + 1 };
    sqlx::query(
        r#"UPDATE placement_lease SET holder = $1, token = $2,
               expires_at = clock_timestamp() + make_interval(secs => $3::double precision / 1000)
           WHERE id = 1"#,
    )
    .bind(holder)
    .bind(token)
    .bind(ttl_millis as f64)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(PlacementLease {
        token: token as u64,
        taken: !ours,
    }))
}

pub(super) async fn release_placement_lease(
    store: &PostgresStore,
    holder: &str,
) -> StoreResult<()> {
    // The holder stays named, so whoever takes it next still advances the
    // token.
    sqlx::query(
        "UPDATE placement_lease SET expires_at = clock_timestamp() WHERE id = 1 AND holder = $1",
    )
    .bind(holder)
    .execute(&store.pool)
    .await?;
    Ok(())
}
