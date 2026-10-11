//! Principal revocations: one row per principal, at its latest revocation.
use sqlx::Row;

use super::PostgresStore;
use crate::auth::revocation::{ALL_PRINCIPALS, PrincipalRevocation};
use crate::store::{ChangeSet, StoreResult};

/// Serializes writers, so a row is committed before any row with a higher
/// `seq` can be. A reader that sees `seq` n has seen everything below it.
const WRITE_LOCK: i64 = 0x7265766f6b65; // "revoke"

pub(super) async fn revoke_principals(
    store: &PostgresStore,
    tenant_id: &str,
    principal_ids: Vec<String>,
    revoked_at: i64,
) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(WRITE_LOCK)
        .execute(&mut *tx)
        .await?;
    let mut seq: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) + 1 FROM principal_revocations")
            .fetch_one(&mut *tx)
            .await?;
    for principal_id in principal_ids {
        sqlx::query(
            r#"INSERT INTO principal_revocations (tenant_id, principal_id, revoked_at, seq)
                   VALUES ($1, $2, $3, $4)
               ON CONFLICT (tenant_id, principal_id) DO UPDATE
                   SET revoked_at = GREATEST(principal_revocations.revoked_at, EXCLUDED.revoked_at),
                       seq = EXCLUDED.seq"#,
        )
        .bind(tenant_id)
        .bind(&principal_id)
        .bind(revoked_at)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        seq += 1;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn principal_revocations(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<PrincipalRevocation>> {
    let head: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) + 1 FROM principal_revocations")
            .fetch_one(&store.pool)
            .await?;
    let since = if since > head as u64 { 0 } else { since };
    let rows = sqlx::query(
        r#"SELECT tenant_id, principal_id, revoked_at, seq FROM principal_revocations
               WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .iter()
        .map(|row| {
            Ok(PrincipalRevocation {
                seq: row.try_get::<i64, _>("seq")? as u64,
                tenant_id: row.try_get("tenant_id")?,
                principal_id: row.try_get("principal_id")?,
                revoked_at: row.try_get("revoked_at")?,
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    let next_seq = items.last().map_or(since, |row| row.seq + 1);
    Ok(ChangeSet { items, next_seq })
}

pub(super) async fn principal_revoked_at(
    store: &PostgresStore,
    tenant_id: &str,
    principal_id: &str,
) -> StoreResult<Option<i64>> {
    let revoked_at: Option<i64> = sqlx::query_scalar(
        r#"SELECT MAX(revoked_at) FROM principal_revocations
               WHERE tenant_id = $1 AND principal_id IN ($2, $3)"#,
    )
    .bind(tenant_id)
    .bind(principal_id)
    .bind(ALL_PRINCIPALS)
    .fetch_one(&store.pool)
    .await?;
    Ok(revoked_at)
}
