//! Refresh tokens, spent and revoked in single statements.
use sqlx::Row;

use super::PostgresStore;
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::store::StoreResult;

impl PostgresStore {
    /// Build a [`RefreshToken`] from a row, with the two flags supplied.
    ///
    /// The flags are passed rather than read because the `UPDATE ... RETURNING`
    /// path knows them and does not select them back.
    fn refresh_row(
        tenant_id: &str,
        token_id: &str,
        row: &sqlx::postgres::PgRow,
        used: bool,
        revoked: bool,
    ) -> StoreResult<RefreshToken> {
        let groups: serde_json::Value = row.try_get("groups")?;
        let narrowing: Option<serde_json::Value> = row.try_get("narrowing")?;
        Ok(RefreshToken {
            token_id: token_id.to_string(),
            tenant_id: tenant_id.to_string(),
            principal_id: row.try_get("principal_id")?,
            groups: serde_json::from_value(groups)?,
            secret_hash: row.try_get("secret_hash")?,
            family_id: row.try_get("family_id")?,
            issued_at_secs: row.try_get("issued_at_secs")?,
            expires_at_secs: row.try_get("expires_at_secs")?,
            used,
            revoked,
            narrowing: narrowing.map(serde_json::from_value).transpose()?,
        })
    }
}

pub(super) async fn insert_refresh_token(
    store: &PostgresStore,
    token: RefreshToken,
) -> StoreResult<()> {
    sqlx::query(
        r#"INSERT INTO refresh_tokens
                 (tenant_id, token_id, principal_id, groups, secret_hash, family_id,
                  issued_at_secs, expires_at_secs, used, revoked, narrowing)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#,
    )
    .bind(&token.tenant_id)
    .bind(&token.token_id)
    .bind(&token.principal_id)
    .bind(serde_json::to_value(&token.groups)?)
    .bind(&token.secret_hash)
    .bind(&token.family_id)
    .bind(token.issued_at_secs)
    .bind(token.expires_at_secs)
    .bind(token.used)
    .bind(token.revoked)
    .bind(
        token
            .narrowing
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
    )
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn take_refresh_token(
    store: &PostgresStore,
    tenant_id: &str,
    token_id: &str,
    now_secs: i64,
) -> StoreResult<RefreshTokenTake> {
    // One statement, so the liveness test and the spend cannot be
    // interleaved. `used = FALSE` in the WHERE clause is what makes this a
    // compare-and-set: a second refresh racing the first updates no row and
    // reads the token back as already spent.
    let updated = sqlx::query(
        r#"UPDATE refresh_tokens SET used = TRUE
               WHERE tenant_id = $1 AND token_id = $2
                 AND used = FALSE AND revoked = FALSE AND expires_at_secs > $3
               RETURNING principal_id, groups, secret_hash, family_id,
                         issued_at_secs, expires_at_secs, narrowing"#,
    )
    .bind(tenant_id)
    .bind(token_id)
    .bind(now_secs)
    .fetch_optional(&store.pool)
    .await?;

    if let Some(row) = updated {
        return Ok(RefreshTokenTake::Taken(Box::new(
            PostgresStore::refresh_row(tenant_id, token_id, &row, true, false)?,
        )));
    }

    // Nothing was spent. Either there is no such token, or it was not
    // usable — and "already spent" has to be told apart from the rest,
    // because only that one means someone is holding a copy.
    let existing = sqlx::query(
        r#"SELECT principal_id, groups, secret_hash, family_id,
                      issued_at_secs, expires_at_secs, used, revoked, narrowing
               FROM refresh_tokens WHERE tenant_id = $1 AND token_id = $2"#,
    )
    .bind(tenant_id)
    .bind(token_id)
    .fetch_optional(&store.pool)
    .await?;

    let Some(row) = existing else {
        return Ok(RefreshTokenTake::Unusable);
    };
    let used: bool = row.try_get("used")?;
    let revoked: bool = row.try_get("revoked")?;
    if used {
        return Ok(RefreshTokenTake::Replayed(Box::new(
            PostgresStore::refresh_row(tenant_id, token_id, &row, used, revoked)?,
        )));
    }
    Ok(RefreshTokenTake::Unusable)
}

pub(super) async fn revoke_refresh_family(
    store: &PostgresStore,
    tenant_id: &str,
    family_id: &str,
) -> StoreResult<u64> {
    let result = sqlx::query(
        r#"UPDATE refresh_tokens SET revoked = TRUE
               WHERE tenant_id = $1 AND family_id = $2 AND revoked = FALSE"#,
    )
    .bind(tenant_id)
    .bind(family_id)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn revoke_refresh_tokens_for_principal(
    store: &PostgresStore,
    tenant_id: &str,
    principal_id: &str,
) -> StoreResult<u64> {
    let result = sqlx::query(
        r#"UPDATE refresh_tokens SET revoked = TRUE
               WHERE tenant_id = $1 AND principal_id = $2 AND revoked = FALSE"#,
    )
    .bind(tenant_id)
    .bind(principal_id)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}

pub(super) async fn purge_expired_refresh_tokens(
    store: &PostgresStore,
    before_secs: i64,
) -> StoreResult<u64> {
    let result = sqlx::query(r#"DELETE FROM refresh_tokens WHERE expires_at_secs < $1"#)
        .bind(before_secs)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected())
}
