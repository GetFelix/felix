//! Per-tenant auth: IdP issuers, RBAC rules, signing keys and bootstrap.
use anyhow::anyhow;
use jsonwebtoken::Algorithm;
use serde_json::Value;
use sqlx::FromRow;

use super::PostgresStore;
use crate::auth::felix_token::{SigningKey, TenantSigningKeys};
use crate::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::store::{StoreError, StoreResult};

#[derive(Debug, Clone, FromRow)]
struct DbIdpIssuer {
    issuer: String,
    audiences: Value,
    discovery_url: Option<String>,
    jwks_url: Option<String>,
    subject_claim: String,
    groups_claim: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct DbSigningKey {
    kid: String,
    alg: String,
    private_pem: Vec<u8>,
    public_pem: Vec<u8>,
    status: String,
}

#[derive(Debug, Clone, FromRow)]
struct DbPolicy {
    subject: String,
    object: String,
    action: String,
}

#[derive(Debug, Clone, FromRow)]
struct DbGrouping {
    user_id: String,
    role: String,
}

impl PostgresStore {
    /// The auth writes below are shared between the pool-backed trait methods
    /// and the single transaction `bootstrap_tenant_auth` runs, so each
    /// statement exists once rather than once per path.
    async fn upsert_idp_issuer_on(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
        issuer: &IdpIssuerConfig,
    ) -> StoreResult<()> {
        let audiences = serde_json::to_value(&issuer.audiences)?;
        sqlx::query(
            "INSERT INTO idp_issuers (tenant_id, issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, issuer) DO UPDATE SET \
                audiences = EXCLUDED.audiences, \
                discovery_url = EXCLUDED.discovery_url, \
                jwks_url = EXCLUDED.jwks_url, \
                subject_claim = EXCLUDED.subject_claim, \
                groups_claim = EXCLUDED.groups_claim",
        )
        .bind(tenant_id)
        .bind(&issuer.issuer)
        .bind(audiences)
        .bind(&issuer.discovery_url)
        .bind(&issuer.jwks_url)
        .bind(&issuer.claim_mappings.subject_claim)
        .bind(&issuer.claim_mappings.groups_claim)
        .execute(conn)
        .await?;
        Ok(())
    }

    async fn insert_rbac_policy_on(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
        policy: &PolicyRule,
    ) -> StoreResult<()> {
        sqlx::query(
            "INSERT INTO rbac_policies (tenant_id, subject, object, action) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id)
        .bind(&policy.subject)
        .bind(&policy.object)
        .bind(&policy.action)
        .execute(conn)
        .await?;
        Ok(())
    }

    async fn insert_rbac_grouping_on(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
        grouping: &GroupingRule,
    ) -> StoreResult<()> {
        sqlx::query(
            "INSERT INTO rbac_groupings (tenant_id, user_id, role) \
             VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(tenant_id)
        .bind(&grouping.user)
        .bind(&grouping.role)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// All signing keys a tenant has, or `None` when it has none yet.
    async fn load_signing_keys_on(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
    ) -> StoreResult<Option<TenantSigningKeys>> {
        // We fetch all keys to support rotation; callers will try `current` first.
        let rows: Vec<DbSigningKey> = sqlx::query_as(
            "SELECT kid, alg, private_pem, public_pem, status \
             FROM tenant_signing_keys WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(conn)
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut current: Option<SigningKey> = None;
        let mut previous = Vec::new();
        for row in rows {
            // Parse and validate EdDSA-only key material from raw bytes.
            // Private key bytes are stored as raw Ed25519 seeds, not PKCS8.
            let key = SigningKey {
                kid: row.kid,
                alg: parse_algorithm(&row.alg)?,
                private_key: decode_key(&row.private_pem, "private key")?,
                public_key: decode_key(&row.public_pem, "public key")?,
            };
            match row.status.as_str() {
                "current" => current = Some(key),
                "previous" => previous.push(key),
                _ => {}
            }
        }

        let current = current.ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
        Ok(Some(TenantSigningKeys { current, previous }))
    }

    async fn insert_signing_keys_on(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
        keys: &TenantSigningKeys,
    ) -> StoreResult<()> {
        let current_alg = algorithm_to_str(keys.current.alg);
        // Store raw Ed25519 seeds; never serialize or log these values.
        sqlx::query(
            "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
             VALUES ($1, $2, $3, $4, $5, 'current')",
        )
        .bind(tenant_id)
        .bind(&keys.current.kid)
        .bind(current_alg)
        .bind(keys.current.private_key.as_slice())
        .bind(keys.current.public_key.as_slice())
        .execute(&mut *conn)
        .await?;

        for key in &keys.previous {
            let alg = algorithm_to_str(key.alg);
            // Store previous keys for rotation; still valid for verification.
            sqlx::query(
                "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
                 VALUES ($1, $2, $3, $4, $5, 'previous')",
            )
            .bind(tenant_id)
            .bind(&key.kid)
            .bind(alg)
            .bind(key.private_key.as_slice())
            .bind(key.public_key.as_slice())
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }
}

pub(super) async fn list_idp_issuers(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<Vec<IdpIssuerConfig>> {
    let rows: Vec<DbIdpIssuer> = sqlx::query_as(
        "SELECT issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim \
             FROM idp_issuers WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_all(&store.pool)
    .await?;

    let mut issuers = Vec::with_capacity(rows.len());
    for row in rows {
        let audiences: Vec<String> = serde_json::from_value(row.audiences)
            .map_err(|err| StoreError::Unexpected(anyhow!("invalid audiences json: {err}")))?;
        issuers.push(IdpIssuerConfig {
            issuer: row.issuer,
            audiences,
            discovery_url: row.discovery_url,
            jwks_url: row.jwks_url,
            claim_mappings: ClaimMappings {
                subject_claim: row.subject_claim,
                groups_claim: row.groups_claim,
            },
        });
    }
    Ok(issuers)
}

pub(super) async fn upsert_idp_issuer(
    store: &PostgresStore,
    tenant_id: &str,
    issuer: IdpIssuerConfig,
) -> StoreResult<()> {
    let mut conn = store.pool.acquire().await?;
    PostgresStore::upsert_idp_issuer_on(&mut conn, tenant_id, &issuer).await
}

pub(super) async fn delete_idp_issuer(
    store: &PostgresStore,
    tenant_id: &str,
    issuer: &str,
) -> StoreResult<()> {
    sqlx::query("DELETE FROM idp_issuers WHERE tenant_id = $1 AND issuer = $2")
        .bind(tenant_id)
        .bind(issuer)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn list_rbac_policies(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<Vec<PolicyRule>> {
    let rows: Vec<DbPolicy> =
        sqlx::query_as("SELECT subject, object, action FROM rbac_policies WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_all(&store.pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|row| PolicyRule {
            subject: row.subject,
            object: row.object,
            action: row.action,
        })
        .collect())
}

pub(super) async fn list_rbac_groupings(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<Vec<GroupingRule>> {
    let rows: Vec<DbGrouping> =
        sqlx::query_as("SELECT user_id, role FROM rbac_groupings WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_all(&store.pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|row| GroupingRule {
            user: row.user_id,
            role: row.role,
        })
        .collect())
}

pub(super) async fn add_rbac_policy(
    store: &PostgresStore,
    tenant_id: &str,
    policy: PolicyRule,
) -> StoreResult<()> {
    let mut conn = store.pool.acquire().await?;
    PostgresStore::insert_rbac_policy_on(&mut conn, tenant_id, &policy).await
}

pub(super) async fn add_rbac_grouping(
    store: &PostgresStore,
    tenant_id: &str,
    grouping: GroupingRule,
) -> StoreResult<()> {
    let mut conn = store.pool.acquire().await?;
    PostgresStore::insert_rbac_grouping_on(&mut conn, tenant_id, &grouping).await
}

pub(super) async fn remove_rbac_policy(
    store: &PostgresStore,
    tenant_id: &str,
    policy: &PolicyRule,
) -> StoreResult<()> {
    let result = sqlx::query(
        "DELETE FROM rbac_policies \
         WHERE tenant_id = $1 AND subject = $2 AND object = $3 AND action = $4",
    )
    .bind(tenant_id)
    .bind(&policy.subject)
    .bind(&policy.object)
    .bind(&policy.action)
    .execute(&store.pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(StoreError::NotFound("policy".into()));
    }
    Ok(())
}

pub(super) async fn remove_rbac_grouping(
    store: &PostgresStore,
    tenant_id: &str,
    grouping: &GroupingRule,
) -> StoreResult<()> {
    let result = sqlx::query(
        "DELETE FROM rbac_groupings WHERE tenant_id = $1 AND user_id = $2 AND role = $3",
    )
    .bind(tenant_id)
    .bind(&grouping.user)
    .bind(&grouping.role)
    .execute(&store.pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(StoreError::NotFound("grouping".into()));
    }
    Ok(())
}

pub(super) async fn get_tenant_signing_keys(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<TenantSigningKeys> {
    let mut conn = store.pool.acquire().await?;
    PostgresStore::load_signing_keys_on(&mut conn, tenant_id)
        .await?
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))
}

pub(super) async fn set_tenant_signing_keys(
    store: &PostgresStore,
    tenant_id: &str,
    keys: TenantSigningKeys,
) -> StoreResult<()> {
    // Validate that keys are EdDSA and public keys match private seeds.
    // This prevents accidental RSA reintroduction and corrupted key storage.
    keys.validate()
        .map_err(|err| StoreError::Unexpected(anyhow!(err)))?;
    let mut tx = store.pool.begin().await?;
    // Replace all keys atomically to keep `current` and `previous` consistent.
    sqlx::query("DELETE FROM tenant_signing_keys WHERE tenant_id = $1")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;
    PostgresStore::insert_signing_keys_on(&mut tx, tenant_id, &keys).await?;
    tx.commit().await?;
    // Invalidate derived key cache so new keys take effect immediately.
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(())
}

/// Open a transaction holding the tenant row, the lock every signing-key
/// write takes: two rotations interleaving could otherwise leave the tenant
/// with no current key, or two.
async fn lock_tenant_keys(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<(
    sqlx::Transaction<'static, sqlx::Postgres>,
    TenantSigningKeys,
)> {
    let mut tx = store.pool.begin().await?;
    let tenant: Option<String> =
        sqlx::query_scalar("SELECT tenant_id FROM tenants WHERE tenant_id = $1 FOR UPDATE")
            .bind(tenant_id)
            .fetch_optional(&mut *tx)
            .await?;
    if tenant.is_none() {
        return Err(StoreError::NotFound("tenant".into()));
    }
    let keys = PostgresStore::load_signing_keys_on(&mut tx, tenant_id)
        .await?
        .ok_or_else(|| StoreError::NotFound("signing keys".into()))?;
    Ok((tx, keys))
}

/// Commit, then read back what is stored. The cache is dropped only after
/// the commit, so an uncommitted key never enters it.
async fn finish_key_change(
    store: &PostgresStore,
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    tenant_id: &str,
) -> StoreResult<TenantSigningKeys> {
    tx.commit().await?;
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    get_tenant_signing_keys(store, tenant_id).await
}

pub(super) async fn stage_signing_key(
    store: &PostgresStore,
    tenant_id: &str,
    key: SigningKey,
) -> StoreResult<TenantSigningKeys> {
    key.validate()
        .map_err(|err| StoreError::Unexpected(anyhow!(err)))?;
    let (mut tx, keys) = lock_tenant_keys(store, tenant_id).await?;
    if keys.all_keys().any(|existing| existing.kid == key.kid) {
        return Err(StoreError::Conflict("kid already in use".into()));
    }
    sqlx::query(
        "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
         VALUES ($1, $2, $3, $4, $5, 'previous')",
    )
    .bind(tenant_id)
    .bind(&key.kid)
    .bind(algorithm_to_str(key.alg))
    .bind(key.private_key.as_slice())
    .bind(key.public_key.as_slice())
    .execute(&mut *tx)
    .await?;
    finish_key_change(store, tx, tenant_id).await
}

pub(super) async fn activate_signing_key(
    store: &PostgresStore,
    tenant_id: &str,
    kid: &str,
) -> StoreResult<TenantSigningKeys> {
    let (mut tx, keys) = lock_tenant_keys(store, tenant_id).await?;
    if keys.current.kid == kid {
        return Ok(keys);
    }
    if !keys.previous.iter().any(|key| key.kid == kid) {
        return Err(StoreError::NotFound("signing key".into()));
    }
    sqlx::query(
        "UPDATE tenant_signing_keys SET status = 'previous' \
         WHERE tenant_id = $1 AND status = 'current'",
    )
    .bind(tenant_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE tenant_signing_keys SET status = 'current' \
         WHERE tenant_id = $1 AND kid = $2 AND status = 'previous'",
    )
    .bind(tenant_id)
    .bind(kid)
    .execute(&mut *tx)
    .await?;
    finish_key_change(store, tx, tenant_id).await
}

pub(super) async fn retire_signing_key(
    store: &PostgresStore,
    tenant_id: &str,
    kid: &str,
) -> StoreResult<TenantSigningKeys> {
    let (mut tx, keys) = lock_tenant_keys(store, tenant_id).await?;
    if keys.current.kid == kid {
        return Err(StoreError::Conflict(
            "the current signing key cannot be retired".into(),
        ));
    }
    let result = sqlx::query(
        "DELETE FROM tenant_signing_keys \
         WHERE tenant_id = $1 AND kid = $2 AND status = 'previous'",
    )
    .bind(tenant_id)
    .bind(kid)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() == 0 {
        return Err(StoreError::NotFound("signing key".into()));
    }
    finish_key_change(store, tx, tenant_id).await
}

pub(super) async fn tenant_auth_is_bootstrapped(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<bool> {
    let row: Option<(bool,)> =
        sqlx::query_as("SELECT auth_bootstrapped FROM tenants WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_optional(&store.pool)
            .await?;
    Ok(row.map(|(value,)| value).unwrap_or(false))
}

pub(super) async fn set_tenant_auth_bootstrapped(
    store: &PostgresStore,
    tenant_id: &str,
    bootstrapped: bool,
) -> StoreResult<()> {
    let result = sqlx::query("UPDATE tenants SET auth_bootstrapped = $2 WHERE tenant_id = $1")
        .bind(tenant_id)
        .bind(bootstrapped)
        .execute(&store.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(StoreError::NotFound("tenant".into()));
    }
    Ok(())
}

pub(super) async fn ensure_signing_key_current(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<TenantSigningKeys> {
    if let Some(keys) =
        PostgresStore::load_signing_keys_on(&mut *store.pool.acquire().await?, tenant_id).await?
    {
        return Ok(keys);
    }
    // Nothing unique says "one current key per tenant" (kids differ), so
    // `ON CONFLICT` cannot pick a winner. The tenant row lock does, as in
    // `bootstrap_tenant_auth`: a caller that waited re-reads and returns the
    // winner's keys instead of storing its own over them.
    let mut tx = store.pool.begin().await?;
    let tenant: Option<String> =
        sqlx::query_scalar("SELECT tenant_id FROM tenants WHERE tenant_id = $1 FOR UPDATE")
            .bind(tenant_id)
            .fetch_optional(&mut *tx)
            .await?;
    if tenant.is_none() {
        return Err(StoreError::NotFound("tenant".into()));
    }
    let keys = match PostgresStore::load_signing_keys_on(&mut tx, tenant_id).await? {
        Some(keys) => keys,
        None => {
            let keys = crate::auth::keys::generate_signing_keys()?;
            PostgresStore::insert_signing_keys_on(&mut tx, tenant_id, &keys).await?;
            keys
        }
    };
    tx.commit().await?;
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(keys)
}

pub(super) async fn seed_rbac_policies_and_groupings(
    store: &PostgresStore,
    tenant_id: &str,
    policies: Vec<PolicyRule>,
    groupings: Vec<GroupingRule>,
) -> StoreResult<()> {
    // Seed policy/grouping data atomically to avoid partial authorization state.
    let mut tx = store.pool.begin().await?;
    for policy in &policies {
        PostgresStore::insert_rbac_policy_on(&mut tx, tenant_id, policy).await?;
    }
    for grouping in &groupings {
        PostgresStore::insert_rbac_grouping_on(&mut tx, tenant_id, grouping).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn bootstrap_tenant_auth(
    store: &PostgresStore,
    tenant_id: &str,
    seed: crate::store::TenantAuthSeed,
) -> StoreResult<TenantSigningKeys> {
    let mut tx = store.pool.begin().await?;

    // The tenant row is the bootstrap lock: `FOR UPDATE` serializes racing
    // initializes across every control-plane instance, and whoever waited
    // sees the winner's committed flag rather than the stale `false` it
    // read before blocking.
    let bootstrapped: Option<bool> =
        sqlx::query_scalar("SELECT auth_bootstrapped FROM tenants WHERE tenant_id = $1 FOR UPDATE")
            .bind(tenant_id)
            .fetch_optional(&mut *tx)
            .await?;
    match bootstrapped {
        None => return Err(StoreError::NotFound("tenant".into())),
        Some(true) => {
            return Err(StoreError::Conflict("tenant already initialized".into()));
        }
        Some(false) => {}
    }

    let keys = match PostgresStore::load_signing_keys_on(&mut tx, tenant_id).await? {
        Some(keys) => keys,
        None => {
            // The seed carries the keys (generated at the API layer):
            // the store must stay free of randomness so the Raft state
            // machine can apply the same operation identically on every
            // replica. Existing keys always win over the seed's.
            seed.signing_keys
                .validate()
                .map_err(|err| StoreError::Unexpected(anyhow!(err)))?;
            PostgresStore::insert_signing_keys_on(&mut tx, tenant_id, &seed.signing_keys).await?;
            seed.signing_keys.clone()
        }
    };

    for issuer in &seed.issuers {
        PostgresStore::upsert_idp_issuer_on(&mut tx, tenant_id, issuer).await?;
    }
    for policy in &seed.policies {
        PostgresStore::insert_rbac_policy_on(&mut tx, tenant_id, policy).await?;
    }
    for grouping in &seed.groupings {
        PostgresStore::insert_rbac_grouping_on(&mut tx, tenant_id, grouping).await?;
    }

    sqlx::query("UPDATE tenants SET auth_bootstrapped = TRUE WHERE tenant_id = $1")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    // Only after commit: an uncommitted key must never enter the cache.
    crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
    Ok(keys)
}

pub(super) fn parse_algorithm(value: &str) -> StoreResult<Algorithm> {
    // Felix tokens must remain EdDSA; reject any other algorithm on load.
    match value {
        "EdDSA" => Ok(Algorithm::EdDSA),
        _ => Err(StoreError::Unexpected(anyhow!("invalid alg {value}"))),
    }
}

pub(super) fn algorithm_to_str(value: Algorithm) -> &'static str {
    // Persist only EdDSA to prevent accidental RSA reintroduction.
    match value {
        Algorithm::EdDSA => "EdDSA",
        _ => "EdDSA",
    }
}

pub(super) fn decode_key(value: &[u8], label: &str) -> StoreResult<[u8; 32]> {
    value
        .try_into()
        .map_err(|_| StoreError::Unexpected(anyhow!("invalid {label} length")))
}
