//! Refresh tokens, spent and revoked under one lock.
use super::InMemoryStore;
use crate::auth::refresh_token::{RefreshToken, RefreshTokenTake};
use crate::store::StoreResult;

pub(super) async fn insert_refresh_token(
    store: &InMemoryStore,
    token: RefreshToken,
) -> StoreResult<()> {
    store
        .refresh_tokens
        .write()
        .await
        .insert((token.tenant_id.clone(), token.token_id.clone()), token);
    Ok(())
}

pub(super) async fn take_refresh_token(
    store: &InMemoryStore,
    tenant_id: &str,
    token_id: &str,
    now_secs: i64,
) -> StoreResult<RefreshTokenTake> {
    // The write lock is taken for the read as well, which is the point:
    // read-then-write under separate locks lets two refreshes both find the
    // token live and both spend it.
    let mut tokens = store.refresh_tokens.write().await;
    let key = (tenant_id.to_string(), token_id.to_string());
    let Some(token) = tokens.get_mut(&key) else {
        return Ok(RefreshTokenTake::Unusable);
    };
    if token.used {
        return Ok(RefreshTokenTake::Replayed(Box::new(token.clone())));
    }
    if !token.is_live(now_secs) {
        return Ok(RefreshTokenTake::Unusable);
    }
    token.used = true;
    Ok(RefreshTokenTake::Taken(Box::new(token.clone())))
}

pub(super) async fn peek_refresh_token(
    store: &InMemoryStore,
    tenant_id: &str,
    token_id: &str,
) -> StoreResult<Option<RefreshToken>> {
    Ok(store
        .refresh_tokens
        .read()
        .await
        .get(&(tenant_id.to_string(), token_id.to_string()))
        .cloned())
}

pub(super) async fn rotate_refresh_token(
    store: &InMemoryStore,
    tenant_id: &str,
    token_id: &str,
    now_secs: i64,
    next: RefreshToken,
) -> StoreResult<RefreshTokenTake> {
    // One lock for the spend and the insert, so no reader sees one without
    // the other.
    let mut tokens = store.refresh_tokens.write().await;
    let key = (tenant_id.to_string(), token_id.to_string());
    let Some(token) = tokens.get_mut(&key) else {
        return Ok(RefreshTokenTake::Unusable);
    };
    if token.used {
        return Ok(RefreshTokenTake::Replayed(Box::new(token.clone())));
    }
    if !token.is_live(now_secs) {
        return Ok(RefreshTokenTake::Unusable);
    }
    token.used = true;
    let taken = token.clone();
    tokens.insert((next.tenant_id.clone(), next.token_id.clone()), next);
    Ok(RefreshTokenTake::Taken(Box::new(taken)))
}

pub(super) async fn revoke_refresh_family(
    store: &InMemoryStore,
    tenant_id: &str,
    family_id: &str,
) -> StoreResult<u64> {
    let mut tokens = store.refresh_tokens.write().await;
    let mut revoked = 0;
    for token in tokens.values_mut() {
        if token.tenant_id == tenant_id && token.family_id == family_id && !token.revoked {
            token.revoked = true;
            revoked += 1;
        }
    }
    Ok(revoked)
}

pub(super) async fn revoke_refresh_tokens_for_principal(
    store: &InMemoryStore,
    tenant_id: &str,
    principal_id: &str,
) -> StoreResult<u64> {
    let mut tokens = store.refresh_tokens.write().await;
    let mut revoked = 0;
    for token in tokens.values_mut() {
        if token.tenant_id == tenant_id && token.principal_id == principal_id && !token.revoked {
            token.revoked = true;
            revoked += 1;
        }
    }
    Ok(revoked)
}

pub(super) async fn purge_expired_refresh_tokens(
    store: &InMemoryStore,
    before_secs: i64,
) -> StoreResult<u64> {
    let mut tokens = store.refresh_tokens.write().await;
    let before = tokens.len();
    tokens.retain(|_, token| token.expires_at_secs >= before_secs);
    Ok((before - tokens.len()) as u64)
}
