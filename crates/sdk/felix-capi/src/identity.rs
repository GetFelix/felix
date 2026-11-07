//! Acting as another principal over a client's connections.

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::boundary::{Failure, guard, optional_str, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, FelixConnectCallback};
use crate::completion::Completion;
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_OK, FelixStatus};
use crate::token::{FelixTokenFetch, FelixTokenRelease, TokenSource, provider};

/// A new client that acts as `tenant_id` with `token` over `client`'s
/// connections, for a gateway serving many principals from one pool.
///
/// The new client follows shards and failovers as `client` does, but its
/// requests are checked against `token`'s grants alone. It is a full client:
/// free it with `felix_client_free`. It shares `client`'s runtime and
/// connections, so freeing `client` first does not stop it, and freeing it
/// closes only its own streams. Keep it alive while subscriptions made from
/// it are in use. A refused token completes with `FELIX_STATUS_AUTH`.
///
/// `token` is fixed for the new client's life; make another client when it
/// expires, or use `felix_client_with_identity_provider`. A lease token from
/// `felix_client_cache_put_if_lease` is a valid `token`. `callback` runs once
/// with the new client, unless this returns something other than
/// `FELIX_STATUS_OK`; `FelixConnectCallback` describes it.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is null
/// or a live token, and `callback` is safe to call with `user_data` from any
/// thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_with_identity(
    client: *const FelixClient,
    tenant_id: *const c_char,
    token: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixConnectCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: NUL-terminated strings, per the contract above.
        let (tenant_id, token) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(token, "token")?.to_string(),
            )
        };
        if token.is_empty() {
            return Err(Failure::invalid("token is empty"));
        }
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixClient>>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let shared = Arc::clone(&client.shared);
        let ask = async move { cluster.with_identity_token(tenant_id, token).await };
        spawn_read(client, user, done, ask, move |identity| {
            Box::new(FelixClient::acting_as(shared, identity, None))
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_with_identity` for a token that expires: the new client asks
/// `token_fetch` for a fresh one when it needs it, as a client connected with
/// `FelixConnectOptions.token_fetch` does.
///
/// `token` is the first token and may be null, in which case the first
/// authentication calls `token_fetch`. An empty `token` counts as none. Return
/// values, `callback` and the free rules are those of
/// `felix_client_with_identity`.
///
/// `token_fetch` is described by `FelixTokenFetch`; answer each reply with
/// `felix_token_reply_ok` or `felix_token_reply_err`. When this returns
/// `FELIX_STATUS_OK`, `token_release` runs exactly once later, after the new
/// client is freed (or the call fails or is cancelled) and every reply has
/// been answered. When it returns anything else `token_release` never runs and
/// `token_user_data` is still the caller's. `token_fetch` is required.
///
/// # Safety
///
/// As for `felix_client_with_identity`, with `token` null or NUL-terminated,
/// and `token_fetch`, `token_release` and `token_user_data` as
/// `FelixConnectOptions` documents them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_with_identity_provider(
    client: *const FelixClient,
    tenant_id: *const c_char,
    token: *const c_char,
    token_fetch: FelixTokenFetch,
    token_user_data: *mut c_void,
    token_release: FelixTokenRelease,
    cancel: *const FelixCancelToken,
    callback: FelixConnectCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: NUL-terminated or null strings, per the contract above.
        let (tenant_id, token) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                optional_str(token, "token")?.map(str::to_string),
            )
        };
        let fetch = token_fetch.ok_or_else(|| Failure::invalid("token_fetch is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixClient>>::new(callback, user_data)?;
        // Built last: from here nothing fails, so `token_release` runs only
        // when this returns `FELIX_STATUS_OK`.
        let tokens = Arc::new(TokenSource::new(fetch, token_user_data, token_release));
        let source = provider(&tokens, token.as_deref());
        let cluster = Arc::clone(client.cluster());
        let shared = Arc::clone(&client.shared);
        let ask = async move { cluster.with_identity(tenant_id, source).await };
        spawn_read(client, user, done, ask, move |identity| {
            Box::new(FelixClient::acting_as(shared, identity, Some(tokens)))
        });
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
