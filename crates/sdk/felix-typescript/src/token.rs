//! The token provider: the app's JavaScript function behind the Rust client's
//! `RefreshingToken`, which keeps the timing (refresh at two thirds of a
//! token's life, one fetch at a time, keep the old token if a fetch fails,
//! retry a rejected one).

use std::sync::Arc;

use anyhow::anyhow;
use felix_client::RefreshingToken;
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{Env, JsFunction, Result};

/// The app's fetch function, called on the JS thread.
///
/// `index.js` wraps what the app passes so that this always returns a promise
/// that resolves with a string or rejects with an `Error`.
pub(crate) type Fetch = Arc<ThreadsafeFunction<(), ErrorStrategy::Fatal>>;

/// Make `callback` callable from any thread without keeping Node alive.
///
/// An unreferenced function does not hold the event loop open, so an idle
/// client never stops the process from exiting. The handle is released once,
/// when the last clone of the returned `Fetch` is dropped.
pub(crate) fn fetch_from(env: &Env, callback: &JsFunction) -> Result<Fetch> {
    let mut fetch: ThreadsafeFunction<(), ErrorStrategy::Fatal> =
        callback.create_threadsafe_function(0, |_| Ok(Vec::<()>::new()))?;
    fetch.unref(env)?;
    Ok(Arc::new(fetch))
}

/// The token source a client authenticates with: `initial` first, if the
/// caller has one, then whatever the app's function returns.
pub(crate) fn provider(fetch: &Fetch, initial: Option<String>) -> Arc<RefreshingToken> {
    let fetch = Arc::clone(fetch);
    let call = move || {
        let fetch = Arc::clone(&fetch);
        async move {
            // Failures carry the app's own message: the client wraps it in its
            // auth error and the caller reads it back unchanged.
            let reply: Promise<String> = fetch
                .call_async(())
                .await
                .map_err(|err| anyhow!("{}", err.reason))?;
            reply.await.map_err(|err| anyhow!("{}", err.reason))
        }
    };
    // An empty string is no token: the first auth fetches instead of sending
    // one the broker will refuse.
    Arc::new(match initial.filter(|token| !token.is_empty()) {
        Some(initial) => RefreshingToken::with_initial(initial, call),
        None => RefreshingToken::new(call),
    })
}
