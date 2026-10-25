//! The token provider: the app's JavaScript function behind the Rust client's
//! `RefreshingToken`, which keeps the timing (refresh at two thirds of a
//! token's life, one fetch at a time, keep the old token if a fetch fails,
//! retry a rejected one).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::anyhow;
use felix_client::RefreshingToken;
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, JsFunction, JsUnknown, Result};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// What the native side asks of the JS wrapper: start fetch number `id`, or
/// (`abort`) abort the signal of the fetch with that id.
type Call = (u32, bool);

/// The app's fetch function, called on the JS thread.
///
/// `index.js` wraps what the app passes so that a fetch call returns a promise
/// that resolves with a string or rejects with an `Error`. Fetches and aborts
/// share one function so an abort can never overtake the fetch it belongs to.
pub(crate) struct Fetch {
    function: ThreadsafeFunction<Call, ErrorStrategy::Fatal>,
    next_id: AtomicU32,
}

/// Make `callback` callable from any thread without keeping Node alive.
///
/// An unreferenced function does not hold the event loop open, so an idle
/// client never stops the process from exiting. The handle is released once,
/// when the last clone of the returned `Fetch` is dropped.
pub(crate) fn fetch_from(env: &Env, callback: &JsFunction) -> Result<Arc<Fetch>> {
    let mut function: ThreadsafeFunction<Call, ErrorStrategy::Fatal> = callback
        .create_threadsafe_function(0, |ctx| {
            let (id, abort) = ctx.value;
            Ok(vec![
                ctx.env.create_uint32(id)?.into_unknown(),
                ctx.env.get_boolean(abort)?.into_unknown(),
            ] as Vec<JsUnknown>)
        })?;
    function.unref(env)?;
    Ok(Arc::new(Fetch {
        function,
        next_id: AtomicU32::new(1),
    }))
}

/// Aborts the JS signal when the fetch's cancellation token fires, until the
/// fetch settles. Dropping it stops the watch.
struct AbortWatch(JoinHandle<()>);

impl Drop for AbortWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The token source a client authenticates with: `initial` first, if the
/// caller has one, then whatever the app's function returns.
pub(crate) fn provider(fetch: &Arc<Fetch>, initial: Option<String>) -> Arc<RefreshingToken> {
    let fetch = Arc::clone(fetch);
    let call = move |cancel: CancellationToken| {
        let fetch = Arc::clone(&fetch);
        async move {
            let id = fetch.next_id.fetch_add(1, Ordering::Relaxed);
            // The fetch call is queued before the watch can queue an abort.
            let reply = fetch.function.call_async::<Promise<String>>((id, false));
            let watcher = Arc::clone(&fetch);
            let _watch = AbortWatch(tokio::spawn(async move {
                cancel.cancelled().await;
                watcher
                    .function
                    .call((id, true), ThreadsafeFunctionCallMode::NonBlocking);
            }));
            // Failures carry the app's own message: the client wraps it in its
            // auth error and the caller reads it back unchanged.
            let reply = reply.await.map_err(|err| anyhow!("{}", err.reason))?;
            reply.await.map_err(|err| anyhow!("{}", err.reason))
        }
    };
    // An empty string is no token: the first auth fetches instead of sending
    // one the broker will refuse.
    Arc::new(match initial.filter(|token| !token.is_empty()) {
        Some(initial) => RefreshingToken::with_initial_cancellable(initial, call),
        None => RefreshingToken::new_cancellable(call),
    })
}
