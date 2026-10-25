//! The token provider: the app's fetch function behind the Rust client's
//! `RefreshingToken`, which keeps the timing (refresh at two thirds of a
//! token's life, keep the old token if a fetch fails, retry a rejected one).

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use anyhow::{Result, anyhow, bail};
use felix_client::RefreshingToken;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, optional_str, required_str};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// Asks the app for a token. Called on a library thread each time the client
/// needs one; the app must not block in it. It completes `reply` exactly once,
/// from any thread, with `felix_token_reply_ok` or `felix_token_reply_err`.
///
/// `reply` is valid until the first `felix_token_reply_ok` or
/// `felix_token_reply_err` call on it that is not refused for a bad argument,
/// or until `token_release` runs, whichever is first.
pub type FelixTokenFetch =
    Option<unsafe extern "C" fn(user_data: *mut c_void, reply: *mut FelixTokenReply)>;

/// Called once when the library is done with `token_user_data`: after the
/// client is freed and no fetch can run or be in flight.
pub type FelixTokenRelease = Option<unsafe extern "C" fn(user_data: *mut c_void)>;

/// One request for a token, answered once.
pub struct FelixTokenReply {
    answer: Mutex<Option<oneshot::Sender<Result<String, String>>>>,
    /// Where this reply is held until it is answered.
    held: Weak<Held>,
}

/// The replies handed out and not yet answered, by address.
#[derive(Default)]
struct Held(Mutex<HashMap<usize, Arc<FelixTokenReply>>>);

impl FelixTokenReply {
    /// Consumes the reply: it is freed once answered, so the app must not
    /// touch it again.
    fn complete(&self, answer: Result<String, String>) -> Result<FelixStatus, Failure> {
        let sender = lock(&self.answer)
            .take()
            .ok_or_else(|| Failure::invalid("the token reply was already completed"))?;
        // The receiver is gone when the fetch was abandoned (the client was
        // freed); that is not the app's mistake.
        let _ = sender.send(answer);
        // Last, because this may free `self`.
        let key = std::ptr::from_ref(self) as usize;
        let freed = self
            .held
            .upgrade()
            .and_then(|held| lock(&held.0).remove(&key));
        drop(freed);
        Ok(FELIX_STATUS_OK)
    }
}

/// Complete a fetch with a token. The string is copied.
///
/// Returns `FELIX_STATUS_INVALID_ARGUMENT` if `token` is null, empty or not
/// UTF-8 (the reply stays open and can still be completed). Otherwise it
/// returns `FELIX_STATUS_OK` and consumes the reply, including when the
/// library had stopped waiting on the fetch; a late token is still used, and
/// the fetch is not called again while a reply is outstanding.
///
/// # Safety
///
/// `reply` is a pointer passed to a `FelixTokenFetch` that no call has
/// consumed yet and whose `token_release` has not run. Using it again after
/// a call that consumed it is undefined behaviour. `token` is null or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_token_reply_ok(
    reply: *const FelixTokenReply,
    token: *const c_char,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live reply, per the contract above.
        let reply = unsafe { reply.as_ref() }.ok_or_else(|| Failure::invalid("reply is null"))?;
        // SAFETY: null or NUL-terminated, per the contract above.
        let token = unsafe { required_str(token, "token") }?;
        if token.is_empty() {
            return Err(Failure::invalid("token is empty"));
        }
        reply.complete(Ok(token.to_string()))
    })
}

/// Complete a fetch with a failure. The client keeps its current token while
/// that is still valid, and asks again a few seconds later. `message` may be
/// null.
///
/// Consumes the reply and returns `FELIX_STATUS_OK`, as for
/// `felix_token_reply_ok`.
///
/// # Safety
///
/// As for `felix_token_reply_ok`, with `message` null or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_token_reply_err(
    reply: *const FelixTokenReply,
    message: *const c_char,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live reply, per the contract above.
        let reply = unsafe { reply.as_ref() }.ok_or_else(|| Failure::invalid("reply is null"))?;
        // SAFETY: null or NUL-terminated, per the contract above.
        let message = unsafe { optional_str(message, "message") }?
            .filter(|message| !message.is_empty())
            .unwrap_or("the token fetch failed");
        reply.complete(Err(message.to_string()))
    })
}

/// The app's fetch function and what it needs, shared by every fetch.
pub(crate) struct TokenSource {
    fetch: unsafe extern "C" fn(*mut c_void, *mut FelixTokenReply),
    user_data: *mut c_void,
    release: FelixTokenRelease,
    /// Cancelled when the client is freed, which fails the fetches waiting on
    /// the app.
    closed: CancellationToken,
    /// Replies the app has not answered yet; answering one frees it.
    replies: Arc<Held>,
}

// SAFETY: the library never dereferences `user_data`. The header tells the
// app that the fetch and release functions run on any thread with it.
unsafe impl Send for TokenSource {}
// SAFETY: as above; the rest is behind locks.
unsafe impl Sync for TokenSource {}

impl TokenSource {
    pub(crate) fn new(
        fetch: unsafe extern "C" fn(*mut c_void, *mut FelixTokenReply),
        user_data: *mut c_void,
        release: FelixTokenRelease,
    ) -> Self {
        Self {
            fetch,
            user_data,
            release,
            closed: CancellationToken::new(),
            replies: Arc::default(),
        }
    }

    /// Replies handed to the app and not yet answered.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        lock(&self.replies.0).len()
    }

    /// Fail every fetch waiting on the app, and any later one.
    pub(crate) fn close(&self) {
        self.closed.cancel();
    }

    /// Hand the app a new reply and call its fetch function.
    fn start(&self) -> oneshot::Receiver<Result<String, String>> {
        let (sender, receiver) = oneshot::channel();
        let reply = Arc::new(FelixTokenReply {
            answer: Mutex::new(Some(sender)),
            held: Arc::downgrade(&self.replies),
        });
        let pointer = Arc::as_ptr(&reply).cast_mut();
        lock(&self.replies.0).insert(pointer as usize, Arc::clone(&reply));
        // SAFETY: the app's function, with the pointer it is documented to
        // take; `reply` keeps it alive through the call even if the app
        // answers inside it.
        unsafe { (self.fetch)(self.user_data, pointer) };
        drop(reply);
        receiver
    }

    async fn fetch(self: Arc<Self>) -> Result<String> {
        if self.closed.is_cancelled() {
            bail!("the client was freed");
        }
        let receiver = self.start();
        tokio::select! {
            biased;
            answer = receiver => match answer {
                Ok(Ok(token)) => Ok(token),
                Ok(Err(message)) => Err(anyhow!(message)),
                Err(_) => bail!("the token reply was dropped without an answer"),
            },
            () = self.closed.cancelled() => {
                bail!("the client was freed before the app answered the token fetch")
            }
        }
    }
}

impl Drop for TokenSource {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            // SAFETY: the app's function. Nothing holds the source, so no
            // fetch can run or be waiting on the app.
            unsafe { release(self.user_data) };
        }
    }
}

/// The token source a client authenticates with: `initial` first, if the
/// caller has one, then whatever the app's fetch returns.
pub(crate) fn provider(source: &Arc<TokenSource>, initial: Option<&str>) -> Arc<RefreshingToken> {
    let source = Arc::clone(source);
    let fetch = move || Arc::clone(&source).fetch();
    // An empty string is no token: the first auth fetches instead of sending
    // one the broker will refuse.
    let initial = initial.filter(|initial| !initial.is_empty());
    Arc::new(match initial {
        Some(initial) => RefreshingToken::with_initial(initial, fetch),
        None => RefreshingToken::new(fetch),
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
pub(crate) mod tests;
