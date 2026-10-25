//! Token sources for stream authentication.
//!
//! Each stream authenticates when it opens, and a client keeps opening streams
//! after connect (subscriptions, watches, group requests, reconnects). Access
//! tokens last 900s by default, so the client asks a [`TokenProvider`] for the
//! current token each time instead of holding the one it connected with.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Delay before retrying a failed fetch, so every stream open doesn't trigger
/// one.
const RETRY_AFTER: Duration = Duration::from_secs(5);

/// How long a caller waits on one fetch before it counts as failed, so a
/// provider that never answers cannot hold every stream open behind it. The
/// fetch itself keeps running and the next refresh waits on it again; see
/// [`RefreshingToken`].
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// The future a [`TokenProvider`] answers with.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

/// Supplies the token a stream authenticates with.
///
/// Called on every stream open, so implementations should cache.
/// [`RefreshingToken`] is the usual one.
///
/// A connect asks for the token before its handshake and gives up on a
/// `token()` that has not answered after [`FETCH_TIMEOUT`], so a provider
/// that hangs fails the connect instead of holding it.
pub trait TokenProvider: Send + Sync {
    /// The token to present now.
    fn token(&self) -> TokenFuture<'_>;

    /// The broker refused `rejected`. The client then asks for a token again
    /// and retries once if it gets a different one, so a caching provider
    /// should drop `rejected` here.
    ///
    /// The token is passed in because several streams can be refused at once,
    /// and a late call shouldn't throw away a token fetched since.
    fn invalidate(&self, rejected: &str) {
        let _ = rejected;
    }
}

/// The fixed [`crate::ClientConfig::auth_token`].
pub(crate) struct StaticToken(pub(crate) String);

impl TokenProvider for StaticToken {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.clone();
        Box::pin(async move { Ok(token) })
    }
}

type Fetch = Box<dyn Fn(CancellationToken) -> TokenFuture<'static> + Send + Sync>;

/// One provider call that has not finished. Later refreshes join it instead of
/// calling the provider again.
#[derive(Clone)]
struct Flight {
    answer: watch::Receiver<Option<Result<String, String>>>,
    cancel: CancellationToken,
    /// Tells this flight's guard apart from a later flight in the slot.
    id: Arc<()>,
}

type State = Arc<Mutex<Option<Held>>>;
type Flights = Arc<Mutex<Option<Flight>>>;

/// A [`TokenProvider`] that replaces its token before it expires.
///
/// `fetch` gets a new token, usually by calling the control plane's
/// `/token/refresh`. It runs on first use and again once two thirds of the
/// current token's life has passed, going by its `exp` claim. A token without
/// a readable `exp` is kept until the broker refuses it.
///
/// The provider is never called while an earlier call is still running, so
/// rotating a refresh token inside it is safe. Concurrent callers wait on the
/// same call.
///
/// A call that takes longer than [`FETCH_TIMEOUT`] fails the callers waiting
/// on it, not the call: it keeps running, the next refresh joins it (waiting
/// up to [`FETCH_TIMEOUT`] again), and an answer that arrives late is used as
/// the held token. The cancellation token given to
/// [`RefreshingToken::new_cancellable`] fires on that timeout and when the
/// `RefreshingToken` is dropped, for providers that can stop early. The call
/// runs on a spawned task, so it needs a Tokio runtime.
///
/// If a call fails while the current token is still valid, the current token
/// is returned and the call is tried again a few seconds later.
pub struct RefreshingToken {
    fetch: Fetch,
    state: State,
    flight: Flights,
    /// Parent of every call's cancellation token; fires on drop.
    closed: CancellationToken,
}

impl RefreshingToken {
    /// Fetch tokens with `fetch`, starting with nothing held.
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self::new_cancellable(move |_| fetch())
    }

    /// Like [`new`](Self::new), for a `fetch` that can stop early. The token
    /// it receives is cancelled when the call outlives [`FETCH_TIMEOUT`] and
    /// when this `RefreshingToken` is dropped. Cancelling does not end the
    /// call; the future still decides what to return.
    pub fn new_cancellable<F, Fut>(fetch: F) -> Self
    where
        F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self {
            fetch: Box::new(move |cancel| Box::pin(fetch(cancel))),
            state: Arc::new(Mutex::new(None)),
            flight: Arc::new(Mutex::new(None)),
            closed: CancellationToken::new(),
        }
    }

    /// Start from `initial`, a token the caller already has, and fetch its
    /// replacements with `fetch`.
    pub fn with_initial<F, Fut>(initial: impl Into<String>, fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self::with_initial_cancellable(initial, move |_| fetch())
    }

    /// [`with_initial`](Self::with_initial) with a cancellable `fetch`, as for
    /// [`new_cancellable`](Self::new_cancellable).
    pub fn with_initial_cancellable<F, Fut>(initial: impl Into<String>, fetch: F) -> Self
    where
        F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        let token = Self::new_cancellable(fetch);
        *token.state.lock().expect("token state poisoned") = Some(Held::new(initial.into()));
        token
    }

    /// The held token, if it is not yet due for replacement.
    fn fresh(&self, now: i64) -> Option<String> {
        let state = self.state.lock().expect("token state poisoned");
        let held = state.as_ref()?;
        match held.refresh_at {
            Some(at) if now >= at => None,
            _ => Some(held.token.clone()),
        }
    }

    /// Join the call in flight, or start one. `Err` carries a token that
    /// turned out fresh.
    fn join(&self) -> Result<Flight, String> {
        // Checked before the flight exists: with no runtime there is nothing to
        // run the call and nothing to clear afterwards.
        let runtime = tokio::runtime::Handle::try_current()
            .expect("RefreshingToken needs a Tokio runtime to run its token fetch");
        let (sender, flight, guard) = {
            let mut slot = lock_flights(&self.flight);
            if let Some(flight) = slot.as_ref() {
                return Ok(flight.clone());
            }
            // The task stores its token and clears the slot in one step, so
            // with none in flight the state is current.
            if let Some(token) = self.fresh(now_secs()) {
                return Err(token);
            }
            let (sender, answer) = watch::channel(None);
            let flight = Flight {
                answer,
                cancel: self.closed.child_token(),
                id: Arc::new(()),
            };
            *slot = Some(flight.clone());
            let guard = FlightGuard {
                slot: Arc::clone(&self.flight),
                id: Arc::clone(&flight.id),
            };
            (sender, flight, guard)
        };
        // Past this point the lock is free: if the call below panics or the
        // runtime drops the task unrun, the guard clears the slot, and it
        // must be able to take the lock.
        let call = (self.fetch)(flight.cancel.clone());
        let state = Arc::clone(&self.state);
        runtime.spawn(async move {
            let result = call.await.map_err(|err| format!("{err:#}"));
            guard.finish(&state, &sender, result);
        });
        Ok(flight)
    }

    async fn current(&self) -> Result<String> {
        loop {
            if let Some(token) = self.fresh(now_secs()) {
                return Ok(token);
            }
            let flight = match self.join() {
                Ok(flight) => flight,
                Err(token) => return Ok(token),
            };
            match self.wait(flight).await {
                // Nothing held means the answer was refused while we waited:
                // fetch again, don't hand it out.
                Ok(_) => match self.held() {
                    Some(held) => return Ok(held),
                    None => continue,
                },
                err => return err,
            }
        }
    }

    /// The held token, whatever its age. A newer one than the answer just
    /// read means a later fetch stored it, and it is not a refused token.
    fn held(&self) -> Option<String> {
        let state = self.state.lock().expect("token state poisoned");
        state.as_ref().map(|held| held.token.clone())
    }

    async fn wait(&self, mut flight: Flight) -> Result<String> {
        let waited = tokio::time::timeout(
            FETCH_TIMEOUT,
            flight.answer.wait_for(|answer| answer.is_some()),
        )
        .await;
        let fetched: Result<String> = match waited {
            Ok(Ok(answer)) => answer
                .clone()
                .expect("waited for an answer")
                .map_err(anyhow::Error::msg),
            Ok(Err(_)) => Err(anyhow::anyhow!("the token fetch ended without an answer")),
            Err(_) => {
                flight.cancel.cancel();
                Err(anyhow::anyhow!(
                    "the token fetch took longer than {}s",
                    FETCH_TIMEOUT.as_secs()
                ))
            }
        };
        match fetched {
            Ok(token) => Ok(token),
            Err(err) => {
                let now = now_secs();
                let mut state = self.state.lock().expect("token state poisoned");
                match state.as_mut() {
                    Some(held) if held.expires_at.is_some_and(|exp| now < exp) => {
                        tracing::warn!(
                            error = %err,
                            remaining_secs = held.expires_at.map(|exp| exp - now),
                            "could not refresh the client token; using the current one",
                        );
                        held.refresh_at = Some(now + RETRY_AFTER.as_secs() as i64);
                        Ok(held.token.clone())
                    }
                    _ => Err(err.context("fetch client token")),
                }
            }
        }
    }
}

impl Drop for RefreshingToken {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}

/// Clears the flight when its task ends, including by panic or by being
/// dropped unrun, so a later refresh is not stuck joining a call that is gone.
struct FlightGuard {
    slot: Flights,
    id: Arc<()>,
}

impl FlightGuard {
    /// Store the token, publish the answer and clear the flight under one
    /// lock, so nobody can join a finished flight and be handed a token that
    /// `invalidate` has since dropped.
    fn finish(
        self,
        state: &State,
        sender: &watch::Sender<Option<Result<String, String>>>,
        result: Result<String, String>,
    ) {
        let mut slot = lock_flights(&self.slot);
        if let Ok(token) = &result {
            *state.lock().expect("token state poisoned") = Some(Held::new(token.clone()));
        }
        sender.send_replace(Some(result));
        self.clear(&mut slot);
    }

    fn clear(&self, slot: &mut Option<Flight>) {
        if slot.as_ref().is_some_and(|f| Arc::ptr_eq(&f.id, &self.id)) {
            *slot = None;
        }
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let slot = Arc::clone(&self.slot);
        self.clear(&mut lock_flights(&slot));
    }
}

fn lock_flights(flights: &Flights) -> std::sync::MutexGuard<'_, Option<Flight>> {
    flights
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl TokenProvider for RefreshingToken {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self.current())
    }

    fn invalidate(&self, rejected: &str) {
        // Same order as the task's finish, so a refusal lands either before
        // the answer is stored or after the flight is gone.
        let _flight = lock_flights(&self.flight);
        let mut state = self.state.lock().expect("token state poisoned");
        if state.as_ref().is_some_and(|held| held.token == rejected) {
            *state = None;
        }
    }
}

struct Held {
    token: String,
    /// Unix seconds. `None` means the token did not say when it expires.
    expires_at: Option<i64>,
    /// When to stop answering from this token and fetch again.
    refresh_at: Option<i64>,
}

impl Held {
    fn new(token: String) -> Self {
        let expires_at = read_exp(&token);
        let refresh_at = expires_at.map(|exp| refresh_at(now_secs(), exp));
        Self {
            token,
            expires_at,
            refresh_at,
        }
    }
}

/// Two thirds of the way to `exp`, leaving the last third for retries.
fn refresh_at(now: i64, exp: i64) -> i64 {
    now + exp.saturating_sub(now).max(0) * 2 / 3
}

/// A JWT's `exp` claim, unverified. It only decides when to refresh; the
/// broker does the real check.
fn read_exp(token: &str) -> Option<i64> {
    use base64::Engine;

    #[derive(serde::Deserialize)]
    struct Claims {
        exp: i64,
    }

    let mut parts = token.split('.');
    let (_header, payload, _signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice::<Claims>(&decoded)
        .ok()
        .map(|claims| claims.exp)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
