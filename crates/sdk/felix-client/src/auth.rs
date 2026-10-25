//! Token sources for stream authentication.
//!
//! Each stream authenticates when it opens, and a client keeps opening streams
//! after connect (subscriptions, watches, group requests, reconnects). Access
//! tokens last 900s by default, so the client asks a [`TokenProvider`] for the
//! current token each time instead of holding the one it connected with.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;

/// Delay before retrying a failed fetch, so every stream open doesn't trigger
/// one.
const RETRY_AFTER: Duration = Duration::from_secs(5);

/// How long one fetch may take before it counts as failed, so a provider that
/// never answers cannot hold every stream open behind it.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// The future a [`TokenProvider`] answers with.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

/// Supplies the token a stream authenticates with.
///
/// Called on every stream open, so implementations should cache.
/// [`RefreshingToken`] is the usual one.
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

type Fetch = Box<dyn Fn() -> TokenFuture<'static> + Send + Sync>;

/// A [`TokenProvider`] that replaces its token before it expires.
///
/// `fetch` gets a new token, usually by calling the control plane's
/// `/token/refresh`. It runs on first use and again once two thirds of the
/// current token's life has passed, going by its `exp` claim. A token without
/// a readable `exp` is kept until the broker refuses it.
///
/// If a fetch fails while the current token is still valid, the current token
/// is returned and the fetch is tried again a few seconds later. A fetch that
/// takes longer than [`FETCH_TIMEOUT`] counts as failed. Concurrent callers
/// wait on the same fetch.
pub struct RefreshingToken {
    fetch: Fetch,
    state: Mutex<Option<Held>>,
    fetching: tokio::sync::Mutex<()>,
}

impl RefreshingToken {
    /// Fetch tokens with `fetch`, starting with nothing held.
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self {
            fetch: Box::new(move || Box::pin(fetch())),
            state: Mutex::new(None),
            fetching: tokio::sync::Mutex::new(()),
        }
    }

    /// Start from `initial`, a token the caller already has, and fetch its
    /// replacements with `fetch`.
    pub fn with_initial<F, Fut>(initial: impl Into<String>, fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        let token = Self::new(fetch);
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

    async fn current(&self) -> Result<String> {
        if let Some(token) = self.fresh(now_secs()) {
            return Ok(token);
        }
        let _fetching = self.fetching.lock().await;
        // Another caller may have fetched while this one waited.
        if let Some(token) = self.fresh(now_secs()) {
            return Ok(token);
        }
        let fetched = match tokio::time::timeout(FETCH_TIMEOUT, (self.fetch)()).await {
            Ok(fetched) => fetched,
            Err(_) => Err(anyhow::anyhow!(
                "the token fetch took longer than {}s",
                FETCH_TIMEOUT.as_secs()
            )),
        };
        match fetched {
            Ok(token) => {
                *self.state.lock().expect("token state poisoned") = Some(Held::new(token.clone()));
                Ok(token)
            }
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

impl TokenProvider for RefreshingToken {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(self.current())
    }

    fn invalidate(&self, rejected: &str) {
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
