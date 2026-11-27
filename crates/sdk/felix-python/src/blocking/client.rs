//! The synchronous `Client`.

mod admin;
mod cache;
mod commit;
mod publish;
mod queues;
mod stream;

use std::net::SocketAddr;
use std::sync::Arc;

use felix_client::ClientConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::args::parse_addrs;
use crate::errors::to_py_err;
use crate::runtime::block_on;
use crate::tls;
use crate::token::{Flavor, Tokens, resolve};

/// A connection to a Felix cluster.
///
/// Holds the seed addresses it was given plus every broker the cluster has
/// since advertised, and moves itself to a healthy broker when the one in use
/// goes away — the reconnect and redirect logic is the Rust client's, not a
/// reimplementation.
#[pyclass(module = "felix")]
pub struct Client {
    inner: Arc<felix_client::Client>,
}

#[pymethods]
impl Client {
    /// Connect to a cluster.
    ///
    /// `addrs` is one `"host:port"` or a list of them; any reachable one is
    /// enough, and the client discovers the rest. A name is resolved again
    /// whenever no broker the client knows answers, so prefer one to an IP. A
    /// name that does not resolve now raises `ValueError`.
    /// Pass `token`, a fixed token, or `token_provider`, a callable returning a
    /// fresh one; `token` is then optional, and without it the first stream to
    /// authenticate calls the provider. A plain callable runs on a Felix worker
    /// thread with the GIL; an `async def` is run there with `asyncio.run`, so
    /// it must not use objects bound to another event loop. The provider is
    /// called lazily (never on a timer), one call at a time, when the held
    /// token is missing, past two thirds of its life, or refused by the
    /// broker. If it raises or returns a non-`str`, the client keeps a token
    /// that is still valid and otherwise raises `AuthError` with the
    /// provider's message. A provider that has not answered after 30 seconds
    /// fails the operations waiting on it; it is not called again until it
    /// returns.
    ///
    /// `server_name` is the name the broker's certificate is expected to
    /// carry.
    ///
    /// TLS is not optional — QUIC has no unencrypted mode. Supply `ca_file`
    /// to trust a specific CA (what the demos and a self-signed development
    /// broker need); omit it to use the operating system's trust store.
    ///
    /// `client_cert_file` and `client_key_file` are PEM files holding a client
    /// certificate (leaf first, then any intermediates) and its private key.
    /// Pass both to connect to a broker started with `FELIX_TLS_CLIENT_CA`,
    /// which refuses a client without one. Either alone, an unreadable or
    /// non-PEM file, or a key that does not match raises `ValueError`.
    ///
    /// `offer_alpn=True` offers the `felix/1` ALPN. A broker with
    /// `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker
    /// older than ALPN support refuses them, which is why it is off by default.
    ///
    /// `reconnect_attempts` (default 5, at least 1) is how many times a call
    /// tries to reach a broker before it gives up. `reconnect_deadline`
    /// (seconds, default none) bounds one call across every attempt and wait.
    /// A deadline shorter than one attempt's own timeout leaves the call a
    /// single attempt. Both apply to `with_identity` handles too. A bad value
    /// raises `ValueError`.
    #[new]
    #[pyo3(signature = (
        addrs,
        *,
        tenant_id,
        token=None,
        token_provider=None,
        server_name="localhost",
        ca_file=None,
        client_cert_file=None,
        client_key_file=None,
        offer_alpn=false,
        reconnect_attempts=None,
        reconnect_deadline=None,
        reconnect_backoff=None,
        reconnect_max_backoff=None,
        publishers=None,
        timestamps=None,
        record_headers=None,
        tuning=None,
    ))]
    fn new(
        py: Python<'_>,
        addrs: Py<PyAny>,
        tenant_id: &str,
        token: Option<&str>,
        token_provider: Option<Py<PyAny>>,
        server_name: &str,
        ca_file: Option<&str>,
        client_cert_file: Option<&str>,
        client_key_file: Option<&str>,
        offer_alpn: bool,
        reconnect_attempts: Option<Bound<'_, PyAny>>,
        reconnect_deadline: Option<f64>,
        reconnect_backoff: Option<f64>,
        reconnect_max_backoff: Option<f64>,
        publishers: Option<bool>,
        timestamps: Option<bool>,
        record_headers: Option<bool>,
        tuning: Option<PyRef<'_, crate::tuning::ClientTuning>>,
    ) -> PyResult<Self> {
        let seeds = parse_addrs(py, &addrs)?;
        if seeds.is_empty() {
            return Err(PyValueError::new_err(
                "at least one broker address is required",
            ));
        }
        let policy = crate::args::reconnect_policy(
            reconnect_attempts.as_ref(),
            reconnect_deadline,
            reconnect_backoff,
            reconnect_max_backoff,
        )?;
        let quinn = tls::client_config(ca_file, client_cert_file, client_key_file, offer_alpn)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        crate::tuning::apply_options(
            &mut config,
            publishers,
            timestamps,
            record_headers,
            tuning.as_deref(),
        );
        match resolve(py, token, token_provider, Flavor::Blocking)? {
            Tokens::Fixed(token) => config.auth_token = Some(token),
            Tokens::Provider(provider) => config.token_provider = Some(provider),
        }

        let server_name = server_name.to_string();
        let inner = block_on(py, async move {
            felix_client::Client::connect_seeds(&seeds, &server_name, config, policy)
                .await
                .map_err(to_py_err)
        })?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// A client that acts as another user over this client's connections.
    ///
    /// Every stream it opens authenticates with `tenant_id` and `token`, and
    /// the broker checks its requests against that token's grants alone. It
    /// follows shards and failovers as this client does, reaching each broker
    /// over this client's connection to it, so a gateway serving many users
    /// holds one connection per broker. Pass `token`, fixed for the identity's
    /// life, or `token_provider` as for `Client(...)`. A refused token fails
    /// here.
    #[pyo3(signature = (tenant_id, token=None, *, token_provider=None))]
    fn with_identity(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        token: Option<&str>,
        token_provider: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let tokens = resolve(py, token, token_provider, Flavor::Blocking)?;
        let inner = Arc::clone(&self.inner);
        let tenant_id = tenant_id.to_string();
        let identity = block_on(py, async move {
            match tokens {
                Tokens::Fixed(token) => inner.with_identity_token(tenant_id, token).await,
                Tokens::Provider(provider) => inner.with_identity(tenant_id, provider).await,
            }
            .map_err(to_py_err)
        })?;
        Ok(Self {
            inner: Arc::new(identity),
        })
    }

    /// Every broker this client would try, seeds included.
    fn endpoints(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let inner = Arc::clone(&self.inner);
        let addrs = block_on(py, async move { Ok(inner.endpoints().await) })?;
        Ok(addrs.iter().map(SocketAddr::to_string).collect())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        "Client(...)".to_string()
    }
}
