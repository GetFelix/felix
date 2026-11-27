//! `AsyncClient`, the asyncio counterpart of the synchronous `Client`.

mod admin;
mod cache;
mod commit;
mod publish;
mod queues;
mod stream;

use std::sync::Arc;

use felix_client::ClientConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::args::parse_addrs;
use crate::errors::to_py_err;
use crate::token::{Flavor, Tokens, resolve};

/// A connection to a Felix cluster, for asyncio.
///
/// Construct with `await AsyncClient.connect(...)` rather than `__init__`:
/// connecting reaches the network, and a constructor that blocked an event
/// loop to do it would be the wrong shape.
#[pyclass(module = "felix", name = "AsyncClient")]
pub struct AsyncClient {
    inner: Arc<felix_client::Client>,
}

#[pymethods]
impl AsyncClient {
    /// Connect to a cluster. Arguments are those of `Client(...)`, except that
    /// `token_provider` must be an `async def` function, or any callable
    /// returning an awaitable; one that returns a `str` fails with `AuthError`
    /// when first called. It is awaited on the event loop running this call,
    /// which must stay open for the life of the client and every
    /// `with_identity` handle; once it is closed the provider fails with "the
    /// event loop the token provider was bound to is closed".
    ///
    /// `reconnect_attempts` and `reconnect_deadline` are as for the
    /// synchronous `Client`.
    #[staticmethod]
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
    fn connect<'py>(
        py: Python<'py>,
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
    ) -> PyResult<Bound<'py, PyAny>> {
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
        let quinn =
            crate::tls::client_config(ca_file, client_cert_file, client_key_file, offer_alpn)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        crate::tuning::apply_options(
            &mut config,
            publishers,
            timestamps,
            record_headers,
            tuning.as_deref(),
        );
        let flavor = Flavor::asyncio(py, token_provider.is_some())?;
        match resolve(py, token, token_provider, flavor)? {
            Tokens::Fixed(token) => config.auth_token = Some(token),
            Tokens::Provider(provider) => config.token_provider = Some(provider),
        }
        let server_name = server_name.to_string();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = felix_client::Client::connect_seeds(&seeds, &server_name, config, policy)
                .await
                .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(inner),
            })
        })
    }

    /// Resolve to a client that acts as another user over this client's
    /// connections. See the synchronous `Client.with_identity`.
    #[pyo3(signature = (tenant_id, token=None, *, token_provider=None))]
    fn with_identity<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        token: Option<&str>,
        token_provider: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let flavor = Flavor::asyncio(py, token_provider.is_some())?;
        let tokens = resolve(py, token, token_provider, flavor)?;
        let inner = Arc::clone(&self.inner);
        let tenant_id = tenant_id.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let identity = match tokens {
                Tokens::Fixed(token) => inner.with_identity_token(tenant_id, token).await,
                Tokens::Provider(provider) => inner.with_identity(tenant_id, provider).await,
            }
            .map_err(to_py_err)?;
            Ok(AsyncClient {
                inner: Arc::new(identity),
            })
        })
    }

    fn endpoints<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner
                .endpoints()
                .await
                .iter()
                .map(std::net::SocketAddr::to_string)
                .collect::<Vec<_>>())
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(handle) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(false) })
    }
}
