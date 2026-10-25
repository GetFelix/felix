//! The `token_provider` argument: a Python callable behind the Rust client's
//! `RefreshingToken`, which owns the timing (refresh at two thirds of a token's
//! life), single-flight, keep-the-old-token-on-failure and the retry of a
//! refused token. Nothing here reimplements those.
//!
//! The GIL is never held across a wait on the broker. A blocking `Client` runs
//! the callable on a Tokio blocking-pool thread, which takes the GIL only for
//! the call. An `AsyncClient` hands the coroutine to the event loop that was
//! running at connect / `with_identity` (captured once, so it must outlive the
//! client and its handles) and awaits the result without the GIL.
//!
//! The Rust client cancels a fetch when it outlives its timeout and when the
//! client is dropped. An `AsyncClient` provider is cancelled then, as an
//! asyncio task on its loop. A blocking `Client` provider is a thread running
//! Python code and cannot be stopped, so it is joined instead.

use std::ffi::CStr;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use felix_client::RefreshingToken;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3_async_runtimes::{TaskLocals, into_future_with_locals};
use tokio_util::sync::CancellationToken;

/// Where streams get their token from.
pub(crate) enum Tokens {
    Fixed(String),
    Provider(Arc<RefreshingToken>),
}

/// Which client is asking, and so how the provider is called.
pub(crate) enum Flavor {
    /// A plain callable, or an `async def` run with `asyncio.run`.
    Blocking,
    /// Any callable returning an awaitable, awaited on this loop.
    Asyncio(TaskLocals),
}

impl Flavor {
    /// The flavor for an `AsyncClient` call, which runs on the loop the
    /// provider will be awaited on. Without a provider no loop is needed.
    pub(crate) fn asyncio(py: Python<'_>, has_provider: bool) -> PyResult<Self> {
        if !has_provider {
            return Ok(Self::Blocking);
        }
        Ok(Self::Asyncio(
            TaskLocals::with_running_loop(py)?.copy_context(py)?,
        ))
    }
}

/// Validate `token` and `token_provider` and build the token source.
///
/// An empty `token` counts as none when a provider is set, so the first stream
/// to authenticate calls the provider.
pub(crate) fn resolve(
    py: Python<'_>,
    token: Option<&str>,
    provider: Option<Py<PyAny>>,
    flavor: Flavor,
) -> PyResult<Tokens> {
    let Some(provider) = provider else {
        return match token {
            Some(token) => Ok(Tokens::Fixed(token.to_string())),
            None => Err(PyTypeError::new_err(
                "pass token, or token_provider to fetch one",
            )),
        };
    };
    let callable = provider.bind(py);
    if !callable.is_callable() {
        return Err(PyTypeError::new_err("token_provider must be callable"));
    }

    let provider = Arc::new(provider);
    let initial = token.filter(|token| !token.is_empty()).map(str::to_string);
    let locals = match flavor {
        Flavor::Blocking => None,
        Flavor::Asyncio(locals) => Some(locals),
    };
    let fetch = move |cancel: CancellationToken| {
        let provider = Arc::clone(&provider);
        let locals = locals.clone();
        async move {
            match locals {
                None => fetch_blocking(provider).await,
                Some(locals) => fetch_on_loop(provider, locals, cancel).await,
            }
        }
    };
    let source = match initial {
        Some(initial) => RefreshingToken::with_initial_cancellable(initial, fetch),
        None => RefreshingToken::new_cancellable(fetch),
    };
    Ok(Tokens::Provider(Arc::new(source)))
}

/// Run the callable on a blocking-pool thread, holding the GIL only while it
/// runs.
async fn fetch_blocking(provider: Arc<Py<PyAny>>) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        Python::attach(|py| -> Result<String> {
            let value = provider.call0(py).map_err(|err| failure(py, &err))?;
            let value = value.into_bound(py);
            // An `async def` (or any callable returning a coroutine) gets its
            // own loop on this worker thread.
            let is_coroutine = py
                .import("inspect")
                .and_then(|inspect| inspect.call_method1("iscoroutine", (&value,)))
                .and_then(|answer| answer.is_truthy())
                .map_err(|err| failure(py, &err))?;
            let value = if is_coroutine {
                py.import("asyncio")
                    .and_then(|asyncio| asyncio.call_method1("run", (value,)))
                    .map_err(|err| failure(py, &err))?
            } else {
                value
            };
            as_token(&value)
        })
    })
    .await
    .map_err(|err| anyhow!("the token provider thread failed: {err}"))?
}

/// Call the provider, then await its result on the client's loop with the GIL
/// released while waiting.
///
/// Any callable is accepted, because a lambda or decorator around an `async
/// def` cannot be told apart from a plain function without calling it. The
/// result decides: an awaitable is awaited, anything else is an error.
///
/// When `cancel` fires the provider's task is cancelled on its loop. The
/// `CancelledError` that comes back is an ordinary fetch failure.
async fn fetch_on_loop(
    provider: Arc<Py<PyAny>>,
    locals: TaskLocals,
    cancel: CancellationToken,
) -> Result<String> {
    let (call, pending) = Python::attach(|py| -> Result<_> {
        ensure_loop_open(py, &locals)?;
        let result = provider.call0(py).map_err(|err| failure(py, &err))?;
        let result = result.into_bound(py);
        let awaitable = py
            .import("inspect")
            .and_then(|inspect| inspect.call_method1("isawaitable", (&result,)))
            .and_then(|answer| answer.is_truthy())
            .map_err(|err| failure(py, &err))?;
        if !awaitable {
            return Err(anyhow!(
                "AsyncClient token_provider must be an `async def` function (or return an \
                 awaitable), not return {}; use Client for a plain callable",
                type_name(&result)
            ));
        }
        let call = provider_call(py).map_err(|err| failure(py, &err))?;
        let wrapped = call
            .call_method1("run", (result,))
            .map_err(|err| failure(py, &err))?;
        let pending = into_future_with_locals(&locals, wrapped)
            .map_err(|err| loop_or_failure(py, &locals, &err))?;
        Ok((call.unbind(), pending))
    })?;
    tokio::pin!(pending);
    let outcome = tokio::select! {
        outcome = &mut pending => outcome,
        () = cancel.cancelled() => {
            cancel_on_loop(&call, &locals);
            // The task decides what the call returns; it ends at its next await.
            pending.await
        }
    };
    let value = outcome.map_err(|err| Python::attach(|py| loop_or_failure(py, &locals, &err)))?;
    Python::attach(|py| as_token(value.bind(py)))
}

/// A provider call as asyncio sees it. `cancel` works whether the task has
/// started or not, which a bare `Task.cancel` scheduled from outside cannot
/// promise.
const CALL_SOURCE: &CStr = c"
import asyncio

class Call:
    def __init__(self):
        self.task = None
        self.cancelled = False

    async def run(self, awaitable):
        if self.cancelled:
            close = getattr(awaitable, 'close', None)
            if close is not None:
                close()
            raise asyncio.CancelledError()
        self.task = asyncio.current_task()
        return await awaitable

    def cancel(self):
        self.cancelled = True
        if self.task is not None:
            self.task.cancel()
";

fn provider_call(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    PyModule::from_code(py, CALL_SOURCE, c"felix_token_call.py", c"felix_token_call")?
        .getattr("Call")?
        .call0()
}

/// Cancel the call's task on its loop. A closed loop has nothing to cancel.
fn cancel_on_loop(call: &Py<PyAny>, locals: &TaskLocals) {
    Python::attach(|py| {
        if loop_closed(py, locals) {
            return;
        }
        let Ok(cancel) = call.bind(py).getattr("cancel") else {
            return;
        };
        let _ = locals
            .event_loop(py)
            .call_method1("call_soon_threadsafe", (cancel,));
    });
}

const LOOP_CLOSED: &str = "the event loop the token provider was bound to is closed";

/// The provider's loop is the one running at connect / `with_identity`; once it
/// is closed no provider call can run, which must not read as a bad token.
fn ensure_loop_open(py: Python<'_>, locals: &TaskLocals) -> Result<()> {
    if loop_closed(py, locals) {
        return Err(anyhow!(LOOP_CLOSED));
    }
    Ok(())
}

fn loop_closed(py: Python<'_>, locals: &TaskLocals) -> bool {
    locals
        .event_loop(py)
        .call_method0("is_closed")
        .and_then(|closed| closed.is_truthy())
        .unwrap_or(false)
}

fn loop_or_failure(py: Python<'_>, locals: &TaskLocals, err: &PyErr) -> anyhow::Error {
    if loop_closed(py, locals) {
        anyhow!(LOOP_CLOSED)
    } else {
        failure(py, err)
    }
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "an unknown type".to_string(), |name| name.to_string())
}

/// The provider's result as a token: a non-empty `str`, or a failure.
fn as_token(value: &Bound<'_, PyAny>) -> Result<String> {
    let token = value.extract::<String>().map_err(|_| {
        if value.is_instance_of::<PyString>() {
            // A str holding lone surrogates cannot be encoded as UTF-8.
            anyhow!("token_provider returned a str that is not valid UTF-8")
        } else {
            anyhow!("token_provider must return a str, not {}", type_name(value))
        }
    })?;
    if token.is_empty() {
        return Err(anyhow!("token_provider returned an empty token"));
    }
    Ok(token)
}

/// A Python exception as an error carrying its message, or its type name when
/// it has none.
fn failure(py: Python<'_>, err: &PyErr) -> anyhow::Error {
    match err.value(py).str().map(|text| text.to_string()) {
        Ok(text) if !text.is_empty() => anyhow!(text),
        _ => anyhow!(
            "{}",
            err.get_type(py).name().map_or_else(
                |_| "token_provider failed".to_string(),
                |name| name.to_string()
            )
        ),
    }
}
