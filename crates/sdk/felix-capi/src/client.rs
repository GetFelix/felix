//! Connecting, and the runtime a client owns.

use std::ffi::{CString, c_char, c_void};
use std::future::Future;
use std::ptr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use felix_client::{ClientConfig, ClusterClient, ReconnectPolicy, Seed};
use tokio::runtime::{Handle, Runtime};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::boundary::{Failure, guard, guard_void, optional_str, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::completion::{Completion, Deliver, user_cancelled};
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_OK, FelixStatus};

/// Two workers are plenty for one client's connections and background tasks,
/// and keep a process with several clients from spawning a thread per core
/// for each.
const RUNTIME_WORKERS: usize = 2;

/// How long freeing the last handle waits for its cancelled calls to report,
/// and then for the runtime to stop.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Called once when `felix_client_connect` finishes. On `FELIX_STATUS_OK`,
/// `client` is a new client the callee owns; otherwise it is null.
pub type FelixConnectCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        client: *mut FelixClient,
    ),
>;

/// Called once when `felix_client_endpoints` finishes. On `FELIX_STATUS_OK`,
/// `addrs` holds `count` NUL-terminated `host:port` strings. The array and the
/// strings are valid only while the callback runs; both are null on failure.
pub type FelixEndpointsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        addrs: *const *const c_char,
        count: usize,
    ),
>;

/// A connection to a Felix cluster, with the runtime that drives it.
///
/// Safe to use from several threads at once, but not while another thread
/// frees it. Freeing it cancels its pending calls; subscriptions made from
/// it keep working until they are freed.
pub struct FelixClient {
    pub(crate) shared: Arc<Shared>,
    /// Cancelled when this handle is freed, which ends its pending calls.
    pub(crate) cancel: CancellationToken,
}

impl FelixClient {
    fn new(shared: Arc<Shared>) -> Self {
        let cancel = shared.cancel.child_token();
        Self { shared, cancel }
    }
}

impl Drop for FelixClient {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Deliver for Box<FelixClient> {
    type Callback = FelixConnectCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    ) {
        if let Some(callback) = callback {
            let client = value.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, client) };
        }
    }
}

/// The client and its runtime, shared with every handle made from it so the
/// runtime outlives the last of them.
///
/// Tasks hold the client, not this, unless they create a handle; so the
/// runtime stops when the last handle is freed, not when the last call ends.
pub(crate) struct Shared {
    client: OnceLock<Arc<ClusterClient>>,
    runtime: Option<Runtime>,
    tracker: TaskTracker,
    /// The parent of every handle's token.
    cancel: CancellationToken,
}

impl Shared {
    fn new(runtime: Runtime) -> Self {
        Self {
            client: OnceLock::new(),
            runtime: Some(runtime),
            tracker: TaskTracker::new(),
            cancel: CancellationToken::new(),
        }
    }

    pub(crate) fn client(&self) -> &Arc<ClusterClient> {
        self.client
            .get()
            .expect("a handle exists only once connected")
    }

    pub(crate) fn handle(&self) -> &Handle {
        self.runtime
            .as_ref()
            .expect("runtime is present until drop")
            .handle()
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel.child_token()
    }

    /// Run `task` on the runtime, handing it `done` to finish.
    ///
    /// Nothing between building the future and spawning it can panic, so
    /// `done` is never dropped here while the call also reports a status.
    pub(crate) fn spawn<V, F>(&self, done: Completion<V>, task: impl FnOnce(Completion<V>) -> F)
    where
        V: Deliver,
        F: Future<Output = ()> + Send + 'static,
    {
        self.handle().spawn(self.tracker.track_future(task(done)));
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let teardown = Teardown {
            runtime: Some(runtime),
            client: self.client.take(),
            cancel: self.cancel.clone(),
            tracker: self.tracker.clone(),
        };
        // The last handle can be freed from a callback, on one of this
        // runtime's own threads, or from inside some other runtime. Blocking
        // on a runtime there panics, so the teardown moves to its own thread.
        // If that thread cannot start, dropping `Teardown` still cancels.
        if Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("felix-capi-shutdown".to_string())
                .spawn(move || teardown.run());
        } else {
            teardown.run();
        }
    }
}

/// Stopping a runtime once its last handle is gone.
struct Teardown {
    runtime: Option<Runtime>,
    client: Option<Arc<ClusterClient>>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl Teardown {
    /// Must not run inside a runtime: it blocks on this one.
    fn run(mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        // Cancel first and let the tasks see it, so every pending call
        // reports through its own cancel path rather than being dropped.
        self.cancel.cancel();
        self.tracker.close();
        let client = self.client.take();
        let tracker = self.tracker.clone();
        runtime.block_on(async {
            let _ = tokio::time::timeout(SHUTDOWN_GRACE, tracker.wait()).await;
            // The client's connections close through the runtime.
            drop(client);
        });
        runtime.shutdown_timeout(SHUTDOWN_GRACE);
    }
}

impl Drop for Teardown {
    /// Reached only when `run` never did. Nothing here blocks, so it is safe
    /// inside a runtime; tasks dropped by the shutdown still call back.
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            self.cancel.cancel();
            {
                let _enter = runtime.enter();
                drop(self.client.take());
            }
            runtime.shutdown_background();
        }
    }
}

/// Connect to a Felix cluster.
///
/// `addrs` is one `host:port`, or several separated by commas; any reachable
/// one is enough and the rest are discovered. A name is resolved again whenever
/// no broker the client knows answers; one that does not resolve now completes
/// with `FELIX_STATUS_INVALID_ARGUMENT`. `tenant_id` and `token` are the
/// credentials. `server_name` is the name the broker certificate must carry,
/// `"localhost"` when null. `ca_file` is a PEM file of CA certificates to
/// trust; when null the operating system's trust store is used.
///
/// `cancel` is null or a token that stops the connect. `callback` runs once
/// with the new client, unless this returns something other than
/// `FELIX_STATUS_OK`.
///
/// # Safety
///
/// Every string is null or NUL-terminated, `cancel` is null or a live token,
/// and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_connect(
    addrs: *const c_char,
    tenant_id: *const c_char,
    token: *const c_char,
    server_name: *const c_char,
    ca_file: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixConnectCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: each pointer is null or a NUL-terminated string, per the
        // contract above.
        let (addrs, tenant_id, token, server_name, ca_file) = unsafe {
            (
                required_str(addrs, "addrs")?,
                required_str(tenant_id, "tenant_id")?,
                required_str(token, "token")?,
                optional_str(server_name, "server_name")?.unwrap_or("localhost"),
                optional_str(ca_file, "ca_file")?,
            )
        };
        // Checked again by `Completion::new`; here it saves starting a runtime.
        if callback.is_none() {
            return Err(Failure::invalid("callback is null"));
        }
        let seeds = parse_addrs(addrs)?;
        let server_name = server_name.to_string();

        let roots = match ca_file {
            Some(path) => Some(Arc::new(
                felix_client::root_store_from_pem_file(path).map_err(invalid)?,
            )),
            None => None,
        };
        let quinn = felix_client::quic_client_config(roots, false).map_err(invalid)?;
        let mut config = ClientConfig::optimized_defaults(quinn);
        config.auth_tenant_id = Some(tenant_id.to_string());
        config.auth_token = Some(token.to_string());
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKERS)
            .thread_name("felix-capi")
            .enable_all()
            .build()
            .context("could not start the Felix runtime")?;
        let shared = Arc::new(Shared::new(runtime));
        let task_shared = Arc::clone(&shared);
        let done = Completion::<Box<FelixClient>>::new(callback, user_data)?;
        shared.spawn(done, move |done| async move {
            let connect = async {
                resolve_names(&seeds).await?;
                ClusterClient::connect_seeds(
                    &seeds,
                    &server_name,
                    config,
                    ReconnectPolicy::default(),
                )
                .await
                .map_err(Failure::from)
            };
            let outcome = tokio::select! {
                biased;
                () = user_cancelled(user.as_ref()) => None,
                outcome = connect => Some(outcome),
            };
            // A client that connected as the caller cancelled is dropped, so
            // a cancelled connect never hands one out.
            let cancelled_now = user.as_ref().is_some_and(CancellationToken::is_cancelled);
            match outcome {
                Some(Ok(client)) if !cancelled_now => {
                    let _ = task_shared.client.set(Arc::new(client));
                    done.ok(Box::new(FelixClient::new(task_shared)));
                }
                Some(Err(failure)) if !cancelled_now => done.fail(failure),
                _ => done.status(FELIX_STATUS_CANCELLED),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a client. Null is ignored.
///
/// Its pending calls complete with `FELIX_STATUS_CANCELLED` (a publish that
/// may have been sent, with `FELIX_STATUS_OUTCOME_UNKNOWN`), possibly after
/// this returns. When it is the last handle on its runtime, this waits up to
/// a few seconds for those calls to report and the runtime to stop, unless
/// it is called from a callback or another async runtime: then it returns at
/// once and a library thread finishes the teardown. Subscriptions made from
/// it keep working until they are freed.
///
/// # Safety
///
/// `client` is null or a pointer from a connect callback that has not been
/// freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_free(client: *mut FelixClient) {
    guard_void(|| {
        if !client.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the connect
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(client) });
        }
    });
}

/// Each comma-separated `host:port`. Names are kept for the client to resolve,
/// on connect and again whenever every address it holds stops answering.
/// The addresses a client would try, ready to hand to C.
pub(crate) struct Endpoints(Vec<CString>);

impl Endpoints {
    fn new(addrs: Vec<String>) -> Self {
        // A socket address never holds a NUL, so none is dropped.
        Self(
            addrs
                .into_iter()
                .filter_map(|addr| CString::new(addr).ok())
                .collect(),
        )
    }
}

impl Deliver for Endpoints {
    type Callback = FelixEndpointsCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    ) {
        if let Some(callback) = callback {
            let addrs: Vec<*const c_char> = value
                .iter()
                .flat_map(|endpoints| endpoints.0.iter().map(|addr| addr.as_ptr()))
                .collect();
            let at = if addrs.is_empty() {
                ptr::null()
            } else {
                addrs.as_ptr()
            };
            // SAFETY: the callback's contract is the header's; `value` and
            // `addrs` live until it returns.
            unsafe { callback(user_data, status, message, at, addrs.len()) };
        }
    }
}

/// List everywhere this client would try to reach the cluster: the seeds it
/// was given and the brokers it learned from them. A client given one seed
/// that reports more than one address has discovered the cluster.
///
/// `cancel` is null or a token that stops the call, which then completes with
/// `FELIX_STATUS_CANCELLED`. `callback` runs once, unless this returns
/// something other than `FELIX_STATUS_OK`.
///
/// # Safety
///
/// `client` is a live client, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_endpoints(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixEndpointsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Endpoints>::new(callback, user_data)?;
        let cluster = Arc::clone(client.shared.client());
        let ask = async move {
            Ok(cluster
                .endpoints()
                .await
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>())
        };
        spawn_read(client, user, done, ask, Endpoints::new);
        Ok(FELIX_STATUS_OK)
    })
}

fn parse_addrs(addrs: &str) -> Result<Vec<Seed>, Failure> {
    let seeds = addrs
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<Seed>()
                .map_err(|err| Failure::invalid(format!("{err:#}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if seeds.is_empty() {
        return Err(Failure::invalid("addrs holds no broker address"));
    }
    Ok(seeds)
}

/// Resolve every name once, so one that does not resolve now is the
/// caller's bad argument rather than a connection failure. Done on the
/// runtime: a lookup can take seconds.
async fn resolve_names(seeds: &[Seed]) -> Result<(), Failure> {
    for seed in seeds {
        if let Seed::Name(name) = seed {
            tokio::net::lookup_host(name.as_str())
                .await
                .map_err(|err| Failure::invalid(format!("could not resolve {name:?}: {err}")))?
                .next()
                .ok_or_else(|| Failure::invalid(format!("{name:?} resolved to no addresses")))?;
        }
    }
    Ok(())
}

fn invalid(err: anyhow::Error) -> Failure {
    Failure::invalid(format!("{err:#}"))
}

#[cfg(test)]
mod tests;
