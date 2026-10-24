//! Connecting, publishing, and the runtime a client owns.

use std::ffi::{c_char, c_void};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use felix_client::{AckMode, ClientConfig, ClusterClient, ReconnectPolicy, Seed};
use tokio::runtime::{Handle, Runtime};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::boundary::{Failure, guard, guard_void, optional_str, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled, user_cancelled};
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_OK, FELIX_STATUS_OUTCOME_UNKNOWN, FelixStatus,
};

/// Two workers are plenty for one client's connections and background tasks,
/// and keep a process with several clients from spawning a thread per core
/// for each.
const RUNTIME_WORKERS: usize = 2;

/// How long freeing the last handle waits for its cancelled calls to report,
/// and then for the runtime to stop.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// What the broker must have done before a publish completes.
pub type FelixAckMode = i32;

/// Fire and forget: complete once the record is sent.
pub const FELIX_ACK_NONE: FelixAckMode = 0;
/// Wait for the broker to acknowledge this record.
pub const FELIX_ACK_PER_MESSAGE: FelixAckMode = 1;
/// Wait for the broker to acknowledge the batch the record went out in.
pub const FELIX_ACK_PER_BATCH: FelixAckMode = 2;

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

/// Called once when `felix_client_publish` finishes. On `FELIX_STATUS_OK`,
/// `has_offset` says whether the broker reported the record's log offset,
/// and `offset` holds it when it did.
pub type FelixPublishCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        has_offset: bool,
        offset: u64,
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

/// A publish's result: the record's offset, when the broker reported one.
pub(crate) struct Published(pub(crate) Option<u64>);

impl Deliver for Published {
    type Callback = FelixPublishCallback;

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
            let offset = value.and_then(|published| published.0);
            // SAFETY: as for the connect callback.
            unsafe {
                callback(
                    user_data,
                    status,
                    message,
                    offset.is_some(),
                    offset.unwrap_or(0),
                )
            };
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
        let client = self.client.take();
        let cancel = self.cancel.clone();
        let tracker = self.tracker.clone();
        let teardown = move || {
            // Cancel first and let the tasks see it, so every pending call
            // reports through its own cancel path rather than being dropped.
            cancel.cancel();
            tracker.close();
            runtime.block_on(async {
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, tracker.wait()).await;
                // The client's connections close through the runtime.
                drop(client);
            });
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        };
        // The last handle can be freed from a callback, on one of this
        // runtime's own threads, or from inside some other runtime. Blocking
        // on a runtime there panics, so the teardown moves to its own thread.
        if Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("felix-capi-shutdown".to_string())
                .spawn(teardown);
        } else {
            teardown();
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
/// a few seconds for those calls to report and the runtime to stop.
/// Subscriptions made from it keep working until they are freed.
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

/// Publish one record to shard 0 of a stream.
///
/// `payload` may be null when `payload_len` is zero; it is copied before
/// this returns. `ack` is one of the `FELIX_ACK_*` values. There is no
/// offset when `ack` is `FELIX_ACK_NONE`, the stream has no log, or the
/// broker acknowledged before writing.
///
/// Publishes in flight at the same time may reach the broker in any order.
/// To keep records in order, start each one from the previous one's
/// callback.
///
/// A publish cancelled before it starts completes with
/// `FELIX_STATUS_CANCELLED`; after it may have been sent, with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`.
///
/// # Safety
///
/// `client` is a live client. The strings are null or NUL-terminated.
/// `payload` is null or valid for `payload_len` bytes. `cancel` is null or a
/// live token, and `callback` is safe to call with `user_data` from any
/// thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    payload: *const u8,
    payload_len: usize,
    ack: FelixAckMode,
    cancel: *const FelixCancelToken,
    callback: FelixPublishCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(stream, "stream")?.to_string(),
            )
        };
        let payload = if payload_len == 0 {
            Vec::new()
        } else if payload.is_null() {
            return Err(Failure::invalid(
                "payload is null but payload_len is not zero",
            ));
        } else {
            // SAFETY: non-null, and the caller promises `payload_len` readable
            // bytes. Copied, because the client owns what it sends.
            unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec()
        };
        let ack = match ack {
            FELIX_ACK_NONE => AckMode::None,
            FELIX_ACK_PER_MESSAGE => AckMode::PerMessage,
            FELIX_ACK_PER_BATCH => AckMode::PerBatch,
            other => {
                return Err(Failure::invalid(format!(
                    "ack {other} is not a FELIX_ACK_* value"
                )));
            }
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };

        let inner = Arc::clone(client.shared.client());
        let handle = client.cancel.clone();
        let done = Completion::<Published>::new(callback, user_data)?;
        client.shared.spawn(done, move |done| async move {
            if is_cancelled(&handle, user.as_ref()) {
                return done.status(FELIX_STATUS_CANCELLED);
            }
            tokio::select! {
                biased;
                // `publish` is not cancel-safe: once started, the record
                // may land anyway.
                () = cancelled(&handle, user.as_ref()) => done.fail(Failure {
                    status: FELIX_STATUS_OUTCOME_UNKNOWN,
                    message: "the publish was cancelled after it may have been sent".to_string(),
                }),
                published = inner.publish(&tenant_id, &ns, &stream, payload, ack) => {
                    done.result(published.map(Published).map_err(Failure::from));
                }
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Each comma-separated `host:port`. Names are kept for the client to resolve,
/// on connect and again whenever every address it holds stops answering.
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
