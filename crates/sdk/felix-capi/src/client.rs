//! Connecting, and the runtime a client owns.

use std::ffi::{CString, c_char, c_void};
use std::future::Future;
use std::ptr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Context;
use felix_client::{Client, ClientConfig, ReconnectPolicy, Seed};
use tokio::runtime::{Handle, Runtime};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::boundary::{Failure, guard, guard_void, optional_str, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::completion::{Completion, Deliver, user_cancelled};
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_OK, FelixStatus};
use crate::token::{FelixTokenFetch, FelixTokenRelease, TokenSource, provider};
use crate::tuning::{FelixPublishSharding, FelixSubQueuePolicy, Tuning};

/// Two workers are plenty for one client's connections and background tasks,
/// and keep a process with several clients from spawning a thread per core
/// for each.
const RUNTIME_WORKERS: usize = 2;

/// How long freeing the last handle waits for its cancelled calls to report,
/// and then for the runtime to stop.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Called once when `felix_client_connect` or `felix_client_connect_with`
/// finishes. On `FELIX_STATUS_OK`,
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
    /// The client acting as another principal, for a handle made by
    /// `felix_client_with_identity`. Absent for the connected client itself.
    identity: Option<Arc<Client>>,
    /// The identity's own token fetch, failed when this handle is freed so a
    /// fetch waiting on the app does not outlive it.
    tokens: Option<Arc<TokenSource>>,
}

impl FelixClient {
    fn new(shared: Arc<Shared>) -> Self {
        let cancel = shared.cancel.child_token();
        Self {
            shared,
            cancel,
            identity: None,
            tokens: None,
        }
    }

    /// A handle on `shared`'s runtime and connections that acts as `identity`.
    pub(crate) fn acting_as(
        shared: Arc<Shared>,
        identity: Client,
        tokens: Option<Arc<TokenSource>>,
    ) -> Self {
        Self {
            cancel: shared.cancel.child_token(),
            shared,
            identity: Some(Arc::new(identity)),
            tokens,
        }
    }

    /// The cluster client every call on this handle goes through.
    pub(crate) fn cluster(&self) -> &Arc<Client> {
        self.identity
            .as_ref()
            .unwrap_or_else(|| self.shared.client())
    }
}

impl Drop for FelixClient {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(tokens) = &self.tokens {
            tokens.close();
        }
        // A client dropped outside the runtime closes its streams without one.
        let _enter = self.shared.handle().enter();
        drop(self.identity.take());
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
    client: OnceLock<Arc<Client>>,
    runtime: Option<Runtime>,
    tracker: TaskTracker,
    /// The parent of every handle's token.
    cancel: CancellationToken,
    /// The app's token fetch, failed when the last handle is freed so a fetch
    /// waiting on the app does not hold the shutdown.
    tokens: Option<Arc<TokenSource>>,
}

impl Shared {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            client: OnceLock::new(),
            runtime: Some(runtime),
            tracker: TaskTracker::new(),
            cancel: CancellationToken::new(),
            tokens: None,
        }
    }

    fn with_tokens(mut self, tokens: Option<Arc<TokenSource>>) -> Self {
        self.tokens = tokens;
        self
    }

    pub(crate) fn client(&self) -> &Arc<Client> {
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
        if let Some(tokens) = &self.tokens {
            tokens.close();
        }
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
    client: Option<Arc<Client>>,
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

/// What a client asks of the cluster beyond the defaults.
///
/// Set `size` to `sizeof(FelixConnectOptions)`; a field is read only when
/// `size` covers it, so a caller built against an older header keeps working
/// when fields are added, and a field it does not know is off.
#[repr(C)]
pub struct FelixConnectOptions {
    /// `sizeof(FelixConnectOptions)` as the caller compiled it.
    pub size: usize,
    /// Ask brokers to say who published each event, for
    /// `felix_event_publisher` and a group record's `publisher`. Adds the
    /// principal to every event batch.
    pub publishers: bool,
    /// Ask brokers for each record's append time, for `felix_event_timestamp`
    /// and a group record's `timestamp_micros`. Adds eight bytes per event.
    pub timestamps: bool,
    /// Supplies tokens after connect, so a client outlives its first token.
    /// The `token` argument is then only the first one, and may be null; the
    /// first connect then calls this. An empty `token` counts as none. See
    /// `FelixTokenFetch`. When `felix_client_connect_with` returns anything
    /// but `FELIX_STATUS_OK` this is not used, `token_release` never runs and
    /// `token_user_data` is still the caller's. When it returns
    /// `FELIX_STATUS_OK`, `token_release` runs exactly once later, including
    /// when the connect then reports failure through its callback.
    /// It runs after the client is freed and the last outstanding reply is
    /// answered: inside `felix_client_free` if none is outstanding, otherwise
    /// inside the answering `felix_token_reply_ok`/`_err` call.
    pub token_fetch: FelixTokenFetch,
    /// Passed to `token_fetch` and `token_release`, from any thread.
    pub token_user_data: *mut c_void,
    /// Runs once, after the client is freed and every reply handed to
    /// `token_fetch` has been answered, so `token_user_data` can be released.
    /// An app that never answers a reply leaks it. Used only with
    /// `token_fetch`.
    pub token_release: FelixTokenRelease,
    /// How many times a retried call tries to reach a broker before it gives
    /// up. Zero is the default, 5. Each attempt tries every known endpoint.
    pub reconnect_attempts: u32,
    /// A ceiling in milliseconds on one retried call, across every attempt and
    /// every wait between them. Zero is no ceiling, the default. A ceiling
    /// shorter than one attempt's own timeout leaves the call a single attempt,
    /// since that attempt spends the whole budget.
    pub reconnect_deadline_ms: u64,
    /// The ceiling in milliseconds of the first wait between attempts. It
    /// doubles each attempt up to `reconnect_max_backoff_ms`, and each wait is
    /// a random fraction of its ceiling. Zero is the default, 200.
    pub reconnect_backoff_ms: u64,
    /// The longest any one wait can be, in milliseconds. Zero is the default,
    /// 2000. Every wait is capped at it, even when the backoff is larger.
    /// Both backoff fields count anything past a year as a year.
    pub reconnect_max_backoff_ms: u64,
    /// A PEM file holding the client certificate, leaf first and then any
    /// intermediates, to present to a broker started with
    /// `FELIX_TLS_CLIENT_CA`. Null presents none. Needs `client_key_file`.
    /// Read during `felix_client_connect_with`; the string is not kept.
    pub client_cert_file: *const c_char,
    /// A PEM file holding the private key of `client_cert_file`: PKCS#8,
    /// PKCS#1 or SEC1. Null presents no certificate. Needs `client_cert_file`.
    pub client_key_file: *const c_char,
    /// Do not offer record headers to brokers. Off, the default, offers them,
    /// so `felix_event_key` and `felix_event_header` work against a broker
    /// that takes them; on, `felix_client_publish_with_headers` fails with
    /// `FELIX_STATUS_UNSUPPORTED` and events carry no headers.
    pub disable_record_headers: bool,
    /// Ask brokers to answer acknowledged publishes after the write, with
    /// the offset, as the broker-wide `FELIX_ACK_ON_COMMIT=true` does. Slower
    /// per publish. Whether a broker honours it is
    /// `FELIX_SUPPORTS_ACK_ON_COMMIT`.
    pub ack_on_commit: bool,
    /// Put every publish on the connection pool instead of giving the first
    /// shards of a stream a publish stream each. Wins over
    /// `publish_shard_streams`.
    pub no_publish_shard_streams: bool,
    /// What a subscription does when its client-side queue is full, a
    /// `FELIX_QUEUE_*` value. `FELIX_QUEUE_DEFAULT` drops the arriving item.
    pub client_sub_queue_policy: FelixSubQueuePolicy,
    /// How publishes are spread across publish streams, a `FELIX_SHARDING_*`
    /// value. `FELIX_SHARDING_DEFAULT` hashes the stream's name, which keeps
    /// a stream's publishes in order.
    pub publish_sharding: FelixPublishSharding,
    /// Events each subscription's client-side queue holds. Zero is the
    /// default, 256.
    pub client_sub_queue_capacity: u32,
    /// Batches the broker's queue for each subscription should hold. The
    /// broker clamps it to its maximum; `felix_subscription_queue_capacity`
    /// reports what was granted. Zero leaves the stream's default.
    pub broker_sub_queue_capacity: u32,
    /// Publish connections per client. Zero is the default, 4.
    pub publish_conn_pool: u32,
    /// Publish streams per publish connection. Zero is the default, 2.
    pub publish_streams_per_conn: u32,
    /// Publishes queued per publish stream before the next waits for room.
    /// Zero is the default, 64.
    pub publish_queue_depth: u32,
    /// Most publish streams opened per broker for one shard each, beside the
    /// pool. Zero is the default, 4; use `no_publish_shard_streams` for none.
    pub publish_shard_streams: u32,
    /// Cache connections per client. Zero is the default, 8.
    pub cache_conn_pool: u32,
    /// Cache streams per cache connection. Zero is the default, 4.
    pub cache_streams_per_conn: u32,
    /// Connections that carry subscriptions and cache watches. Zero is the
    /// default, 8.
    pub event_conn_pool: u32,
    /// Most connections opened to one broker. Zero is the default, 8.
    pub cluster_conn_pool: u32,
    /// Streams one of those connections carries before another is opened.
    /// Zero is the default, 1024.
    pub cluster_streams_per_conn: u32,
    /// Registrations and unclaimed event streams one event connection holds
    /// before refusing more. Zero is the default, 16384.
    pub event_router_max_pending: u32,
    /// Bytes of publishes sent but not yet answered, across the client. Zero
    /// is the default, 4 MiB.
    pub publish_inflight_bytes: u64,
    /// Largest frame the client will read. Zero is the default, 16 MiB.
    pub max_frame_bytes: u64,
    /// QUIC receive window per event connection, in bytes. Zero is the
    /// default, 256 MiB. The six windows must be below 2^62.
    pub event_conn_recv_window: u64,
    /// QUIC receive window per event stream, in bytes. Zero is the default,
    /// 64 MiB.
    pub event_stream_recv_window: u64,
    /// QUIC send window per event connection, in bytes. Zero is the default,
    /// 256 MiB.
    pub event_send_window: u64,
    /// QUIC receive window per cache connection, in bytes. Zero is the
    /// default, 256 MiB.
    pub cache_conn_recv_window: u64,
    /// QUIC receive window per cache stream, in bytes. Zero is the default,
    /// 64 MiB.
    pub cache_stream_recv_window: u64,
    /// QUIC send window per cache connection, in bytes. Zero is the default,
    /// 256 MiB.
    pub cache_send_window: u64,
    /// Longest a connect to one broker may take, in milliseconds. Zero is the
    /// default, 10000.
    pub connect_timeout_ms: u64,
    /// Longest a producer waits to flush when closed, in milliseconds. Zero is
    /// the default, 60000.
    pub finish_timeout_ms: u64,
}

/// The options a connect applies, read out of [`FelixConnectOptions`].
#[derive(Clone, Copy, Debug)]
struct ConnectOptions {
    publishers: bool,
    timestamps: bool,
    token_fetch: FelixTokenFetch,
    token_user_data: *mut c_void,
    token_release: FelixTokenRelease,
    reconnect_attempts: Option<usize>,
    reconnect_deadline: Option<Duration>,
    reconnect_backoff: Option<Duration>,
    reconnect_max_backoff: Option<Duration>,
    client_cert_file: *const c_char,
    client_key_file: *const c_char,
    disable_record_headers: bool,
    tuning: Tuning,
}

impl ConnectOptions {
    /// The default policy with the caller's attempts, deadline and backoff
    /// applied. Backoffs are capped at a year so a wait cannot overflow the clock.
    fn reconnect_policy(&self) -> Result<ReconnectPolicy, Failure> {
        const A_YEAR: Duration = Duration::from_secs(365 * 24 * 3600);
        let mut policy = ReconnectPolicy::default();
        if let Some(attempts) = self.reconnect_attempts {
            policy.attempts = attempts;
        }
        policy.deadline = self.reconnect_deadline;
        if let Some(backoff) = self.reconnect_backoff {
            policy.backoff = backoff.min(A_YEAR);
        }
        if let Some(max) = self.reconnect_max_backoff {
            policy.max_backoff = max.min(A_YEAR);
        }
        Ok(policy)
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            publishers: false,
            timestamps: false,
            token_fetch: None,
            token_user_data: ptr::null_mut(),
            token_release: None,
            reconnect_attempts: None,
            reconnect_deadline: None,
            reconnect_backoff: None,
            reconnect_max_backoff: None,
            client_cert_file: ptr::null(),
            client_key_file: ptr::null(),
            disable_record_headers: false,
            tuning: Tuning::default(),
        }
    }
}

impl ConnectOptions {
    /// # Safety
    ///
    /// `options` is null or points to at least `options.size` readable bytes.
    unsafe fn read(options: *const FelixConnectOptions) -> Result<Self, Failure> {
        if options.is_null() {
            return Ok(Self::default());
        }
        // SAFETY: readable up to its own `size`, which starts with `size`.
        let size = unsafe { ptr::addr_of!((*options).size).read() };
        if size < size_of::<usize>() {
            return Err(Failure::invalid(
                "options.size is smaller than the size field itself",
            ));
        }
        let flag = |offset: usize| {
            // SAFETY: `size` covers the byte. It is read as a byte, since C
            // may hold any value there.
            size > offset && unsafe { options.cast::<u8>().add(offset).read() } != 0
        };
        // A field the caller's struct does not reach reads as its default,
        // whatever follows the struct in memory.
        let at = |offset: usize, len: usize| {
            // SAFETY: `size` covers the field, so the address is in bounds.
            (size >= offset + len).then(|| unsafe { options.cast::<u8>().add(offset) })
        };
        let fetch = std::mem::offset_of!(FelixConnectOptions, token_fetch);
        let data = std::mem::offset_of!(FelixConnectOptions, token_user_data);
        let release = std::mem::offset_of!(FelixConnectOptions, token_release);
        let attempts = std::mem::offset_of!(FelixConnectOptions, reconnect_attempts);
        let deadline = std::mem::offset_of!(FelixConnectOptions, reconnect_deadline_ms);
        let backoff = std::mem::offset_of!(FelixConnectOptions, reconnect_backoff_ms);
        let max_backoff = std::mem::offset_of!(FelixConnectOptions, reconnect_max_backoff_ms);
        let cert_file = std::mem::offset_of!(FelixConnectOptions, client_cert_file);
        let key_file = std::mem::offset_of!(FelixConnectOptions, client_key_file);
        Ok(Self {
            publishers: flag(std::mem::offset_of!(FelixConnectOptions, publishers)),
            timestamps: flag(std::mem::offset_of!(FelixConnectOptions, timestamps)),
            // SAFETY: each is inside the `size` bytes the caller promised
            // readable. Read unaligned: nothing checks the caller's alignment.
            token_fetch: at(fetch, size_of::<FelixTokenFetch>())
                .and_then(|p| unsafe { p.cast::<FelixTokenFetch>().read_unaligned() }),
            token_user_data: at(data, size_of::<*mut c_void>())
                .map_or(ptr::null_mut(), |p| unsafe {
                    p.cast::<*mut c_void>().read_unaligned()
                }),
            token_release: at(release, size_of::<FelixTokenRelease>())
                .and_then(|p| unsafe { p.cast::<FelixTokenRelease>().read_unaligned() }),
            reconnect_attempts: at(attempts, size_of::<u32>())
                .map(|p| unsafe { p.cast::<u32>().read_unaligned() })
                .filter(|&n| n != 0)
                .map(|n| n as usize),
            reconnect_deadline: at(deadline, size_of::<u64>())
                .map(|p| unsafe { p.cast::<u64>().read_unaligned() })
                .filter(|&ms| ms != 0)
                .map(Duration::from_millis),
            reconnect_backoff: at(backoff, size_of::<u64>())
                .map(|p| unsafe { p.cast::<u64>().read_unaligned() })
                .filter(|&ms| ms != 0)
                .map(Duration::from_millis),
            reconnect_max_backoff: at(max_backoff, size_of::<u64>())
                .map(|p| unsafe { p.cast::<u64>().read_unaligned() })
                .filter(|&ms| ms != 0)
                .map(Duration::from_millis),
            client_cert_file: at(cert_file, size_of::<*const c_char>())
                .map_or(ptr::null(), |p| unsafe {
                    p.cast::<*const c_char>().read_unaligned()
                }),
            client_key_file: at(key_file, size_of::<*const c_char>())
                .map_or(ptr::null(), |p| unsafe {
                    p.cast::<*const c_char>().read_unaligned()
                }),
            disable_record_headers: flag(std::mem::offset_of!(
                FelixConnectOptions,
                disable_record_headers
            )),
            // SAFETY: `size` bytes are readable, per the contract above.
            tuning: unsafe { Tuning::read(options, size) }?,
        })
    }
}

/// Connect to a Felix cluster. `felix_client_connect` is this with default
/// options.
///
/// `addrs` is one `host:port`, or several separated by commas; any reachable
/// one is enough and the rest are discovered. A name is resolved again whenever
/// no broker the client knows answers; one that does not resolve now completes
/// with `FELIX_STATUS_INVALID_ARGUMENT`. `tenant_id` and `token` are the
/// credentials. `server_name` is the name the broker certificate must carry,
/// `"localhost"` when null. `ca_file` is a PEM file of CA certificates to
/// trust; when null the operating system's trust store is used.
///
/// `options` is null for the defaults, or a `FelixConnectOptions` with its
/// `size` set. A broker that does not negotiate an option leaves it absent on
/// the records, never an error.
///
/// `cancel` is null or a token that stops the connect. `callback` runs once
/// with the new client, unless this returns something other than
/// `FELIX_STATUS_OK`.
///
/// # Safety
///
/// Every string is null or NUL-terminated, `options` is null or readable up
/// to its `size`, `cancel` is null or a live token, and `callback` is safe to
/// call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_connect_with(
    addrs: *const c_char,
    tenant_id: *const c_char,
    token: *const c_char,
    server_name: *const c_char,
    ca_file: *const c_char,
    options: *const FelixConnectOptions,
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
                optional_str(token, "token")?,
                optional_str(server_name, "server_name")?.unwrap_or("localhost"),
                optional_str(ca_file, "ca_file")?,
            )
        };
        // Checked again by `Completion::new`; here it saves starting a runtime.
        if callback.is_none() {
            return Err(Failure::invalid("callback is null"));
        }
        // SAFETY: null or readable up to its size, per the contract above.
        let options = unsafe { ConnectOptions::read(options) }?;
        // Without a fetch function the token is all the client will ever have.
        if token.is_none() && options.token_fetch.is_none() {
            return Err(Failure::invalid("token is null"));
        }
        let seeds = parse_addrs(addrs)?;
        let server_name = server_name.to_string();

        // SAFETY: null or NUL-terminated strings, per the contract above.
        let (cert_file, key_file) = unsafe {
            (
                optional_str(options.client_cert_file, "client_cert_file")?,
                optional_str(options.client_key_file, "client_key_file")?,
            )
        };
        let mut config = connect_config(&options, tenant_id, token, ca_file, cert_file, key_file)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKERS)
            .thread_name("felix-capi")
            .enable_all()
            .build()
            .context("could not start the Felix runtime")?;
        let done = Completion::<Box<FelixClient>>::new(callback, user_data)?;
        // Built last: from here nothing fails, so `token_release` runs only
        // for a connect that returned OK.
        let tokens = options.token_fetch.map(|fetch| {
            Arc::new(TokenSource::new(
                fetch,
                options.token_user_data,
                options.token_release,
            ))
        });
        if let Some(tokens) = &tokens {
            config.token_provider = Some(provider(tokens, token));
        }
        let policy = options.reconnect_policy()?;
        let shared = Arc::new(Shared::new(runtime).with_tokens(tokens));
        let task_shared = Arc::clone(&shared);
        shared.spawn(done, move |done| async move {
            let connect = async {
                resolve_names(&seeds).await?;
                Client::connect_seeds(&seeds, &server_name, config, policy)
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

/// The client config a connect builds: TLS, credentials, the protocol features
/// the caller asked for, and the queue and transport tuning.
fn connect_config(
    options: &ConnectOptions,
    tenant_id: &str,
    token: Option<&str>,
    ca_file: Option<&str>,
    cert_file: Option<&str>,
    key_file: Option<&str>,
) -> Result<ClientConfig, Failure> {
    let mut config = tls_config(ca_file, cert_file, key_file)?;
    config.auth_tenant_id = Some(tenant_id.to_string());
    config.auth_token = token.map(str::to_string);
    config.publishers = options.publishers;
    config.timestamps = options.timestamps;
    config.record_headers = !options.disable_record_headers;
    options.tuning.apply(&mut config);
    Ok(config)
}

/// The QUIC config for a connect: the CA to trust, and the client certificate
/// to present when both files are given.
fn tls_config(
    ca_file: Option<&str>,
    cert_file: Option<&str>,
    key_file: Option<&str>,
) -> Result<ClientConfig, Failure> {
    let roots = match ca_file {
        Some(path) => Some(Arc::new(
            felix_client::root_store_from_pem_file(path).map_err(invalid)?,
        )),
        None => None,
    };
    let quinn = match (cert_file, key_file) {
        (None, None) => felix_client::quic_client_config(roots, false).map_err(invalid)?,
        (Some(cert), Some(key)) => {
            let identity =
                felix_client::ClientIdentity::from_pem_files(cert, key).map_err(invalid)?;
            felix_client::quic_client_config_with_identity(roots, identity, false)
                .map_err(invalid)?
        }
        _ => {
            return Err(Failure::invalid(
                "client_cert_file and client_key_file must be given together",
            ));
        }
    };
    Ok(ClientConfig::optimized_defaults(quinn))
}

/// Connect to a Felix cluster with the default options: the same as
/// `felix_client_connect_with` with null `options`.
///
/// # Safety
///
/// As for `felix_client_connect_with`, without `options`.
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
    // SAFETY: the caller's contract, passed through.
    unsafe {
        felix_client_connect_with(
            addrs,
            tenant_id,
            token,
            server_name,
            ca_file,
            ptr::null(),
            cancel,
            callback,
            user_data,
        )
    }
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
        let cluster = Arc::clone(client.cluster());
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
