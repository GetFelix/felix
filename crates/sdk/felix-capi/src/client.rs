//! Connecting, publishing, and the runtime a client owns.

use std::ffi::c_char;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use felix_client::{AckMode, ClientConfig, ClusterClient};
use tokio::runtime::Runtime;

use crate::boundary::{Failure, guard, guard_void, optional_str, required_str, write_optional};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// Two workers are plenty for one client's connections and background tasks,
/// and keep a process with several clients from spawning a thread per core
/// for each.
const RUNTIME_WORKERS: usize = 2;

/// How long freeing a client waits for its background tasks to stop.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// What the broker must have done before a publish returns.
pub type FelixAckMode = i32;

/// Fire and forget: return once the record is sent.
pub const FELIX_ACK_NONE: FelixAckMode = 0;
/// Wait for the broker to acknowledge this record.
pub const FELIX_ACK_PER_MESSAGE: FelixAckMode = 1;
/// Wait for the broker to acknowledge the batch the record went out in.
pub const FELIX_ACK_PER_BATCH: FelixAckMode = 2;

/// A connection to a Felix cluster, with the runtime that drives it.
///
/// Safe to use from several threads at once. Free it with
/// `felix_client_free` after every subscription made from it is freed or
/// abandoned; a subscription keeps the runtime alive on its own.
pub struct FelixClient {
    pub(crate) shared: Arc<Shared>,
}

/// The client and its runtime, shared with every subscription made from it so
/// the runtime outlives the last of them.
pub(crate) struct Shared {
    client: Option<Arc<ClusterClient>>,
    runtime: Option<Runtime>,
}

impl Shared {
    pub(crate) fn client(&self) -> &Arc<ClusterClient> {
        self.client.as_ref().expect("client is present until drop")
    }

    pub(crate) fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("runtime is present until drop")
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // The client's connections close through the runtime, so it is
            // dropped inside it, and the runtime last.
            let guard = runtime.enter();
            self.client.take();
            drop(guard);
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        }
    }
}

/// Connect to a Felix cluster.
///
/// `addrs` is one `host:port`, or several separated by commas; any reachable
/// one is enough and the rest are discovered. `tenant_id` and `token` are the
/// credentials. `server_name` is the name the broker certificate must carry,
/// `"localhost"` when null. `ca_file` is a PEM file of CA certificates to
/// trust; when null the operating system's trust store is used.
///
/// On success `*out_client` holds a client to free with `felix_client_free`.
///
/// # Safety
///
/// Every string is null or NUL-terminated, and `out_client` points to
/// writable storage for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_connect(
    addrs: *const c_char,
    tenant_id: *const c_char,
    token: *const c_char,
    server_name: *const c_char,
    ca_file: *const c_char,
    out_client: *mut *mut FelixClient,
) -> FelixStatus {
    guard(|| {
        if out_client.is_null() {
            return Err(Failure::invalid("out_client is null"));
        }
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

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKERS)
            .thread_name("felix-capi")
            .enable_all()
            .build()
            .context("could not start the Felix runtime")?;
        let client = runtime.block_on(ClusterClient::connect(&seeds, server_name, config))?;

        let handle = Box::new(FelixClient {
            shared: Arc::new(Shared {
                client: Some(Arc::new(client)),
                runtime: Some(runtime),
            }),
        });
        // SAFETY: checked non-null above, and the caller promises it is
        // writable.
        unsafe { out_client.write(Box::into_raw(handle)) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a client. Null is ignored.
///
/// Waits briefly for the client's background tasks to stop. Subscriptions
/// made from it keep working until they are freed.
///
/// # Safety
///
/// `client` is null or a pointer from `felix_client_connect` that has not
/// been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_free(client: *mut FelixClient) {
    guard_void(|| {
        if !client.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in
            // `felix_client_connect` and is freed exactly once.
            drop(unsafe { Box::from_raw(client) });
        }
    });
}

/// Publish one record to shard 0 of a stream.
///
/// `payload` may be null when `payload_len` is zero. `ack` is one of the
/// `FELIX_ACK_*` values.
///
/// On success, `*out_has_offset` says whether the broker reported the
/// record's log offset, and `*out_offset` holds it when it did. There is no
/// offset when `ack` is `FELIX_ACK_NONE`, the stream has no log, or the
/// broker acknowledged before writing. Either out-pointer may be null.
///
/// # Safety
///
/// `client` is a live client. The strings are null or NUL-terminated.
/// `payload` is null or valid for `payload_len` bytes. The out-pointers are
/// null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    payload: *const u8,
    payload_len: usize,
    ack: FelixAckMode,
    out_offset: *mut u64,
    out_has_offset: *mut bool,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?,
                required_str(ns, "ns")?,
                required_str(stream, "stream")?,
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

        let shared = &client.shared;
        let offset = shared
            .runtime()
            .block_on(shared.client().publish(tenant_id, ns, stream, payload, ack))?;
        // SAFETY: each is null or writable, per the contract above.
        unsafe {
            write_optional(out_has_offset, offset.is_some());
            write_optional(out_offset, offset.unwrap_or(0));
        }
        Ok(FELIX_STATUS_OK)
    })
}

/// Resolve each comma-separated `host:port`. Names are resolved, not only
/// literal addresses, since a service name is the usual way to reach a broker.
fn parse_addrs(addrs: &str) -> Result<Vec<SocketAddr>, Failure> {
    let mut seeds = Vec::new();
    for item in addrs
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let addr = item
            .to_socket_addrs()
            .map_err(|err| Failure::invalid(format!("could not resolve {item:?}: {err}")))?
            .next()
            .ok_or_else(|| Failure::invalid(format!("{item:?} resolved to no addresses")))?;
        seeds.push(addr);
    }
    if seeds.is_empty() {
        return Err(Failure::invalid("addrs holds no broker address"));
    }
    Ok(seeds)
}

fn invalid(err: anyhow::Error) -> Failure {
    Failure::invalid(format!("{err:#}"))
}
