//! The dedicated runtimes quinn's driver tasks run on.
//!
//! Drivers do bounded work per poll and reschedule themselves, so their re-poll
//! latency is the pipeline's byte-rate ceiling; on a runtime shared with app
//! tasks that latency grows with load, and because wakeups scale with datagram
//! count it caps throughput per *byte* (measured ~7.5x below capacity). Each
//! endpoint gets a single-threaded runtime so driver self-wakes re-poll
//! immediately and never migrate cores; see [`EndpointRole`] for which
//! endpoints share one. `FELIX_IO_RUNTIME_THREADS` sets the pool size (derived
//! from the listener count on macOS, `0` elsewhere); `0` restores drivers to
//! the app runtime the endpoint was bound on.

use std::sync::{Arc, OnceLock};

/// How many server endpoints this process has said it will bind.
///
/// One unless a process declares otherwise, which is what a single-listener
/// broker and every test binding one server are.
static PLANNED_SERVER_ENDPOINTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1);

/// Built once, on the first endpoint. Module scope so [`plan_server_endpoints`]
/// can tell whether it is already too late to size it.
static IO_RUNTIMES: OnceLock<Vec<tokio::runtime::Runtime>> = OnceLock::new();

/// Whether an endpoint accepts connections or makes them.
///
/// Assignment is not round-robin across both: a server endpoint multiplexes
/// every connection and drives traffic in both directions, so it gets a
/// runtime to itself, while client endpoints share the rest. Letting a client
/// endpoint land on the server's runtime measured **5-6x slower** — the two
/// halves of a request/response ping-pong end up serialized on one thread. In
/// a standalone broker or a standalone client this is a no-op; it matters when
/// both live in one process, as in the benchmark harness and the in-process
/// client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointRole {
    Server,
    Client,
}

/// `quinn::Runtime` that runs driver tasks on one fixed runtime: the dedicated
/// I/O runtime, or the one the endpoint was bound on. Timers and the UDP socket
/// are created in that runtime's context so they register with its reactor;
/// application-facing stream futures are unaffected.
#[derive(Debug)]
struct IoRuntime {
    handle: tokio::runtime::Handle,
}

impl quinn::Runtime for IoRuntime {
    fn new_timer(&self, i: std::time::Instant) -> std::pin::Pin<Box<dyn quinn::AsyncTimer>> {
        let _guard = self.handle.enter();
        quinn::TokioRuntime.new_timer(i)
    }

    fn spawn(&self, future: std::pin::Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.handle.spawn(future);
    }

    fn wrap_udp_socket(
        &self,
        t: std::net::UdpSocket,
    ) -> std::io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        let _guard = self.handle.enter();
        quinn::TokioRuntime.wrap_udp_socket(t)
    }
}

/// The I/O runtime pool a process binding `server_endpoints` needs.
///
/// One runtime per server endpoint, because an endpoint's driver is a single
/// task and two endpoints sharing a runtime share its one thread, plus the one
/// reserved for every client endpoint.
///
/// Note this reproduces the historical default exactly: a process with one
/// server endpoint needs 2, which is what macOS defaulted to when a broker
/// could only have one listener.
pub const fn required_io_runtime_threads(server_endpoints: usize) -> usize {
    server_endpoints + 1
}

/// Declare how many server endpoints this process will bind, before it binds
/// the first one.
///
/// The pool is sized from this rather than from a separate setting. The two
/// numbers have to hold a relationship -- a pool smaller than the endpoints
/// using it silently puts several drivers on one thread, which is the ceiling
/// the endpoints were split up to escape -- and a relationship that must hold
/// is not something to leave two knobs free to break.
///
/// Has no effect once the pool exists: it is built on the first endpoint, and
/// tokio runtimes are not resized. Called after that, it warns rather than
/// pretending.
pub fn plan_server_endpoints(count: usize) {
    use std::sync::atomic::Ordering;
    PLANNED_SERVER_ENDPOINTS.store(count.max(1), Ordering::Relaxed);
    if io_runtime_pool_built() {
        tracing::warn!(
            planned = count,
            "plan_server_endpoints called after the I/O runtime pool was built; \
             the pool keeps the size it was created with",
        );
    }
}

/// Where an endpoint's quinn drivers and its connections' pump tasks run.
pub(crate) struct EndpointRuntime {
    pub(crate) quinn: Arc<dyn quinn::Runtime>,
    /// The dedicated I/O runtime, when isolation is on.
    pub(crate) io: Option<tokio::runtime::Handle>,
    /// The runtime the endpoint was bound on.
    pub(crate) app: Option<tokio::runtime::Handle>,
}

/// The quinn runtime new endpoints should use, honouring the isolation switch.
///
/// Without isolation, drivers still go to the runtime current at bind, not
/// the one current at spawn: quinn's `TokioRuntime` spawns a connection's
/// driver from whatever task dials it, and a Raft group thread that dials a
/// peer would otherwise carry that connection's I/O for its whole life.
pub(crate) fn quinn_runtime(role: EndpointRole) -> EndpointRuntime {
    let app = tokio::runtime::Handle::try_current().ok();
    let io = io_runtime_handle(role);
    let quinn: Arc<dyn quinn::Runtime> = match io.as_ref().or(app.as_ref()) {
        Some(handle) => Arc::new(IoRuntime {
            handle: handle.clone(),
        }),
        None => Arc::new(quinn::TokioRuntime),
    };
    EndpointRuntime { quinn, io, app }
}

/// Which runtime in a pool of `pool_len` an endpoint gets. The last one is
/// reserved for clients.
fn io_runtime_index(role: EndpointRole, sequence: usize, pool_len: usize) -> usize {
    if pool_len <= 1 {
        return 0;
    }
    match role {
        // The final runtime is permanently reserved for clients. Server
        // creation history must never let a server drift onto it.
        EndpointRole::Server => sequence % (pool_len - 1),
        EndpointRole::Client => pool_len - 1,
    }
}

fn io_runtime_pool_built() -> bool {
    IO_RUNTIMES.get().is_some()
}

fn io_runtime_handle(role: EndpointRole) -> Option<tokio::runtime::Handle> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT_SERVER: AtomicUsize = AtomicUsize::new(0);
    static NEXT_CLIENT: AtomicUsize = AtomicUsize::new(0);
    let pool = IO_RUNTIMES.get_or_init(|| {
        // Two, not one per core. A bigger pool cannot make a single endpoint
        // faster -- an endpoint's driver is one task on one runtime -- and it
        // actively hurts, because endpoints that talk to each other end up on
        // different threads and every message pays cross-thread wakes. Measured
        // at 6 runtimes before endpoints were grouped by role: slow mode on
        // every run. Two is what the roles need: servers on one, clients on the
        // other.
        // Driver isolation is a macOS optimization; on Linux it is a
        // pessimization, so it is defaulted on only where it measures faster.
        //
        // The ~7.5x per-byte ceiling this pool was built to fix is specific to
        // macOS: on Linux the same benchmark at the pre-fix baseline already
        // sustains ~643 MB/s (628K msg/s x 1 KiB), above what macOS reaches
        // even with every fix applied. Isolating drivers there only adds a
        // cross-thread hop per datagram, and it measures that way — p50
        // latency 86 us -> 152 us and fanout-10 throughput 1.48M -> 1.09M
        // msg/s, consistent across pool sizes 1/2/4/8 and with pump
        // colocation both on and off.
        //
        // `FELIX_IO_RUNTIME_THREADS` overrides on any platform.
        //
        // Sized from the endpoints that will use it, not fixed at 2. A broker
        // binding N client listeners has N+1 server endpoints with the internal
        // one, and a pool of 2 puts every one of their drivers on the same
        // thread -- `io_runtime_index` gives servers `sequence % (pool_len - 1)`,
        // which is `% 1` for a pool of 2. That is exactly the single feeder
        // multiple listeners exist to escape.
        let planned = PLANNED_SERVER_ENDPOINTS.load(Ordering::Relaxed).max(1);
        let default_threads = if cfg!(target_os = "macos") {
            required_io_runtime_threads(planned)
        } else {
            0
        };
        let threads = std::env::var("FELIX_IO_RUNTIME_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(default_threads);
        (0..threads)
            .filter_map(|index| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name(format!("felix-quic-io-{index}"))
                    .on_thread_start(|| {
                        // High QoS: OS demotion of these threads turns wakeup
                        // latency directly into a throughput ceiling.
                        #[cfg(target_os = "macos")]
                        unsafe {
                            libc::pthread_set_qos_class_self_np(
                                libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
                                0,
                            );
                        }
                    })
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        tracing::warn!(error = %err, "failed to build QUIC I/O runtime; drivers will share the app runtime");
                        err
                    })
                    .ok()
            })
            .collect()
    });
    if pool.is_empty() {
        return None;
    }
    // Keep the role partition stable for the life of the process. In
    // particular, repeated in-process benchmark cases create many sequential
    // server endpoints; creation history must not eventually place one on the
    // client runtime and recreate the measured 5-6x lockstep mode.
    let seq = match role {
        EndpointRole::Server => NEXT_SERVER.fetch_add(1, Ordering::Relaxed),
        EndpointRole::Client => {
            // Every client endpoint shares one runtime, deliberately. A client's
            // publish and event endpoints carry the two halves of the same
            // request/response flow, so splitting them across threads makes each
            // message pay two cross-thread wakes and the pipeline drops into
            // lockstep. Measured on the benchmark harness: co-located clients
            // ~123K msg/s, spread across 6 runtimes ~21K on every single run.
            NEXT_CLIENT.fetch_add(1, Ordering::Relaxed)
        }
    };
    let index = io_runtime_index(role, seq, pool.len());
    // Which endpoints share a runtime decides whether their drivers hand off
    // in-thread or pay a cross-thread wake per datagram, so this assignment is
    // load-bearing for throughput, not just bookkeeping.
    tracing::debug!(
        ?role,
        endpoint_seq = seq,
        io_runtime = index,
        pool = pool.len(),
        "assigned QUIC endpoint to I/O runtime"
    );
    Some(pool[index].handle().clone())
}

#[cfg(test)]
mod tests;
