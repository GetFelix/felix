//! The audit log: who did what on the control plane.
//!
//! A middleware on both routers records every write and every refused
//! request once the handler answers. Handlers say who the caller was through
//! the `note_*` functions, which fill a slot scoped to the request. Records
//! are committed through the metadata Raft log, so a member cannot rewrite
//! what a majority already holds. `docs-site/.../security/audit-log.md` has
//! the claims and their limits.
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{ConnectInfo, MatchedPath, Query, Request, State};
use axum::http::{HeaderMap, Method};
use axum::middleware::Next;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal};
use crate::auth::cert_binding::PeerCertificates;
use crate::store::ControlPlaneAuthStore;

/// How many records every member keeps. A constant, not a setting, so every
/// member trims at the same record.
pub const AUDIT_RETAIN: usize = 10_000;

/// The largest page `GET /v1/audit` answers.
const MAX_PAGE: usize = 1_000;

/// Refusals recorded per second, and the burst, per member. Refusals need no
/// credential, so without a bound a flood of bad requests is a flood of Raft
/// writes.
const REFUSALS_PER_SEC: f64 = 20.0;
const REFUSAL_BURST: f64 = 100.0;

/// Records not committed, by `reason`.
pub(crate) const AUDIT_DROPPED_TOTAL: &str = "felix_audit_records_dropped_total";

/// The log target every audit line is written under.
pub(crate) const AUDIT_TARGET: &str = "felix_audit";

/// One request, as recorded. Never a token, a body, a query string or a
/// claim that did not verify.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AuditEntry {
    /// The serving member's clock, in milliseconds since the epoch.
    pub at_ms: u64,
    /// The control-plane member that served the request, as it reports itself.
    pub member: String,
    /// `api` or `bootstrap`.
    pub listener: String,
    /// The socket address the request came from. Forwarding headers are not read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    pub method: String,
    /// The route template, such as `/v1/tenants/{tenant_id}/rbac/policies`.
    pub route: String,
    /// The path, without its query string.
    pub path: String,
    /// The tenant of the verified credential, or the one the path names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The verified subject, or the one a minted token was issued to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// The actor of a delegated token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// SHA-256 of the client's TLS leaf certificate, in hex.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_sha256: Option<String>,
    /// What the caller presented that verified: `felix_token`,
    /// `bootstrap_token`, `operator_token`, `idp_token`, `refresh_token`, or
    /// `none`.
    pub credential: String,
    /// The HTTP status answered, or 0 when the request ended before an answer.
    pub status: u16,
    /// Why a credential was refused, as `felix_controlplane_auth_rejected_total`
    /// labels it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// Refusals this member did not record, over its rate limit, since the
    /// last one it did.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub suppressed: u64,
}

/// A committed entry and its place in the log. `seq` has no gaps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AuditRecord {
    pub seq: u64,
    #[serde(flatten)]
    pub entry: AuditEntry,
}

/// The records a member keeps: the newest [`AUDIT_RETAIN`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLog {
    /// The last `seq` given out.
    pub last_seq: u64,
    pub records: VecDeque<AuditRecord>,
}

impl AuditLog {
    pub(crate) fn is_empty(&self) -> bool {
        self.last_seq == 0
    }

    /// Append in order, trim to [`AUDIT_RETAIN`], and log each record.
    pub(crate) fn append(&mut self, entries: Vec<AuditEntry>) {
        for entry in entries {
            self.last_seq += 1;
            let record = AuditRecord {
                seq: self.last_seq,
                entry,
            };
            // Every member writes every record to its own log, so one member
            // cannot remove the copies a log collector already has.
            tracing::info!(
                target: AUDIT_TARGET,
                record = %serde_json::to_string(&record).unwrap_or_default(),
                "audit record"
            );
            self.records.push_back(record);
        }
        while self.records.len() > AUDIT_RETAIN {
            self.records.pop_front();
        }
    }

    pub(crate) fn page(&self, after: u64, limit: usize) -> AuditPage {
        let skip = self.records.partition_point(|record| record.seq <= after);
        AuditPage {
            records: self
                .records
                .iter()
                .skip(skip)
                .take(limit)
                .cloned()
                .collect(),
            oldest_seq: self.records.front().map(|record| record.seq),
            last_seq: self.last_seq,
        }
    }
}

/// One page of `GET /v1/audit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AuditPage {
    pub records: Vec<AuditRecord>,
    /// The oldest record still kept. A reader whose `after + 1` is below it
    /// missed records to retention.
    pub oldest_seq: Option<u64>,
    pub last_seq: u64,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct AuditQuery {
    /// Answer records after this `seq`. 0 by default.
    pub after: Option<u64>,
    /// At most this many, up to 1000. 100 by default.
    pub limit: Option<usize>,
}

/// Read the audit log. Requires `audit.view` on `cluster:*`.
#[utoipa::path(
    get,
    path = "/v1/audit",
    tag = "audit",
    params(AuditQuery),
    responses((status = 200, body = AuditPage), (status = 401), (status = 403))
)]
pub async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AuditQuery>,
) -> Result<axum::Json<AuditPage>, ApiError> {
    crate::auth::bearer::require_cluster_action(
        &state,
        &headers,
        crate::auth::rbac::authorize::ACTION_AUDIT_VIEW,
    )
    .await?;
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_PAGE);
    state
        .store
        .audit_records(query.after.unwrap_or(0), limit)
        .await
        .map(axum::Json)
        .map_err(|err| api_internal("failed to read the audit log", &err))
}

/// What the handler learned about the caller.
#[derive(Debug, Default)]
struct Noted {
    tenant: Option<String>,
    subject: Option<String>,
    actor: Option<String>,
    credential: Option<&'static str>,
    refusal: Option<&'static str>,
}

tokio::task_local! {
    static NOTED: Arc<Mutex<Noted>>;
}

fn note(fill: impl FnOnce(&mut Noted)) {
    // Outside a request (a handler called from a test) there is nothing to fill.
    let _ = NOTED.try_with(|noted| {
        if let Ok(mut noted) = noted.lock() {
            fill(&mut noted);
        }
    });
}

/// A credential verified: the tenant, subject and actor it names.
///
/// The first credential a request verifies is the caller. A second one, such
/// as a gateway's `actor_token` on an exchange, is recorded as the actor.
pub(crate) fn note_verified(
    credential: &'static str,
    tenant: &str,
    subject: &str,
    actor: Option<&str>,
) {
    note(|noted| {
        noted.refusal = None;
        if noted.subject.is_some() {
            if noted.actor.is_none() {
                noted.actor = Some(subject.to_string());
            }
            return;
        }
        noted.credential = Some(credential);
        noted.tenant = Some(tenant.to_string());
        noted.subject = Some(subject.to_string());
        noted.actor = actor.map(str::to_string);
    });
}

/// A credential that names no subject verified, such as the bootstrap token.
pub(crate) fn note_credential(credential: &'static str) {
    note(|noted| {
        noted.credential = Some(credential);
        noted.refusal = None;
    });
}

/// A credential was refused, for `reason`.
pub(crate) fn note_refusal(reason: &'static str) {
    note(|noted| noted.refusal = Some(reason));
}

/// Record the requests `router` serves on the listener `name` (`api` or
/// `bootstrap`). Apply it last, outside every other layer, so a refusal any
/// of them answers is recorded too.
pub fn audited(router: axum::Router, state: AppState, name: &'static str) -> axum::Router {
    router.layer(axum::middleware::from_fn_with_state(
        Listener { state, name },
        record,
    ))
}

/// Which listener a router serves, for [`record`].
#[derive(Clone)]
struct Listener {
    state: AppState,
    name: &'static str,
}

/// Broker reports that run every few seconds per broker. A success there is
/// telemetry, not an action, and would push real actions out of the window;
/// a refusal is still recorded.
const TELEMETRY_ROUTES: &[&str] = &[
    "/v1/nodes/{node_id}/heartbeat",
    "/v1/nodes/{node_id}/replica-status",
    "/v1/nodes/{node_id}/raft-copy-id",
    "/v1/nodes/{node_id}/raft-floors",
    "/v1/nodes/{node_id}/seal-report",
    "/v1/nodes/{node_id}/seal-give-up",
    "/v1/nodes/{node_id}/reform/reports",
    "/v1/shard-assignments/confirmed",
];

/// Record the request once it is answered, or as unfinished if it is dropped
/// first.
async fn record(State(listener): State<Listener>, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string());
    let write = !matches!(method, Method::GET | Method::HEAD | Method::OPTIONS);
    let Some(route) = route else {
        // No route matched: a 404 with no handler, nothing was done.
        return next.run(request).await;
    };
    let entry = AuditEntry {
        at_ms: now_ms(),
        member: listener.state.audit.member().to_string(),
        listener: listener.name.to_string(),
        peer: request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.0.to_string()),
        method: method.to_string(),
        path: truncate(request.uri().path(), MAX_PATH),
        tenant: path_tenant(request.uri().path()),
        cert_sha256: request
            .extensions()
            .get::<PeerCertificates>()
            .and_then(|certs| certs.0.first().map(|leaf| hex(&sha2::Sha256::digest(leaf)))),
        route,
        subject: None,
        actor: None,
        credential: "none".to_string(),
        status: 0,
        refusal: None,
        suppressed: 0,
    };
    let noted = Arc::new(Mutex::new(Noted::default()));
    let mut pending = Pending {
        sink: listener.state.audit.clone(),
        store: Arc::clone(&listener.state.store),
        entry: Some(entry),
        noted: Arc::clone(&noted),
        write,
    };
    let response = NOTED.scope(noted, next.run(request)).await;
    pending.finish(response.status().as_u16());
    response
}

/// A request's entry until it is submitted. Dropped unsubmitted, the request
/// future was cancelled (the client went away mid-handler) and the entry goes
/// in with status 0, since the action may have been done anyway.
struct Pending {
    sink: AuditSink,
    store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    entry: Option<AuditEntry>,
    noted: Arc<Mutex<Noted>>,
    write: bool,
}

impl Pending {
    fn finish(&mut self, status: u16) {
        let Some(mut entry) = self.entry.take() else {
            return;
        };
        let refused = matches!(status, 401 | 403);
        if !refused && (!self.write || TELEMETRY_ROUTES.contains(&entry.route.as_str())) {
            return;
        }
        entry.status = status;
        if let Ok(noted) = self.noted.lock() {
            if noted.tenant.is_some() {
                entry.tenant.clone_from(&noted.tenant);
            }
            entry.subject.clone_from(&noted.subject);
            entry.actor.clone_from(&noted.actor);
            entry.credential = noted.credential.unwrap_or("none").to_string();
            entry.refusal = noted.refusal.map(str::to_string);
            // A credential that did not verify names nobody. A verified one
            // that lacked the permission is the refusal most worth naming.
            if refused && !matches!(noted.refusal, Some("forbidden" | "tenant_mismatch")) {
                entry.subject = None;
                entry.actor = None;
            }
            if refused {
                // The caller picks the path of a refused request: keep only
                // the template, so it cannot size or fill the record.
                entry.path.clone_from(&entry.route);
                entry.tenant = noted.tenant.clone();
            }
        }
        if refused && !self.sink.admit_refusal(&mut entry) {
            return;
        }
        self.sink.submit(&self.store, entry);
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if self.entry.is_some() && self.write {
            self.finish(0);
        }
    }
}

/// Where a member's records go: queued, then committed in batches by one
/// task, so a burst of requests is a few Raft writes rather than one each.
/// The request has already been answered by then: a record never holds up or
/// refuses the action it describes. Past the queue bound a record is logged
/// and counted, not kept.
#[derive(Clone)]
pub struct AuditSink {
    inner: Arc<SinkInner>,
}

struct SinkInner {
    member: String,
    /// `None` once closed. The worker starts with the first record, on the
    /// store that record's request was served from.
    queue: Mutex<Queue>,
    refusals: Mutex<RefusalBucket>,
}

#[derive(Default)]
struct Queue {
    sender: Option<tokio::sync::mpsc::Sender<AuditEntry>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    closed: bool,
}

struct RefusalBucket {
    tokens: f64,
    at: Instant,
    suppressed: u64,
}

impl AuditSink {
    /// A sink for the member named `member` in its records.
    pub fn new(member: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(SinkInner {
                member: member.into(),
                queue: Mutex::new(Queue::default()),
                refusals: Mutex::new(RefusalBucket {
                    tokens: REFUSAL_BURST,
                    at: Instant::now(),
                    suppressed: 0,
                }),
            }),
        }
    }

    /// The member id this sink writes into its records.
    pub(crate) fn member(&self) -> &str {
        &self.inner.member
    }

    /// Stop taking records and wait until the queued ones are committed or
    /// dropped. Called before the Raft node stops, so a member that drains
    /// does not lose the records of the requests it just answered.
    pub async fn close(&self) {
        let worker = {
            let Ok(mut queue) = self.inner.queue.lock() else {
                return;
            };
            queue.closed = true;
            queue.sender = None;
            queue.worker.take()
        };
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }

    /// Take a refusal under the rate limit, carrying the count suppressed
    /// since the last one taken.
    fn admit_refusal(&self, entry: &mut AuditEntry) -> bool {
        let Ok(mut bucket) = self.inner.refusals.lock() else {
            return true;
        };
        let now = Instant::now();
        let refill = now.duration_since(bucket.at).as_secs_f64() * REFUSALS_PER_SEC;
        bucket.tokens = (bucket.tokens + refill).min(REFUSAL_BURST);
        bucket.at = now;
        if bucket.tokens < 1.0 {
            bucket.suppressed += 1;
            metrics::counter!(AUDIT_DROPPED_TOTAL, "reason" => "rate_limited").increment(1);
            return false;
        }
        bucket.tokens -= 1.0;
        entry.suppressed = std::mem::take(&mut bucket.suppressed);
        true
    }

    fn submit(&self, store: &Arc<dyn ControlPlaneAuthStore + Send + Sync>, entry: AuditEntry) {
        let Ok(mut queue) = self.inner.queue.lock() else {
            dropped("closed", &entry);
            return;
        };
        if queue.closed {
            drop(queue);
            dropped("closed", &entry);
            return;
        }
        if queue.sender.is_none() {
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                drop(queue);
                dropped("no_runtime", &entry);
                return;
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(MAX_QUEUED);
            queue.worker = Some(runtime.spawn(commit_batches(Arc::clone(store), receiver)));
            queue.sender = Some(sender);
        }
        let sent = queue.sender.as_ref().map(|sender| sender.try_send(entry));
        drop(queue);
        if let Some(Err(err)) = sent {
            dropped("busy", &err.into_inner());
        }
    }
}

impl Default for AuditSink {
    fn default() -> Self {
        Self::new("local")
    }
}

/// Records queued and not yet committed, per member.
const MAX_QUEUED: usize = 4_096;

/// The most records one Raft entry carries.
const MAX_BATCH: usize = 256;

/// Commit queued records, a batch at a time, until the sink closes and the
/// queue is empty. One batch in flight bounds the Raft writes records cost,
/// however many requests arrive. A failed batch is not retried here: the
/// store's write already retries across a leader change under one request
/// id, and a second id could commit the same records twice.
async fn commit_batches(
    store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    mut receiver: tokio::sync::mpsc::Receiver<AuditEntry>,
) {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    while receiver.recv_many(&mut batch, MAX_BATCH).await > 0 {
        let entries = std::mem::take(&mut batch);
        let count = entries.len() as u64;
        // Kept for the log line if the batch does not commit; moved into the
        // proposal otherwise.
        let lines: Vec<String> = entries
            .iter()
            .map(|entry| serde_json::to_string(entry).unwrap_or_default())
            .collect();
        match store.record_audit(entries).await {
            Ok(true) => {}
            // Below the metadata level that carries records: the local line
            // is the only copy, as on a build without them.
            Ok(false) => {
                metrics::counter!(AUDIT_DROPPED_TOTAL, "reason" => "level_pending")
                    .increment(count);
                for line in &lines {
                    tracing::info!(target: AUDIT_TARGET, entry = %line, "audit entry, not replicated");
                }
            }
            Err(err) => {
                metrics::counter!(AUDIT_DROPPED_TOTAL, "reason" => "store").increment(count);
                for line in &lines {
                    tracing::error!(target: AUDIT_TARGET, error = %err, entry = %line, "audit entry not committed");
                }
            }
        }
    }
}

fn dropped(reason: &'static str, entry: &AuditEntry) {
    metrics::counter!(AUDIT_DROPPED_TOTAL, "reason" => reason).increment(1);
    let line = serde_json::to_string(entry).unwrap_or_default();
    tracing::error!(target: AUDIT_TARGET, reason, entry = %line, "audit entry dropped");
}

/// The tenant a `/v1/tenants/{tenant_id}/...` or bootstrap path names.
fn path_tenant(path: &str) -> Option<String> {
    let rest = path
        .strip_prefix("/v1/tenants/")
        .or_else(|| path.strip_prefix("/internal/bootstrap/tenants/"))?;
    let tenant = rest.split('/').next()?;
    (!tenant.is_empty()).then(|| tenant.to_string())
}

/// The longest path kept in a record.
const MAX_PATH: usize = 256;

fn truncate(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests;
