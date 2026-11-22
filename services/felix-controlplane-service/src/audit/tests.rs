use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::*;
use crate::store::AuthStore;

fn entry(path: &str) -> AuditEntry {
    AuditEntry {
        at_ms: 1,
        member: "1".to_string(),
        listener: "api".to_string(),
        peer: None,
        method: "POST".to_string(),
        route: path.to_string(),
        path: path.to_string(),
        tenant: None,
        subject: None,
        actor: None,
        cert_sha256: None,
        credential: "none".to_string(),
        status: 204,
        refusal: None,
        suppressed: 0,
    }
}

#[test]
fn the_ring_keeps_the_newest_and_numbers_without_gaps() {
    let mut log = AuditLog::default();
    log.append((0..AUDIT_RETAIN + 5).map(|_| entry("/v1/x")).collect());
    let page = log.page(0, 3);
    assert_eq!(page.oldest_seq, Some(6));
    assert_eq!(page.last_seq, (AUDIT_RETAIN + 5) as u64);
    assert_eq!(
        page.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        [6, 7, 8]
    );
    assert_eq!(log.records.len(), AUDIT_RETAIN);
    let seqs: Vec<u64> = log.records.iter().map(|r| r.seq).collect();
    assert!(seqs.windows(2).all(|pair| pair[1] == pair[0] + 1));
    // Paging past the end answers nothing, with the position still known.
    let end = log.page(page.last_seq, 10);
    assert!(end.records.is_empty());
    assert_eq!(end.last_seq, page.last_seq);
}

#[test]
fn refusals_past_the_burst_are_counted_on_the_next_one_recorded() {
    let sink = AuditSink::new("1");
    for _ in 0..REFUSAL_BURST as usize {
        assert!(sink.admit_refusal(&mut entry("/v1/x")));
    }
    assert!(!sink.admit_refusal(&mut entry("/v1/x")));
    assert!(!sink.admit_refusal(&mut entry("/v1/x")));
    // A second later the bucket has refilled.
    sink.inner.refusals.lock().expect("bucket").at -= std::time::Duration::from_secs(1);
    let mut next = entry("/v1/x");
    assert!(sink.admit_refusal(&mut next));
    assert_eq!(next.suppressed, 2);
}

struct Fixture {
    state: AppState,
    app: axum::Router,
    keys: crate::auth::felix_token::TenantSigningKeys,
}

async fn fixture() -> Fixture {
    let (store, _) = crate::test_support::one_shard_cluster().await;
    let keys = store.get_tenant_signing_keys("t1").await.expect("keys");
    let mut state = crate::test_support::app_state_ready(store);
    state.audit = AuditSink::new("7");
    let app = audited(
        crate::api::build_router(state.clone()),
        state.clone(),
        "api",
    );
    Fixture { state, app, keys }
}

impl Fixture {
    async fn send(&self, method: &str, uri: &str, token: Option<&str>, body: &str) -> StatusCode {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let mut request = request.body(Body::from(body.to_string())).expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 1, 2, 3], 4567))));
        self.app
            .clone()
            .oneshot(request)
            .await
            .expect("response")
            .status()
    }

    /// Every record committed, once the sink has drained.
    async fn records(&self) -> Vec<AuditRecord> {
        self.state.audit.close().await;
        self.state
            .store
            .audit_records(0, MAX_PAGE)
            .await
            .expect("records")
            .records
    }
}

const POLICY: &str =
    r#"{"subject":"role:BODYSECRET","object":"stream:t1/ns/orders","action":"stream.publish"}"#;

#[tokio::test]
async fn a_write_is_recorded_without_its_token_body_or_query() {
    let fixture = fixture().await;
    let token = crate::test_support::token(&fixture.keys, &["rbac.policy.manage:tenant:t1"]);
    let status = fixture
        .send(
            "POST",
            "/v1/tenants/t1/rbac/policies?secret=QUERYSECRET",
            Some(&token),
            POLICY,
        )
        .await;
    assert!(status.is_success(), "{status}");

    let records = fixture.records().await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0].entry;
    assert_eq!(record.member, "7");
    assert_eq!(record.listener, "api");
    assert_eq!(record.peer.as_deref(), Some("10.1.2.3:4567"));
    assert_eq!(record.route, "/v1/tenants/{tenant_id}/rbac/policies");
    assert_eq!(record.path, "/v1/tenants/t1/rbac/policies");
    assert_eq!(record.tenant.as_deref(), Some("t1"));
    assert_eq!(record.subject.as_deref(), Some("p:test"));
    assert_eq!(record.credential, "felix_token");
    assert_eq!(record.status, status.as_u16());
    let line = serde_json::to_string(&records).expect("json");
    for secret in [token.as_str(), "BODYSECRET", "QUERYSECRET"] {
        assert!(!line.contains(secret), "{secret} recorded: {line}");
    }
}

#[tokio::test]
async fn a_forbidden_request_names_the_verified_subject() {
    let fixture = fixture().await;
    let token = crate::test_support::token(&fixture.keys, &["stream.publish:tenant:t1"]);
    let status = fixture
        .send("POST", "/v1/tenants/t1/rbac/policies", Some(&token), POLICY)
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let records = fixture.records().await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0].entry;
    assert_eq!(record.status, 403);
    assert_eq!(record.subject.as_deref(), Some("p:test"));
    assert_eq!(record.refusal.as_deref(), Some("forbidden"));
}

#[tokio::test]
async fn a_refused_credential_names_nobody_and_keeps_only_the_route() {
    let fixture = fixture().await;
    let status = fixture
        .send(
            "GET",
            "/v1/tenants/t1/rbac/policies",
            Some("not.a.token"),
            "",
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let records = fixture.records().await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0].entry;
    assert_eq!(record.status, 401);
    assert_eq!(record.subject, None);
    assert_eq!(record.path, record.route);
    assert_eq!(record.credential, "none");
    assert!(record.refusal.is_some());
}

#[tokio::test]
async fn reading_the_log_needs_audit_view() {
    let fixture = fixture().await;
    let writer = crate::test_support::token(&fixture.keys, &["rbac.policy.manage:tenant:t1"]);
    fixture
        .send(
            "POST",
            "/v1/tenants/t1/rbac/policies",
            Some(&writer),
            POLICY,
        )
        .await;
    // What a broker holds is not enough.
    let broker = crate::test_support::token(
        &fixture.keys,
        &["node.manage:cluster:*", "node.view:cluster:*"],
    );
    assert_eq!(
        fixture.send("GET", "/v1/audit", Some(&broker), "").await,
        StatusCode::FORBIDDEN
    );
    let auditor = crate::test_support::token(&fixture.keys, &["audit.view:cluster:*"]);
    let request = Request::get("/v1/audit?after=0&limit=10")
        .header("authorization", format!("Bearer {auditor}"))
        .body(Body::empty())
        .expect("request");
    // Let the write's record commit before reading.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let response = fixture
        .app
        .clone()
        .oneshot(request)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    // The broker's refusal and the write, but not the successful read.
    assert_eq!(fixture.records().await.len(), 2);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let page: AuditPage = serde_json::from_slice(&body).expect("page");
    assert!(
        page.records.iter().any(|record| record.entry.route
            == "/v1/tenants/{tenant_id}/rbac/policies"
            && record.entry.method == "POST"),
        "{page:?}"
    );
}

#[tokio::test]
async fn telemetry_is_not_recorded_but_its_refusals_are() {
    let fixture = fixture().await;
    let broker = crate::test_support::token(&fixture.keys, &["node.manage:cluster:*"]);
    fixture
        .send(
            "POST",
            "/v1/nodes/broker-x/heartbeat",
            Some(&broker),
            r#"{"incarnation":0}"#,
        )
        .await;
    let status = fixture
        .send(
            "POST",
            "/v1/nodes/broker-x/heartbeat",
            None,
            r#"{"incarnation":0}"#,
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let records = fixture.records().await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].entry.status, 401);
}

#[tokio::test]
async fn a_closed_sink_drops_and_counts() {
    let fixture = fixture().await;
    fixture.state.audit.close().await;
    let token = crate::test_support::token(&fixture.keys, &["rbac.policy.manage:tenant:t1"]);
    fixture
        .send("POST", "/v1/tenants/t1/rbac/policies", Some(&token), POLICY)
        .await;
    let page = fixture
        .state
        .store
        .audit_records(0, 10)
        .await
        .expect("records");
    assert!(page.records.is_empty());
}
