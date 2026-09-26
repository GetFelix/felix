//! Requests and credentials for the API suites.
use axum::body::Body;
use axum::http::Request;

/// The tenant whose keys sign every cluster-scoped credential minted here.
///
/// A cluster-scoped permission lives in some tenant's token — the `tid` only
/// picks the verification keys — so an operator needs a tenant to be minted
/// from. Tests use this one; a deployment bootstraps its own.
pub(crate) const OPERATOR_TENANT: &str = "ops";

/// Signing keys every test credential is minted with.
///
/// One key set for every tenant a test touches, so a token minted before a
/// tenant exists verifies once [`Credentials::adopt`] binds the keys to it.
pub(crate) struct Credentials {
    pub(crate) keys: felix_controlplane_service::auth::felix_token::TenantSigningKeys,
}

impl Credentials {
    pub(crate) fn token(&self, tenant_id: &str, perms: &[&str]) -> String {
        felix_controlplane_service::auth::felix_token::mint_token_for(
            &self.keys,
            tenant_id,
            "p:test",
            perms.iter().map(|perm| perm.to_string()).collect(),
            std::time::Duration::from_secs(900),
            felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
        )
        .expect("mint token")
    }

    /// An operator: manages the tenant catalog and reads cluster metadata.
    pub(crate) fn operator(&self) -> String {
        self.token(
            OPERATOR_TENANT,
            &["tenant.manage:cluster:*", "node.view:cluster:*"],
        )
    }

    /// A broker: reads the metadata feeds, nothing else.
    pub(crate) fn broker(&self) -> String {
        self.token(OPERATOR_TENANT, &["node.view:cluster:*"])
    }

    /// A tenant admin, with the same expansion token exchange would apply to
    /// `tenant.manage:tenant:{id}`.
    pub(crate) fn tenant_admin(&self, tenant_id: &str) -> String {
        self.token(
            tenant_id,
            &[
                &format!("tenant.manage:tenant:{tenant_id}"),
                &format!("ns.manage:namespace:{tenant_id}/*"),
                &format!("stream.manage:stream:{tenant_id}/*/*"),
                &format!("cache.manage:cache:{tenant_id}/*/*"),
            ],
        )
    }

    /// Bind the shared keys to `tenant_id`.
    ///
    /// Needed after a tenant is created through the API, which generates its
    /// own keys; tokens minted here would not verify against those.
    pub(crate) async fn adopt(
        &self,
        store: &(dyn felix_controlplane_service::store::AuthStore + Send + Sync),
        tenant_id: &str,
    ) {
        store
            .set_tenant_signing_keys(tenant_id, self.keys.clone())
            .await
            .expect("adopt tenant keys");
    }
}

/// Seed the operator tenant and its keys, so cluster-scoped tokens verify.
pub(crate) async fn seed_credentials(
    store: &(dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync),
) -> Credentials {
    let keys =
        felix_controlplane_service::auth::keys::generate_signing_keys().expect("generate keys");
    // Create-then-set, in that order: creation resets a tenant's keys.
    match store
        .create_tenant(felix_controlplane_service::model::Tenant {
            tenant_id: OPERATOR_TENANT.to_string(),
            display_name: "Operators".to_string(),
        })
        .await
    {
        Ok(_) | Err(felix_controlplane_service::store::StoreError::Conflict(_)) => {}
        Err(err) => panic!("seed operator tenant: {err}"),
    }
    let credentials = Credentials { keys };
    credentials.adopt(store, OPERATOR_TENANT).await;
    credentials
}

pub(crate) fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(crate) fn json_request_as(
    method: &str,
    uri: &str,
    token: &str,
    body: serde_json::Value,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("request")
}

pub(crate) fn request_as(method: &str, uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .expect("request")
}

pub(crate) async fn read_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}
