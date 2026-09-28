//! The control plane's two routers: the public API and the bootstrap listener.
//!
//! Every public route is registered here and, separately, in
//! [`crate::api::openapi`]; axum does not care about the order routes are
//! added, so they are grouped by resource.
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utoipa::OpenApi;

use crate::api;
use crate::api::AppState;
use crate::api::openapi::ApiDoc;
use crate::api::trace_context::trace_context_from_headers;
use crate::auth;

/// Count a request for the whole time it is being served.
///
/// Placed outside the handlers so it cannot be forgotten on a new route, and so
/// it counts time spent in every other layer too — a request stuck in the trace
/// layer is still a request this instance owes an answer for.
async fn count_in_flight(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _guard = state.in_flight.enter();
    next.run(request).await
}

/// The public API: every `/v1` route, the OpenAPI document, and the
/// middleware that traces and counts each request.
pub fn build_router(state: AppState) -> Router {
    let trace_layer =
        TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
            let parent = trace_context_from_headers(request.headers());
            let span = tracing::info_span!(
                "http.request",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version()
            );
            let _ = span.set_parent(parent);
            span
        });

    Router::new()
        .route(
            "/v1/system/info",
            axum::routing::get(api::system::system_info),
        )
        .route(
            "/v1/system/health",
            axum::routing::get(api::system::system_health),
        )
        // Two probes, because they drive different actions: `live` decides
        // whether to restart the process, `ready` whether to send it traffic.
        // See `docs/control-plane.md` for the intervals these expect.
        .route(
            "/v1/system/live",
            axum::routing::get(api::system::system_live),
        )
        .route(
            "/v1/system/ready",
            axum::routing::get(api::system::system_ready),
        )
        .route(
            "/v1/regions",
            axum::routing::get(api::regions::list_regions),
        )
        .route(
            "/v1/regions/{region_id}",
            axum::routing::get(api::regions::get_region),
        )
        .route(
            "/v1/tenants",
            axum::routing::get(api::tenants::list_tenants).post(api::tenants::create_tenant),
        )
        .route(
            "/v1/tenants/{tenant_id}",
            axum::routing::delete(api::tenants::delete_tenant),
        )
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(api::tenants::tenant_snapshot),
        )
        .route(
            "/v1/tenants/changes",
            axum::routing::get(api::tenants::tenant_changes),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces",
            axum::routing::get(api::namespaces::list_namespaces)
                .post(api::namespaces::create_namespace),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}",
            axum::routing::delete(api::namespaces::delete_namespace),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(api::namespaces::namespace_snapshot),
        )
        .route(
            "/v1/namespaces/changes",
            axum::routing::get(api::namespaces::namespace_changes),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
            axum::routing::get(api::streams::list_streams).post(api::streams::create_stream),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
            axum::routing::get(api::streams::get_stream)
                .patch(api::streams::patch_stream)
                .delete(api::streams::delete_stream),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(api::streams::stream_snapshot),
        )
        .route(
            "/v1/streams/changes",
            axum::routing::get(api::streams::stream_changes),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
            axum::routing::get(api::caches::list_caches).post(api::caches::create_cache),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
            axum::routing::get(api::caches::get_cache)
                .patch(api::caches::patch_cache)
                .delete(api::caches::delete_cache),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(api::caches::cache_snapshot),
        )
        .route(
            "/v1/caches/changes",
            axum::routing::get(api::caches::cache_changes),
        )
        .route(
            "/v1/nodes",
            axum::routing::get(api::nodes::listing::list_nodes).post(api::nodes::register_node),
        )
        .route(
            "/v1/fleet/features",
            axum::routing::get(api::nodes::fleet::fleet_features),
        )
        .route(
            "/v1/fleet/features/{feature}/finalize",
            axum::routing::post(api::nodes::fleet::finalize_fleet_feature),
        )
        .route(
            "/v1/nodes/{node_id}",
            axum::routing::get(api::nodes::listing::get_node)
                .patch(api::nodes::patch_node)
                .delete(api::nodes::delete_node),
        )
        .route(
            "/v1/nodes/{node_id}/heartbeat",
            axum::routing::post(api::nodes::reports::report_health),
        )
        .route(
            "/v1/nodes/{node_id}/replica-status",
            axum::routing::post(api::nodes::reports::report_replica_status),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            axum::routing::post(api::nodes::drain_node),
        )
        .route(
            "/v1/nodes/{node_id}/deregister",
            axum::routing::post(api::nodes::deregister_node),
        )
        .route(
            "/v1/shard-assignments",
            axum::routing::get(api::shard_assignments::list_shard_assignments),
        )
        .route(
            "/v1/shard-assignments/snapshot",
            axum::routing::get(api::shard_assignments::shard_assignment_snapshot),
        )
        .route(
            "/v1/shard-assignments/changes",
            axum::routing::get(api::shard_assignments::shard_assignment_changes),
        )
        .route(
            "/v1/shard-moves",
            axum::routing::get(api::shard_moves::list_shard_moves)
                .post(api::shard_moves::start_shard_move),
        )
        .route(
            "/v1/shard-moves/{tenant_id}/{namespace}/{name}/{shard}",
            axum::routing::delete(api::shard_moves::cancel_shard_move),
        )
        .route(
            "/v1/placement/plan",
            axum::routing::get(api::shard_moves::placement_plan),
        )
        .route(
            "/v1/placement/abandon/{tenant_id}/{namespace}/{name}/{shard}",
            axum::routing::post(api::shard_moves::abandon_shard_log),
        )
        .route(
            "/v1/placement/pause",
            axum::routing::post(api::shard_moves::pause_placement),
        )
        .route(
            "/v1/placement/resume",
            axum::routing::post(api::shard_moves::resume_placement),
        )
        .route(
            "/v1/tenants/{tenant_id}/token/exchange",
            axum::routing::post(auth::exchange::exchange_token),
        )
        .route(
            "/v1/tenants/{tenant_id}/token/refresh",
            axum::routing::post(auth::refresh::refresh_token_handler),
        )
        .route(
            "/v1/tenants/{tenant_id}/.well-known/jwks.json",
            axum::routing::get(auth::jwks::tenant_jwks),
        )
        .route(
            "/v1/tenants/{tenant_id}/idp-issuers",
            axum::routing::post(auth::admin::upsert_idp_issuer),
        )
        .route(
            "/v1/tenants/{tenant_id}/idp-issuers/{issuer}",
            axum::routing::delete(auth::admin::delete_idp_issuer),
        )
        .route(
            "/v1/tenants/{tenant_id}/rbac/policies",
            axum::routing::get(auth::admin::list_policies)
                .post(auth::admin::add_policy)
                .delete(auth::admin::remove_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/rbac/groupings",
            axum::routing::get(auth::admin::list_groupings)
                .post(auth::admin::add_grouping)
                .delete(auth::admin::remove_grouping),
        )
        .route(
            "/v1/tenants/{tenant_id}/refresh-tokens/revoke",
            axum::routing::post(auth::admin::revoke_principal_refresh_tokens),
        )
        .route(
            "/v1/tenants/{tenant_id}/signing-keys",
            axum::routing::get(auth::signing_keys::list_signing_keys)
                .post(auth::signing_keys::stage_signing_key),
        )
        .route(
            "/v1/tenants/{tenant_id}/signing-keys/{kid}",
            axum::routing::delete(auth::signing_keys::retire_signing_key),
        )
        .route(
            "/v1/tenants/{tenant_id}/signing-keys/{kid}/activate",
            axum::routing::post(auth::signing_keys::activate_signing_key),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs").url("/v1/openapi.json", ApiDoc::openapi()),
        )
        .layer(trace_layer)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            count_in_flight,
        ))
        .with_state(state)
}

/// The bootstrap listener's router: tenant auth initialization and nothing else.
pub fn build_bootstrap_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/internal/bootstrap/tenants/{tenant_id}/initialize",
            axum::routing::post(api::bootstrap::initialize),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
