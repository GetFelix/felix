use super::*;

/// A model that is not registered here is absent from the published schema
/// and from every generated client, and nothing else would catch it.
#[test]
fn the_node_model_reaches_the_generated_schema() {
    let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
    let schemas = doc["components"]["schemas"]
        .as_object()
        .expect("schemas object");

    for name in [
        "Node",
        "NodeSpec",
        "NodeStatus",
        "NodeCapacity",
        "NodeLifecycle",
        "NodePatchRequest",
        "NodeChange",
        "NodeChangeOp",
        "ShardAssignment",
        "ShardKey",
        "ShardState",
        "ShardAssignmentListResponse",
        "ShardAssignmentSnapshotResponse",
        "ShardAssignmentChangesResponse",
        "ShardAssignmentChange",
        "NodeView",
        "NodePlacement",
        "NodeListResponse",
        "NodeRegistrationRequest",
        "NodeRegistrationResponse",
        "NodeHeartbeatRequest",
        "NodeHeartbeatResponse",
    ] {
        assert!(
            schemas.contains_key(name),
            "{name} is missing from the schema"
        );
    }
}

/// A route the document does not describe is a route no generated client
/// can call, and nothing else in the build would notice.
#[test]
fn every_node_route_is_described() {
    let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
    let paths = doc["paths"].as_object().expect("paths object");

    for (path, method) in [
        ("/v1/nodes", "get"),
        ("/v1/nodes", "post"),
        ("/v1/nodes/{node_id}", "get"),
        ("/v1/nodes/{node_id}", "patch"),
        ("/v1/nodes/{node_id}", "delete"),
        ("/v1/nodes/{node_id}/heartbeat", "post"),
        ("/v1/nodes/{node_id}/drain", "post"),
        ("/v1/nodes/{node_id}/deregister", "post"),
        ("/v1/shard-assignments", "get"),
        ("/v1/shard-assignments/snapshot", "get"),
        ("/v1/shard-assignments/changes", "get"),
    ] {
        let described = paths
            .get(path)
            .and_then(|entry| entry.get(method))
            .is_some();
        assert!(
            described,
            "{method} {path} is missing from the OpenAPI document"
        );
    }
}

/// The split only holds on the wire if the patch schema has no observed
/// field to send.
#[test]
fn the_node_patch_schema_exposes_no_observed_field() {
    let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
    let properties = doc["components"]["schemas"]["NodePatchRequest"]["properties"]
        .as_object()
        .expect("patch properties");

    // Guards against the negative assertions below passing on an empty map.
    for settable in ["region", "labels", "capacity", "lifecycle"] {
        assert!(
            properties.contains_key(settable),
            "{settable} should be patchable",
        );
    }
    for observed in [
        "last_heartbeat_at_millis",
        "registered_at_millis",
        "incarnation",
    ] {
        assert!(
            !properties.contains_key(observed),
            "{observed} must not be patchable",
        );
    }
}

/// The operator's move controls are described, and so are their shapes.
#[test]
fn every_placement_route_is_described() {
    let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
    let paths = doc["paths"].as_object().expect("paths object");
    for (path, method) in [
        ("/v1/shard-moves", "get"),
        ("/v1/shard-moves", "post"),
        (
            "/v1/shard-moves/{tenant_id}/{namespace}/{name}/{shard}",
            "delete",
        ),
        ("/v1/placement/plan", "get"),
        (
            "/v1/placement/abandon/{tenant_id}/{namespace}/{name}/{shard}",
            "post",
        ),
        ("/v1/placement/pause", "post"),
        ("/v1/placement/resume", "post"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} is not described"
        );
    }
    let schemas = doc["components"]["schemas"]
        .as_object()
        .expect("schemas object");
    for name in [
        "ShardMove",
        "ShardMoveListResponse",
        "ShardMoveRequest",
        "ShardMoveResponse",
        "PlacementPlanResponse",
        "PlacementStatusResponse",
        "MoveReason",
    ] {
        assert!(
            schemas.contains_key(name),
            "{name} is missing from the schema"
        );
    }
}
