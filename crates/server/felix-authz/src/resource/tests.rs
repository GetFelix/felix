use super::*;

#[test]
fn resource_builders() {
    let tenant = TenantId::new("tenant-a");
    let namespace = Namespace::new("payments");
    let stream = StreamName::new("orders.v1");
    let cache = CacheScope::new("session");

    assert_eq!(tenant_resource(&tenant), "tenant:tenant-a");
    assert_eq!(
        namespace_resource(&tenant, &namespace),
        "namespace:tenant-a/payments"
    );
    assert_eq!(
        stream_resource(&tenant, &namespace, &stream),
        "stream:tenant-a/payments/orders.v1"
    );
    assert_eq!(
        cache_resource(&tenant, &namespace, &cache),
        "cache:tenant-a/payments/session"
    );
    assert_eq!(
        group_resource(&tenant, &namespace, &stream, &GroupName::new("workers")),
        "group:tenant-a/payments/orders.v1/workers"
    );
}
