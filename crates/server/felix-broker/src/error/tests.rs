use super::BrokerError;

#[tokio::test]
async fn broker_error_display() {
    let err = BrokerError::CapacityTooLarge;
    assert!(err.to_string().contains("capacity"));

    let err = BrokerError::CursorTooOld {
        oldest: 10,
        requested: 5,
    };
    assert!(err.to_string().contains("10"));
    assert!(err.to_string().contains("5"));

    let err = BrokerError::TenantNotFound("t1".to_string());
    assert!(err.to_string().contains("t1"));

    let err = BrokerError::NamespaceNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));

    let err = BrokerError::StreamNotFound {
        tenant_id: "t1".to_string(),
        namespace: "ns1".to_string(),
        stream: "s1".to_string(),
    };
    assert!(err.to_string().contains("t1"));
    assert!(err.to_string().contains("ns1"));
    assert!(err.to_string().contains("s1"));
}

#[test]
fn a_full_disk_stays_distinct_from_other_storage_failures() {
    let full =
        felix_storage::StorageError::Full(std::io::Error::from(std::io::ErrorKind::StorageFull));
    assert!(matches!(
        BrokerError::from(full),
        BrokerError::StorageFull(_)
    ));
    let other = felix_storage::StorageError::Io(std::io::Error::other("eio"));
    assert!(matches!(BrokerError::from(other), BrokerError::Storage(_)));
}
