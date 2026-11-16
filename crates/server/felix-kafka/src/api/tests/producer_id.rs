use kafka_protocol::messages::{
    EndTxnRequest, FindCoordinatorRequest, InitProducerIdRequest, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;

use super::Fixture;
use crate::api::transactions::MESSAGE;

#[tokio::test]
async fn each_idempotent_producer_gets_its_own_id_at_epoch_zero() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let mut ids = Vec::new();
    for version in [0, 4] {
        let response = client
            .call(&InitProducerIdRequest::default(), version)
            .await;
        assert_eq!(response.error_code, 0);
        assert!(response.producer_id.0 > 0);
        // Fits the bits a Felix producer id keeps beside the epoch.
        assert!(response.producer_id.0 as u64 <= crate::api::producer_id::KAFKA_ID_MASK);
        assert_eq!(response.producer_epoch, 0);
        ids.push(response.producer_id.0);
    }
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn an_unauthenticated_connection_gets_no_producer_id() {
    let fixture = Fixture::secured(&[]).await;
    let mut client = fixture.connect();
    let response = client.call(&InitProducerIdRequest::default(), 4).await;
    assert_eq!(response.error_code, 31, "CLUSTER_AUTHORIZATION_FAILED");
}

/// A transactional producer is refused at each step it could reach: its
/// coordinator lookup (with a message), its producer id, and the
/// transaction APIs.
#[tokio::test]
async fn transactions_are_refused_everywhere_a_producer_could_start_one() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();

    let lookup = client
        .call(
            &FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("tx"))
                .with_key_type(1),
            2,
        )
        .await;
    assert_eq!(
        lookup.error_code, 53,
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
    );
    assert_eq!(
        lookup.error_message.as_ref().map(|m| m.as_str()),
        Some(MESSAGE)
    );

    let init = client
        .call(
            &InitProducerIdRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("tx")))),
            4,
        )
        .await;
    assert_eq!(init.error_code, 53);
    assert_eq!(init.producer_id.0, -1);

    let end = client
        .call(
            &EndTxnRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("tx"))),
            3,
        )
        .await;
    assert_eq!(end.error_code, 53);
}
