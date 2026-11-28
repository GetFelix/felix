//! What a broker says an acknowledged publish means, against a stub.

use anyhow::Result;
use felix_wire::{AckCopies, AckDurability, AckFsync, AckPoint, Message};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::Client;

async fn client_of(broker_says: Option<AckDurability>) -> Result<(StubBroker, Client)> {
    let (broker, cert) = StubBroker::start(|_| Message::Ok)?;
    if let Some(said) = broker_says {
        broker.say_ack_durability(said);
    }
    let cluster =
        Client::connect_with_policy(&[broker.addr], "localhost", default_config(cert)?, policy())
            .await?;
    Ok((broker, cluster))
}

#[tokio::test]
#[serial_test::serial]
async fn the_client_reports_what_the_broker_says() -> Result<()> {
    let said = AckDurability {
        ack_at: AckPoint::Write,
        copies: AckCopies::Quorum,
        fsync: Some(AckFsync::OnCommit),
    };
    let (_broker, cluster) = client_of(Some(said)).await?;
    assert_eq!(
        cluster
            .stream_ack_durability("t1", "default", "orders")
            .await?,
        Some(said)
    );
    Ok(())
}
