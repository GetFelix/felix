use felix_broker::BrokerError;
use kafka_protocol::ResponseError;

use super::from_broker;

#[test]
fn read_errors_map_to_what_a_client_should_do_next() {
    let cases = [
        (
            BrokerError::CursorTooOld {
                oldest: 10,
                requested: 2,
                trim_headers: None,
            },
            ResponseError::OffsetOutOfRange,
        ),
        (
            BrokerError::StreamNotFound {
                tenant_id: "t".into(),
                namespace: "n".into(),
                stream: "s".into(),
            },
            ResponseError::UnknownTopicOrPartition,
        ),
        (
            BrokerError::StreamHandleInactive(7),
            ResponseError::NotLeaderOrFollower,
        ),
        (
            BrokerError::Storage("disk".into()),
            ResponseError::KafkaStorageError,
        ),
    ];
    for (err, expected) in cases {
        assert_eq!(from_broker(&err), expected, "{err}");
    }
}
