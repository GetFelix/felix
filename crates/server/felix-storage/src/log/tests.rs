use super::*;

/// Exactly the records a client reads are data: events, producer batches
/// and commits. Every other kind is skipped by every client read.
#[test]
fn only_client_records_are_data() {
    let opens = RecordMark::Opens(ProducerBatch {
        producer_id: 1,
        sequence: 0,
        len: 2,
    });
    for mark in [
        RecordMark::None,
        opens,
        RecordMark::Continues,
        RecordMark::Commit,
    ] {
        assert!(mark.is_data(), "{mark:?}");
        assert!(!mark.needs_generation(), "{mark:?}");
    }
    for mark in [RecordMark::GenerationStart, RecordMark::Trim { floor: 3 }] {
        assert!(!mark.is_data(), "{mark:?}");
        assert!(!mark.needs_generation(), "{mark:?}");
    }
    for mark in [
        RecordMark::Membership,
        RecordMark::CounterDelta,
        RecordMark::CounterCheckpoint,
    ] {
        assert!(!mark.is_data(), "{mark:?}");
        assert!(mark.needs_generation(), "{mark:?}");
    }
}
