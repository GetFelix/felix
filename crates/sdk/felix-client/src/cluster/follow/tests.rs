use felix_wire::StartPosition;

use super::resume_position;

#[test]
fn resumes_past_whichever_is_further() {
    assert_eq!(
        resume_position(Some(9), Some(8), None),
        StartPosition::Offset(10)
    );
    assert_eq!(
        resume_position(Some(4), Some(8), None),
        StartPosition::Offset(8)
    );
}

#[test]
fn resumes_at_the_broker_position_when_nothing_was_delivered() {
    assert_eq!(
        resume_position(None, Some(8), None),
        StartPosition::Offset(8)
    );
}

#[test]
fn resumes_after_the_last_delivered_without_a_broker_position() {
    assert_eq!(
        resume_position(Some(4), None, None),
        StartPosition::Offset(5)
    );
}

#[test]
fn resumes_at_the_tail_with_no_known_start() {
    assert_eq!(resume_position(None, None, None), StartPosition::Latest);
}

#[test]
fn resumes_where_it_started_with_nothing_delivered_and_no_broker_position() {
    // Latest would skip whatever was written while the shard was re-established.
    assert_eq!(
        resume_position(None, None, Some(17)),
        StartPosition::Offset(17)
    );
}

#[test]
fn delivered_records_and_a_broker_position_outrank_the_start() {
    assert_eq!(
        resume_position(Some(9), None, Some(3)),
        StartPosition::Offset(10)
    );
    assert_eq!(
        resume_position(None, Some(12), Some(3)),
        StartPosition::Offset(12)
    );
}
