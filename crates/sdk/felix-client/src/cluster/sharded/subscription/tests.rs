use super::*;

#[test]
fn a_shard_that_delivered_nothing_resumes_where_it_started() {
    // Latest would skip whatever was written while the shard was re-established.
    assert_eq!(
        resume_after_loss(None, None, Some(17)),
        StartPosition::Offset(17)
    );
}

#[test]
fn a_shard_with_no_known_start_resumes_at_the_tail() {
    assert_eq!(resume_after_loss(None, None, None), StartPosition::Latest);
}

#[test]
fn delivered_records_and_a_moved_shard_still_decide_the_position() {
    assert_eq!(
        resume_after_loss(Some(9), None, Some(3)),
        StartPosition::Offset(10)
    );
    assert_eq!(
        resume_after_loss(None, Some(12), Some(3)),
        StartPosition::Offset(12)
    );
}
