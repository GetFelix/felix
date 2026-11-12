use super::ConditionalTrim;

#[test]
fn a_trim_has_an_offset_and_a_floor_and_no_tail() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Trimmed {
        offset: Some(12),
        oldest: 9,
    });
    assert!(answer.trimmed);
    assert_eq!(
        (answer.offset, answer.oldest, answer.tail),
        (Some(12), Some(9), None)
    );
}

#[test]
fn a_trim_that_wrote_nothing_has_a_floor_and_no_offset() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Trimmed {
        offset: None,
        oldest: 4,
    });
    assert!(answer.trimmed);
    assert_eq!(
        (answer.offset, answer.oldest, answer.tail),
        (None, Some(4), None)
    );
}

#[test]
fn a_stale_tail_is_an_answer_with_the_tail() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Refused { tail: 31 });
    assert!(!answer.trimmed);
    assert_eq!(
        (answer.offset, answer.oldest, answer.tail),
        (None, None, Some(31))
    );
}

#[test]
fn a_lease_token_never_prints_its_secret() {
    let token = super::LeaseToken {
        token: "s3cret-bearer".to_string(),
        expires_at: 1_900_000_000,
    };
    let shown = token.__repr__();
    assert!(!shown.contains("s3cret"), "{shown}");
    assert!(shown.contains("1900000000"));
}
