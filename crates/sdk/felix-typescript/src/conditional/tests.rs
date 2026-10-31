use napi::bindgen_prelude::Either;

use super::ConditionalTrim;

fn big(value: &Either<napi::bindgen_prelude::BigInt, napi::bindgen_prelude::Null>) -> Option<u64> {
    match value {
        Either::A(n) => Some(n.get_u64().1),
        Either::B(_) => None,
    }
}

fn fields(answer: &ConditionalTrim) -> (bool, Option<u64>, Option<u64>, Option<u64>) {
    (
        answer.trimmed,
        big(&answer.offset),
        big(&answer.oldest),
        big(&answer.tail),
    )
}

#[test]
fn a_trim_has_an_offset_and_a_floor_and_no_tail() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Trimmed {
        offset: Some(12),
        oldest: 9,
    });
    assert_eq!(fields(&answer), (true, Some(12), Some(9), None));
}

#[test]
fn a_trim_that_wrote_nothing_has_a_floor_and_no_offset() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Trimmed {
        offset: None,
        oldest: 4,
    });
    assert_eq!(fields(&answer), (true, None, Some(4), None));
}

#[test]
fn a_stale_tail_is_an_answer_with_the_tail() {
    let answer = ConditionalTrim::from(felix_client::ConditionalTrim::Refused { tail: 31 });
    assert_eq!(fields(&answer), (false, None, None, Some(31)));
}
