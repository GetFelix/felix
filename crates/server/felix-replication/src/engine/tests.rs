#[test]
fn written_ends_one_past_the_batch() {
    let outcome = felix_broker::PublishOutcome {
        subscribers: 0,
        offsets: Some((3, 6)),
    };
    assert_eq!(
        super::Written::of(&outcome, 4),
        Some(super::Written {
            end: 7,
            generation: 4
        })
    );
    let ephemeral = felix_broker::PublishOutcome {
        subscribers: 0,
        offsets: None,
    };
    assert_eq!(super::Written::of(&ephemeral, 4), None);
}
