use super::*;

#[test]
fn a_publisher_that_was_acknowledged_nothing_fails_the_run() {
    let err = Published {
        acknowledged: Vec::new(),
        stopped_by: "its publisher did not open: refused".to_string(),
    }
    .acknowledged_some("Quorum")
    .unwrap_err();
    let said = err.to_string();
    assert!(said.contains("no publish was acknowledged"), "{said}");
    assert!(said.contains("its publisher did not open"), "{said}");
}

#[test]
fn a_publisher_that_was_acknowledged_something_carries_on() {
    let published = Published {
        acknowledged: vec![Record::new(Some(1), "Quorum-0001")],
        stopped_by: "Quorum-0002 went unanswered".to_string(),
    };
    assert_eq!(published.acknowledged_some("Quorum").unwrap().len(), 1);
}
