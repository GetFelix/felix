use super::{CommitError, CommitOp, prepare};

#[test]
fn a_commit_on_one_stream_is_prepared() {
    let prepared = prepare(vec![
        CommitOp::enqueue("orders", "placed"),
        CommitOp::put("orders", "order-1", "placed"),
        CommitOp::delete("orders", "cart-1"),
    ])
    .expect("prepared");
    assert_eq!(prepared.stream, "orders");
    assert_eq!(prepared.event, "placed");
    assert_eq!(prepared.changes.len(), 2);
}

/// **An operation on another stream is refused, never split off.** Another
/// stream is another log, and writing it separately would be exactly the
/// partial commit this API exists to rule out.
#[test]
fn an_operation_on_another_stream_is_refused() {
    let refused = prepare(vec![
        CommitOp::publish("orders", "placed"),
        CommitOp::put("inventory", "sku-1", "3"),
    ])
    .expect_err("refused");
    assert_eq!(
        refused,
        CommitError::NotOnOwningShard {
            index: 1,
            stream: "inventory".to_owned(),
            owner: "orders".to_owned(),
        }
    );
}

#[test]
fn a_commit_needs_exactly_one_event() {
    assert_eq!(
        prepare(vec![CommitOp::put("orders", "k", "v")]).expect_err("none"),
        CommitError::EventCount(0)
    );
    assert_eq!(
        prepare(vec![
            CommitOp::publish("orders", "a"),
            CommitOp::enqueue("orders", "b"),
        ])
        .expect_err("two"),
        CommitError::EventCount(2)
    );
}
