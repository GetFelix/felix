use super::*;

#[test]
fn a_marked_error_exits_with_its_status() {
    let err = fail(Exit::NotFound, "no such key");
    assert_eq!(exit_for(&err), Exit::NotFound);
    assert_eq!(err.to_string(), "no such key");
}

#[test]
fn the_outermost_mark_wins() {
    let inner: anyhow::Result<()> = Err(fail(Exit::Server, "refused"));
    let err = inner.mark(Exit::Connection, "connect").unwrap_err();
    assert_eq!(exit_for(&err), Exit::Connection);
}

#[test]
fn a_mark_survives_further_context() {
    let err = fail(Exit::Usage, "no brokers").context("pub");
    assert_eq!(exit_for(&err), Exit::Usage);
}

#[test]
fn a_broker_refusal_is_a_server_error() {
    let refusal = SubscribeCursorError {
        reason: felix_client::CursorErrorReason::TooOld,
        requested: 1,
        available: 10,
    };
    let err = anyhow::Error::new(refusal).context("publish");
    assert_eq!(exit_for(&err), Exit::Server);
}

#[test]
fn anything_else_is_a_plain_failure() {
    let err = anyhow::anyhow!("something broke");
    assert_eq!(exit_for(&err), Exit::Failure);
}

#[test]
fn statuses_are_stable() {
    assert_eq!(
        [
            Exit::Failure,
            Exit::Usage,
            Exit::Connection,
            Exit::Server,
            Exit::NotFound
        ]
        .map(Exit::code),
        [1, 2, 3, 4, 5]
    );
}
