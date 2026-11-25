use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_client::Event;

use super::{Events, drain, payloads};

/// What a subscription does next.
enum Step {
    Event(&'static str),
    /// The broker reset the event stream: it stopped serving the shard.
    Lost,
    /// The broker closed the event stream.
    Ended,
}

/// A subscription that plays `steps` and then stays quiet, as a live one
/// does once it has caught up.
struct Scripted(VecDeque<Step>);

impl Scripted {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self(steps.into_iter().collect())
    }
}

impl Events for Scripted {
    async fn next_event(&mut self) -> Result<Option<Event>> {
        match self.0.pop_front() {
            Some(Step::Event(payload)) => Ok(Some(event(payload))),
            Some(Step::Lost) => Err(anyhow!("stream reset by peer: error 1")),
            Some(Step::Ended) => Ok(None),
            None => std::future::pending().await,
        }
    }
}

fn event(payload: &'static str) -> Event {
    Event {
        tenant_id: Arc::from("t1"),
        namespace: Arc::from("ns"),
        stream: Arc::from("orders"),
        payload: Bytes::from_static(payload.as_bytes()),
        offset: None,
        skipped_before: 0,
        publisher: None,
        timestamp_micros: None,
        headers: None,
    }
}

const QUIET: Duration = Duration::from_secs(2);

/// **A replay that goes quiet is complete.**
#[tokio::test(start_paused = true)]
async fn a_replay_that_goes_quiet_is_read_to_its_end() {
    let mut replay = Scripted::new([Step::Event("harness-probe"), Step::Event("before-1")]);
    let read = drain(&mut replay, QUIET).await.expect("a whole replay");
    assert_eq!(payloads(&read), ["harness-probe", "before-1"]);
}

/// **A subscription the broker reset part way is not a replay.** This is
/// #3112: a follower that could not pass its read barrier for the second
/// page of history reset the stream after `harness-probe`, and the test read
/// that prefix as everything the broker held.
#[tokio::test(start_paused = true)]
async fn a_subscription_lost_part_way_is_not_a_replay() {
    let mut replay = Scripted::new([Step::Event("harness-probe"), Step::Lost]);
    let err = drain(&mut replay, QUIET)
        .await
        .map(|read| payloads(&read))
        .expect_err("a prefix");
    assert!(
        format!("{err:#}").contains("lost after 1 events"),
        "{err:#}"
    );
}

/// **Nor is one the broker ended**, as a deposed leader ends its readers.
#[tokio::test(start_paused = true)]
async fn a_subscription_ended_part_way_is_not_a_replay() {
    let mut replay = Scripted::new([Step::Event("harness-probe"), Step::Ended]);
    let err = drain(&mut replay, QUIET)
        .await
        .map(|read| payloads(&read))
        .expect_err("a prefix");
    assert!(
        format!("{err:#}").contains("ended the subscription"),
        "{err:#}"
    );
}
