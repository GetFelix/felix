//! The live subscribers: one per list for the whole run, recording every
//! event delivered to it, for rules 9 to 11.
//!
//! A subscriber rides out moves and lost connections inside its
//! `ClusterSubscription`. When that gives up or the broker ends it, the
//! subscriber connects a fresh client from the current address book, since a
//! restarted broker listens on new ports, and resumes after the last offset
//! it was delivered. Nothing here draws from the seed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_client::StartPosition;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::model::{Element, Resume, Subscription};
use super::workload::{Workload, decode};
use crate::{Cluster, client};

/// How long connecting and subscribing may each take before the attempt is
/// abandoned and retried.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The pause between a session ending and the next attempt.
const BACKOFF: Duration = Duration::from_millis(250);

/// How often the catch-up wait looks at the subscribers' progress.
const POLL: Duration = Duration::from_millis(100);

/// How long a stopped subscriber has to wind down before it is aborted.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// The running subscribers. Dropping this stops them.
pub(super) struct Subscribers {
    running: Vec<Running>,
    stop: CancellationToken,
}

impl Subscribers {
    /// Start one subscriber per `(list, base)`, each from its list's base.
    pub(super) fn start(
        cluster: &Cluster,
        workload: &Arc<Workload>,
        lists: impl IntoIterator<Item = (String, u64)>,
    ) -> Self {
        let stop = CancellationToken::new();
        let target = Arc::new(Target {
            tenant_id: cluster.tenant_id.clone(),
            namespace: cluster.namespace.clone(),
            token: cluster.client_token(),
        });
        let running = lists
            .into_iter()
            .enumerate()
            .map(|(subscriber, (list, base))| {
                let record = Arc::new(Mutex::new(Subscription {
                    subscriber,
                    list,
                    start: base,
                    delivered: Vec::new(),
                    resumes: Vec::new(),
                    next: base,
                }));
                let follower = Follower {
                    target: Arc::clone(&target),
                    workload: Arc::clone(workload),
                    record: Arc::clone(&record),
                    opened: false,
                    ended: None,
                };
                let task = tokio::spawn(follower.run(stop.clone()));
                Running { record, task }
            })
            .collect();
        Self { running, stop }
    }

    /// Wait until every subscriber has been delivered past the offset `ends`
    /// gives for its list, or until `timeout`. Falling short is not an error:
    /// the record shows it and the checker reports it.
    pub(super) async fn catch_up(&self, ends: &BTreeMap<String, u64>, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            let behind = self.running.iter().any(|running| {
                let record = running.record.lock().expect("subscription lock");
                ends.get(&record.list)
                    .is_some_and(|&end| record.next <= end)
            });
            if !behind {
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Stop every subscriber and return what each was delivered.
    pub(super) async fn stop(mut self) -> Vec<Subscription> {
        self.stop.cancel();
        let mut subscriptions = Vec::new();
        for running in std::mem::take(&mut self.running) {
            let mut task = running.task;
            if tokio::time::timeout(STOP_TIMEOUT, &mut task).await.is_err() {
                task.abort();
            }
            subscriptions.push(running.record.lock().expect("subscription lock").clone());
        }
        subscriptions
    }
}

impl Drop for Subscribers {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// One subscriber's task and the record it writes to.
struct Running {
    record: Arc<Mutex<Subscription>>,
    task: JoinHandle<()>,
}

/// Who the subscribers connect as.
struct Target {
    tenant_id: String,
    namespace: String,
    token: String,
}

/// One subscriber's loop over sessions.
struct Follower {
    target: Arc<Target>,
    workload: Arc<Workload>,
    record: Arc<Mutex<Subscription>>,
    /// Whether a session has opened yet, so the first is not a resume.
    opened: bool,
    /// Why the last session that opened ended, until the next one opens.
    ended: Option<String>,
}

impl Follower {
    async fn run(mut self, stop: CancellationToken) {
        loop {
            let reason = tokio::select! {
                () = stop.cancelled() => return,
                reason = self.session() => reason,
            };
            if self.opened && self.ended.is_none() {
                self.ended = Some(reason);
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(BACKOFF) => {}
            }
        }
    }

    /// Connect, subscribe after the last delivered offset, and record events
    /// until the subscription fails or ends. Returns why it stopped.
    async fn session(&mut self) -> String {
        let (list, from) = {
            let record = self.record.lock().expect("subscription lock");
            (record.list.clone(), record.next)
        };
        let target = &self.target;
        let addrs = self.workload.addrs();
        let connected = tokio::time::timeout(
            CONNECT_TIMEOUT,
            client::connect_cluster(&addrs, &target.tenant_id, &target.token),
        )
        .await;
        let cluster = match connected {
            Ok(Ok(cluster)) => Arc::new(cluster),
            Ok(Err(err)) => return format!("connect: {err:#}"),
            Err(_) => return "connecting timed out".to_string(),
        };
        let subscribed = tokio::time::timeout(
            CONNECT_TIMEOUT,
            cluster.subscribe_from(
                &target.tenant_id,
                &target.namespace,
                &list,
                Some(StartPosition::Offset(from)),
            ),
        )
        .await;
        let mut subscription = match subscribed {
            Ok(Ok(subscription)) => subscription,
            Ok(Err(err)) => return format!("subscribe from offset {from}: {err:#}"),
            Err(_) => return format!("subscribing from offset {from} timed out"),
        };

        {
            let mut record = self.record.lock().expect("subscription lock");
            if self.opened {
                let reason = self.ended.take().unwrap_or_else(|| "unknown".to_string());
                let resume = Resume {
                    at: self.workload.now(),
                    from,
                    first: record.delivered.len(),
                    reason,
                };
                record.resumes.push(resume);
            }
            self.opened = true;
        }

        loop {
            let event = match subscription.next_event().await {
                Ok(Some(event)) => event,
                Ok(None) => return "the broker ended the subscription".to_string(),
                Err(err) => return format!("the subscription failed: {err:#}"),
            };
            let Some(offset) = event.offset else {
                return "a record arrived without an offset".to_string();
            };
            let mut record = self.record.lock().expect("subscription lock");
            record.delivered.push(Element {
                offset,
                value: decode(&event.payload),
                skipped_before: event.skipped_before,
            });
            record.next = offset + 1;
        }
    }
}
