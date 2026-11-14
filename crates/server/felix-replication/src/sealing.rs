//! The leader's side of a seal order: the control plane told this broker to
//! seal a shard it leads, so the shard can move to Raft.
//!
//! A shard under a seal order is closed. This task runs its
//! [`SealRound`](crate::seal::SealRound) across passes, claims only once the
//! shard is quiet, sends the one report, and tells the control plane when
//! the round is given up. It never reopens the shard: only a later
//! generation from the control plane does. A round that reaches no majority
//! by its deadline is given up too, so a closed shard is not left waiting
//! on copies that will not answer. See "The seal on the hybrid side" in
//! `docs/per-shard-raft.md`.
//!
//! A build without the Raft engine cannot seal. It keeps the shard closed
//! and gives every order up, so the control plane moves on.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_common::random::HashMap;
use felix_router::ShardRouter;
use felix_storage::StorageId;
use futures::StreamExt;
use serde::Deserialize;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::controlplane_link::ControlPlaneLink;
use crate::peer::PeerRequester;
use crate::reporter::Credential;

/// How soon a pass follows one that left a round short of its report. The
/// shard is closed meanwhile, so short; each pass already waits on the
/// copies it asks.
const SEAL_RETRY: Duration = Duration::from_millis(50);

/// How soon a report or a give-up the control plane did not answer is sent
/// again.
const SEND_RETRY: Duration = Duration::from_millis(500);

/// How long a round may run without a majority holding the seal's record.
/// A healthy round ends within a pass or two; this only has to outlast a
/// follower catching up to the record.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// The shortest deadline taken from the environment: less would give up
/// rounds that only needed a second pass.
const MIN_DEADLINE: Duration = Duration::from_secs(1);

/// How many rounds run at once in a pass, so one shard's slow copy does not
/// hold up the others.
const CONCURRENT_ROUNDS: usize = 8;

/// How a seal leader paces its rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealPace {
    /// How soon a round short of its report runs again.
    pub retry: Duration,
    /// How soon a report or a give-up the control plane did not answer is
    /// sent again.
    pub resend: Duration,
    /// How long a round may go, from when this broker first saw its order,
    /// without a majority holding the seal's record. Past it the round is
    /// given up, so the control plane promotes a later generation instead
    /// of leaving the shard closed.
    pub deadline: Duration,
}

impl Default for SealPace {
    fn default() -> Self {
        Self {
            retry: SEAL_RETRY,
            resend: SEND_RETRY,
            deadline: DEFAULT_DEADLINE,
        }
    }
}

impl SealPace {
    /// The default pace, with the deadline from `FELIX_SEAL_DEADLINE_MS`
    /// when it is set, at least one second.
    pub fn from_env() -> Self {
        let deadline = std::env::var("FELIX_SEAL_DEADLINE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map_or(DEFAULT_DEADLINE, |millis| {
                Duration::from_millis(millis).max(MIN_DEADLINE)
            });
        Self {
            deadline,
            ..Self::default()
        }
    }
}

/// A seal the control plane ordered on a shard this broker leads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealOrder {
    pub key: crate::ShardKey,
    /// The seal's generation, which the shard's assignment is at.
    pub generation: u64,
    /// This process was serving the shard at the generation right before
    /// the seal's, so its copy holds every acknowledged record. A leader
    /// that was not would have to take a follower's tail first, which a
    /// seal does not do: it gives the order up.
    pub continues: bool,
}

/// The seal orders in force, and the write fence of the shards they close.
/// The broker's shard fence answers it.
pub trait SealOrders: Send + Sync {
    /// Every shard this broker leads under a seal order.
    fn orders(&self) -> Vec<SealOrder>;

    /// Whether `key` is closed with no client write still inside its fence.
    fn quiesced(&self, key: &crate::ShardKey) -> bool;
}

/// Where a seal leader reports, and as whom.
pub struct SealTo<L> {
    pub link: L,
    pub node_id: String,
    pub token: Option<Arc<dyn Credential>>,
    /// Until cancelled, nothing is sent: as for replica reports, a broker
    /// whose data directory the control plane has not confirmed announces
    /// no storage id.
    pub held: Option<CancellationToken>,
    /// This broker's copy id. `None` without durable storage, which leaves
    /// nothing to seal.
    pub storage_id: Option<StorageId>,
}

/// Where one order's round stands.
enum Stage {
    /// Not begun: the promise on this copy, then the round.
    Starting,
    #[cfg(feature = "raft")]
    Sealing(Box<crate::seal::SealRound>),
    /// A majority holds the record. The report is kept until the control
    /// plane answers it, with how often it was refused; the round is never
    /// run again.
    Reporting(serde_json::Value, u32),
    /// Given up, for this reason, until the control plane hears it.
    GivingUp(String),
    /// The control plane answered. Nothing more happens at this generation.
    Done,
}

impl Stage {
    /// Whether the round is still short of its report: the stages the
    /// deadline ends.
    fn short(&self) -> bool {
        match self {
            Stage::Starting => true,
            #[cfg(feature = "raft")]
            Stage::Sealing(_) => true,
            Stage::Reporting(..) | Stage::GivingUp(_) | Stage::Done => false,
        }
    }
}

enum Step {
    /// Wait for the next pass.
    Stay(Stage),
    /// Moved on: carry on in this pass.
    Next(Stage),
}

struct Round {
    generation: u64,
    stage: Stage,
    /// When this broker first saw the order. The deadline counts from it.
    since: Instant,
    /// When the round next has something to do: `None` once it is done.
    /// Each round has its own, so one that retries often does not make
    /// another resend early.
    due: Option<Instant>,
}

#[derive(Deserialize)]
struct ReportAnswer {
    accepted: bool,
}

#[derive(Deserialize)]
struct GiveUpAnswer {
    given_up: bool,
}

/// Every seal order's round on this broker.
pub struct Sealing<R, L> {
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    requester: Arc<R>,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    broker: Arc<Broker>,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    router: Arc<ShardRouter>,
    orders: Arc<dyn SealOrders>,
    to: SealTo<L>,
    pace: SealPace,
    /// One round per shard and generation. Kept, once it has reported or
    /// given up, for as long as the order stands, so nothing runs twice.
    rounds: HashMap<crate::ShardKey, Round>,
}

impl<R: PeerRequester + Send + Sync, L: ControlPlaneLink> Sealing<R, L> {
    pub fn new(
        requester: Arc<R>,
        broker: Arc<Broker>,
        router: Arc<ShardRouter>,
        orders: Arc<dyn SealOrders>,
        to: SealTo<L>,
    ) -> Self {
        Self {
            requester,
            broker,
            router,
            orders,
            to,
            pace: SealPace::default(),
            rounds: HashMap::default(),
        }
    }

    pub fn with_pace(mut self, pace: SealPace) -> Self {
        self.pace = pace;
        self
    }

    /// One pass over every order whose round is due. Returns how soon the
    /// next is wanted, or `None` when no round has work left.
    pub async fn pass(&mut self) -> Option<Duration> {
        let orders = self.orders.orders();
        self.rounds.retain(|key, round| {
            orders
                .iter()
                .any(|order| order.key == *key && order.generation == round.generation)
        });
        let now = Instant::now();
        let mut due = Vec::new();
        for order in orders {
            let waits = self
                .rounds
                .get(&order.key)
                .is_some_and(|round| round.due.is_none_or(|due| due > now));
            if waits {
                continue;
            }
            let round = self.rounds.remove(&order.key).unwrap_or(Round {
                generation: order.generation,
                stage: Stage::Starting,
                since: now,
                due: Some(now),
            });
            due.push((order, round));
        }
        let advanced: Vec<(crate::ShardKey, Round)> = {
            let this = &*self;
            let running: Vec<_> = due
                .into_iter()
                .map(|(order, round)| this.advance(order, round))
                .collect();
            futures::stream::iter(running)
                .buffer_unordered(CONCURRENT_ROUNDS)
                .collect()
                .await
        };
        self.rounds.extend(advanced);
        let now = Instant::now();
        self.rounds
            .values()
            .filter_map(|round| round.due)
            .map(|due| due.saturating_duration_since(now))
            .min()
    }

    /// Run one round as far as it goes now.
    async fn advance(&self, order: SealOrder, round: Round) -> (crate::ShardKey, Round) {
        let Round {
            mut stage, since, ..
        } = round;
        loop {
            let step = match stage {
                Stage::Starting => self.start(&order).await,
                #[cfg(feature = "raft")]
                Stage::Sealing(round) => self.seal(&order, round).await,
                Stage::Reporting(report, refused) => self.report(&order, report, refused).await,
                Stage::GivingUp(reason) => self.give_up(&order, reason).await,
                Stage::Done => Step::Stay(Stage::Done),
            };
            match step {
                Step::Next(next) => stage = next,
                // Checked after the pass, so a round that reaches its
                // majority on the pass that crosses the deadline reports.
                Step::Stay(stayed) if stayed.short() && since.elapsed() >= self.pace.deadline => {
                    stage = Stage::GivingUp(format!(
                        "no majority held the seal's record within {:?}",
                        self.pace.deadline
                    ));
                }
                Step::Stay(stayed) => {
                    stage = stayed;
                    break;
                }
            }
        }
        let due = match stage {
            Stage::Done => None,
            Stage::Reporting(..) | Stage::GivingUp(_) => Some(Instant::now() + self.pace.resend),
            _ => Some(Instant::now() + self.pace.retry),
        };
        let round = Round {
            generation: order.generation,
            stage,
            since,
            due,
        };
        (order.key, round)
    }

    #[cfg(not(feature = "raft"))]
    async fn start(&self, order: &SealOrder) -> Step {
        let _ = order;
        Step::Next(Stage::GivingUp(
            "this broker's build has no Raft engine".to_string(),
        ))
    }

    /// Take this copy's promise, wait out its own side writes, and build
    /// the round from where its logs then start.
    #[cfg(feature = "raft")]
    async fn start(&self, order: &SealOrder) -> Step {
        use crate::seal::{LogStarts, SealRound};

        let give_up = |why: &str| Step::Next(Stage::GivingUp(why.to_string()));
        if !order.continues {
            return give_up(
                "this broker was not serving the generation before the seal's, so its copy may \
                 lack an acknowledged record",
            );
        }
        if order.key.kind != crate::ShardKind::Stream {
            return give_up("only a stream shard is sealed");
        }
        let Some(copy) = self.to.storage_id else {
            return give_up("this broker has no storage identity to seal under");
        };
        let Some(route) = self.led(order) else {
            return Step::Stay(Stage::Starting);
        };
        let Some([log, dead_letters, cursors]) = self.logs(&order.key).await else {
            return give_up("this broker holds no dead-letter or cursor log for the shard");
        };
        let own = (self.to.node_id.clone(), copy);
        match SealRound::promise(log.disk_log(), order.generation, &own).await {
            Ok(Ok(())) => {}
            Ok(Err(why)) => return give_up(&format!("{why:?}")),
            Err(err) => {
                tracing::warn!(stream = %order.key.stream, shard = order.key.shard, error = %err,
                    "could not promise the seal on this copy; trying again");
                return Step::Stay(Stage::Starting);
            }
        }
        // Under the promise nothing local starts on the side logs. Whatever
        // began before it must end before their starts are read.
        if !self.side_logs_quiet(&order.key) {
            return Step::Stay(Stage::Starting);
        }
        let starts = LogStarts {
            log: log.disk_log().base_offset(),
            dead_letters: dead_letters.disk_log().base_offset(),
            cursors: cursors.disk_log().base_offset(),
        };
        let set = route.replicas.iter().map(|node| node.node_id.clone());
        Step::Next(Stage::Sealing(Box::new(SealRound::new(
            order.generation,
            set,
            own,
            starts,
        ))))
    }

    /// One pass of the round.
    #[cfg(feature = "raft")]
    async fn seal(&self, order: &SealOrder, mut round: Box<crate::seal::SealRound>) -> Step {
        use crate::seal::{SealLogs, SealProgress};

        let Some(route) = self.led(order) else {
            return Step::Stay(Stage::Sealing(round));
        };
        let Some([log, dead_letters, cursors]) = self.logs(&order.key).await else {
            return Step::Stay(Stage::Sealing(round));
        };
        let local = self.router.local_node_id();
        let peers: Vec<(String, std::net::SocketAddr)> = route
            .replicas
            .iter()
            .filter(|node| node.node_id != local)
            .map(|node| (node.node_id.clone(), node.advertise_addr))
            .collect();
        // Quiet on every side: no client write inside the fence, no commit,
        // dead letter or compaction pass under way, and the promise that
        // refuses new ones still held.
        let quiesced = self.orders.quiesced(&order.key)
            && log.disk_log().holds_seal()
            && self.side_logs_quiet(&order.key);
        let shard =
            crate::incarnation::shard_ref(&self.broker, &router_key(&order.key), order.generation);
        let logs = SealLogs {
            log: log.disk_log(),
            dead_letters: dead_letters.disk_log(),
            cursors: cursors.disk_log(),
        };
        match round
            .step(self.requester.as_ref(), &shard, &peers, logs, quiesced)
            .await
        {
            Ok(SealProgress::Waiting) => Step::Stay(Stage::Sealing(round)),
            Ok(SealProgress::Reported(report)) => {
                tracing::info!(
                    stream = %order.key.stream,
                    shard = order.key.shard,
                    generation = order.generation,
                    offset = report.base.offset,
                    voters = report.voters.len(),
                    "sealed: a majority holds the seal's record",
                );
                Step::Next(Stage::Reporting(report_body(&order.key, &report), 0))
            }
            Ok(SealProgress::Refused(why)) => Step::Next(Stage::GivingUp(format!("{why:?}"))),
            Ok(SealProgress::Abandoned) => {
                Step::Next(Stage::GivingUp("the round was abandoned".to_string()))
            }
            // A failure after the claim's mark shows as a claim already
            // made on the next pass, which gives the round up.
            Err(err) => {
                tracing::warn!(stream = %order.key.stream, shard = order.key.shard, error = %err,
                    "a seal pass failed on this copy's log; trying again");
                Step::Stay(Stage::Sealing(round))
            }
        }
    }

    /// The route, while it still names this broker at the order's
    /// generation.
    #[cfg(feature = "raft")]
    fn led(&self, order: &SealOrder) -> Option<felix_router::Route> {
        let table = self.router.snapshot();
        table
            .get(&router_key(&order.key))
            .filter(|route| {
                route.generation == order.generation
                    && route.leader.node_id == self.router.local_node_id()
            })
            .cloned()
    }

    /// The shard's log, dead letters and cursors.
    #[cfg(feature = "raft")]
    async fn logs(&self, key: &crate::ShardKey) -> Option<[felix_broker::StreamLog; 3]> {
        use felix_broker::LogKind;
        let log = |kind| {
            self.broker
                .shard_log(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
        };
        Some([
            log(LogKind::Stream).await?,
            log(LogKind::GroupDeadLetters).await?,
            log(LogKind::GroupCursors).await?,
        ])
    }

    #[cfg(feature = "raft")]
    fn side_logs_quiet(&self, key: &crate::ShardKey) -> bool {
        self.broker
            .group_logs_quiet(&key.tenant_id, &key.namespace, &key.stream, key.shard)
    }

    fn held(&self) -> bool {
        self.to
            .held
            .as_ref()
            .is_some_and(|held| !held.is_cancelled())
    }

    /// Send the report. A 200 ends the round, accepted or not. A refusal
    /// that may pass (a control plane not yet at the level that takes
    /// reports) is sent again for a while and then given up; anything else
    /// is sent again.
    async fn report(&self, order: &SealOrder, report: serde_json::Value, refused: u32) -> Step {
        if self.held() {
            return Step::Stay(Stage::Reporting(report, refused));
        }
        let bearer = self.to.token.as_ref().map(|token| token.bearer());
        let answer = self
            .to
            .link
            .seal_report(
                bearer.as_ref().map(|bearer| bearer.as_str()),
                &self.to.node_id,
                &report,
            )
            .await;
        match answered::<ReportAnswer>(answer) {
            Answer::Ok(answer) => {
                tracing::info!(
                    stream = %order.key.stream,
                    shard = order.key.shard,
                    generation = order.generation,
                    accepted = answer.accepted,
                    "the control plane answered the seal report",
                );
                Step::Next(Stage::Done)
            }
            Answer::NotSealer => Step::Next(Stage::Done),
            Answer::Conflict if refused + 1 >= REPORT_REFUSALS => Step::Next(Stage::GivingUp(
                "the control plane keeps refusing the seal report".to_string(),
            )),
            Answer::Conflict => Step::Stay(Stage::Reporting(report, refused + 1)),
            Answer::Later => Step::Stay(Stage::Reporting(report, refused)),
        }
    }

    /// Tell the control plane the round is given up, so it promotes a later
    /// generation. The shard stays closed until it does.
    async fn give_up(&self, order: &SealOrder, reason: String) -> Step {
        if self.held() {
            return Step::Stay(Stage::GivingUp(reason));
        }
        let bearer = self.to.token.as_ref().map(|token| token.bearer());
        let body = serde_json::json!({
            "key": order.key,
            "generation": order.generation,
            "reason": reason,
        });
        let answer = self
            .to
            .link
            .seal_give_up(
                bearer.as_ref().map(|bearer| bearer.as_str()),
                &self.to.node_id,
                &body,
            )
            .await;
        match answered::<GiveUpAnswer>(answer) {
            Answer::Ok(answer) => {
                tracing::warn!(
                    stream = %order.key.stream,
                    shard = order.key.shard,
                    generation = order.generation,
                    given_up = answer.given_up,
                    reason,
                    "gave the seal up; the shard stays closed until a later generation",
                );
                Step::Next(Stage::Done)
            }
            // Not this broker's seal to give up: nothing more to say.
            Answer::NotSealer => Step::Next(Stage::Done),
            Answer::Conflict | Answer::Later => Step::Stay(Stage::GivingUp(reason)),
        }
    }
}

/// How many refusals of a report are sent again before the round is given
/// up: about ten seconds at [`SEND_RETRY`].
const REPORT_REFUSALS: u32 = 20;

/// What the control plane said to a report or a give-up.
enum Answer<T> {
    /// A 200 and its body.
    Ok(T),
    /// A 409 `not_sealer`: the switch record names another sealer.
    NotSealer,
    /// Any other 409.
    Conflict,
    /// No answer, another status, or a body that does not parse.
    Later,
}

fn answered<T: serde::de::DeserializeOwned>(
    answer: anyhow::Result<crate::controlplane_link::LinkResponse>,
) -> Answer<T> {
    let Ok(answer) = answer else {
        return Answer::Later;
    };
    match answer.status {
        reqwest::StatusCode::OK => answer.json().map_or(Answer::Later, Answer::Ok),
        reqwest::StatusCode::CONFLICT if answer.text().contains("not_sealer") => Answer::NotSealer,
        reqwest::StatusCode::CONFLICT => Answer::Conflict,
        _ => Answer::Later,
    }
}

#[cfg(feature = "raft")]
fn router_key(key: &crate::ShardKey) -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            crate::ShardKind::Cache => felix_router::ShardKind::Cache,
            crate::ShardKind::Stream => felix_router::ShardKind::Stream,
        },
    }
}

/// The body of `POST /v1/nodes/{node_id}/seal-report`.
#[cfg(feature = "raft")]
fn report_body(key: &crate::ShardKey, report: &crate::seal::SealReport) -> serde_json::Value {
    let hex = |bytes: &[u8]| {
        use std::fmt::Write;
        bytes
            .iter()
            .fold(String::with_capacity(2 * bytes.len()), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            })
    };
    let side = |range: &felix_storage::disk_log::switch::SideRange| {
        serde_json::json!({
            "start": range.start,
            "end": range.end,
            "digest": hex(&range.digest.0),
        })
    };
    let base = &report.base;
    let voters: std::collections::BTreeMap<&str, String> = report
        .voters
        .iter()
        .map(|(node, copy)| (node.as_str(), copy.to_hex()))
        .collect();
    serde_json::json!({
        "key": key,
        "report": {
            "generation": base.generation,
            "offset": base.offset,
            "start": base.start,
            "digest": hex(&base.digest.0),
            "dead_letters": side(&base.dead_letters),
            "cursors": side(&base.cursors),
            "voters": voters,
        },
    })
}

/// How often the orders are read while none has work, at most. A new order
/// closes its shard at once and waits this long for its first pass.
const IDLE_MAX: Duration = Duration::from_millis(200);

/// Run every seal order's round until `shutdown`. `idle` is how often the
/// orders are read while none has work, up to [`IDLE_MAX`].
pub fn spawn<R, L>(
    mut sealing: Sealing<R, L>,
    idle: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()>
where
    R: PeerRequester + Send + Sync + 'static,
    L: ControlPlaneLink,
{
    tokio::spawn(async move {
        loop {
            // Never longer than the idle read, whatever the rounds wait
            // for: a new order must not sit behind a resend.
            let idle = idle.min(IDLE_MAX);
            let wait = sealing.pass().await.map_or(idle, |wait| wait.min(idle));
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    })
}
