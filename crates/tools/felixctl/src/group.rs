//! `felixctl group`: create, inspect, move and delete consumer groups, and
//! claim and settle their records by hand.
//!
//! A group lives on each shard of its stream separately, with that shard's
//! leader, so every command here works shard by shard: on the one `--shard`
//! names, or on each in turn. Offsets are per shard, so a record is named by a
//! claim, `SHARD:OFFSET`, with `:ATTEMPTS` added where the broker checks it.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Subcommand};
use felix_client::{GroupInfo, GroupPollOptions, GroupPosition, GroupRecord, StartPosition};
use serde_json::{Value, json};

use crate::cli::Confirm;
use crate::cli::StartArg;
use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, fail};
use crate::manage::ask;
use crate::output::{Output, payload_field, table};

#[derive(Debug, Subcommand)]
pub(crate) enum GroupCommand {
    /// Create a group where its first poll should begin
    #[command(
        long_about = "Create GROUP on every shard of STREAM, or on --shard, so that its \
                      first poll begins at --from instead of the beginning of the log. A \
                      shard where the group already exists is left where it is. Needs the \
                      group.manage permission.",
        after_long_help = "Examples:
  felixctl group create orders billing
  felixctl group create orders billing --from earliest
  felixctl group create orders billing --shard 2 --from 1500"
    )]
    Create {
        #[command(flatten)]
        target: Target,
        /// Where the group starts: `latest`, `earliest`, or an offset (with
        /// --shard, or on a single-shard stream)
        #[arg(long, value_name = "POSITION", default_value = "latest")]
        from: StartArg,
    },
    /// Show where a group stands on each shard
    #[command(
        long_about = "Show, for each shard, the offset GROUP has finished up to, the \
                      shard's tail, the lag between them, records handed out and not \
                      settled, records owed again, and dead letters. In-flight and owed \
                      counts are the leader's memory and start from zero after a leader \
                      change.",
        after_long_help = "Examples:
  felixctl group describe orders billing
  felixctl group describe orders billing --shard 0 --json"
    )]
    Describe {
        #[command(flatten)]
        target: Target,
    },
    /// Move a group's cursor backwards or forwards
    #[command(
        long_about = "Move GROUP's cursor on every shard, or on --shard, to POSITION. \
                      Records the group had handed out are void and are delivered again \
                      from the new position. Dead letters are kept. Moving the cursor \
                      backwards replays finished records, so it asks first on a terminal \
                      and needs --yes elsewhere. Needs the group.manage permission.",
        after_long_help = "Examples:
  felixctl group seek orders billing latest
  felixctl group seek orders billing earliest --yes
  felixctl group seek orders billing 1500 --shard 2"
    )]
    Seek {
        #[command(flatten)]
        target: Target,
        /// Where to move to: `latest`, `earliest`, or an offset (with --shard,
        /// or on a single-shard stream)
        #[arg(value_name = "POSITION")]
        to: StartArg,
        #[command(flatten)]
        confirm: Confirm,
    },
    /// Delete a group
    #[command(
        long_about = "Delete GROUP on every shard, or on --shard: its cursor, its dead \
                      letters and the records it has handed out. A consumer that polls it \
                      again starts afresh. Asks first on a terminal; elsewhere it needs \
                      --yes. Needs the group.manage permission.",
        after_long_help = "Examples:
  felixctl group rm orders billing
  felixctl group rm orders billing --yes"
    )]
    Rm {
        #[command(flatten)]
        target: Target,
        #[command(flatten)]
        confirm: Confirm,
    },
    /// Claim records for a group
    #[command(
        long_about = "Claim up to --max records for GROUP and print each with its claim, \
                      SHARD:OFFSET:ATTEMPTS, which ack, nack, extend and dead-letters add \
                      take. Without --shard each shard is asked in turn until --max \
                      records are claimed. A claim not settled is handed out again once \
                      it lapses.",
        after_long_help = "Examples:
  felixctl group poll orders billing
  felixctl group poll orders billing --max 100 --wait-ms 5000
  felixctl group poll orders billing --shard 1 --visibility-ms 60000 --json"
    )]
    Poll {
        #[command(flatten)]
        target: Target,
        /// Most records to claim
        #[arg(long, value_name = "N", default_value_t = 10,
              value_parser = clap::value_parser!(u32).range(1..))]
        max: u32,
        /// How long the broker may wait for records on each shard it is asked
        #[arg(long, value_name = "MS", default_value_t = 0)]
        wait_ms: u64,
        /// How long the claims stand [default: the broker's visibility timeout]
        #[arg(long, value_name = "MS")]
        visibility_ms: Option<u64>,
    },
    /// Finish claimed records
    #[command(
        long_about = "Finish each claimed record. Claims are acted on in order; one that \
                      fails stops the rest.",
        after_long_help = "Examples:
  felixctl group ack orders billing 0:15:1
  felixctl group ack orders billing 0:15 0:16 1:4"
    )]
    Ack(Claims),
    /// Hand claimed records back
    #[command(
        long_about = "Hand each claimed record back to the group, to be delivered again at \
                      once, or after --delay-ms.",
        after_long_help = "Examples:
  felixctl group nack orders billing 0:15
  felixctl group nack orders billing 0:15 --delay-ms 30000"
    )]
    Nack {
        #[command(flatten)]
        claims: Claims,
        /// Deliver the records again only after this long
        #[arg(long, value_name = "MS")]
        delay_ms: Option<u64>,
    },
    /// Keep a claim standing longer
    #[command(
        long_about = "Keep the claim on a record standing for --for-ms from now, for a \
                      consumer still working on it. The claim needs its attempt count, as \
                      poll prints it: the broker refuses an extension for a record that \
                      has been handed out again since. The broker may cap the time; the \
                      answer says how long the claim now stands.",
        after_long_help = "Examples:
  felixctl group extend orders billing 0:15:1 --for-ms 60000"
    )]
    Extend {
        /// Stream the group reads
        stream: String,
        /// Group name
        group: String,
        /// The claim, SHARD:OFFSET:ATTEMPTS
        claim: Claim,
        /// How long the claim should stand from now
        #[arg(long = "for-ms", value_name = "MS")]
        for_ms: u64,
    },
    /// List, add, redrive and discard dead letters
    #[command(
        subcommand,
        long_about = "Dead letters are records a group gave up on. The records stay in the \
                      log; the group keeps a list of their offsets.",
        after_long_help = "Examples:
  felixctl group dead-letters ls orders billing
  felixctl group dead-letters redrive orders billing 0:15"
    )]
    DeadLetters(DeadLetterCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum DeadLetterCommand {
    /// List a group's dead letters
    #[command(
        long_about = "List the dead letters of GROUP on every shard, or on --shard, as \
                      claims that redrive and discard take.",
        after_long_help = "Examples:
  felixctl group dead-letters ls orders billing
  felixctl group dead-letters ls orders billing --shard 0 --json"
    )]
    Ls {
        #[command(flatten)]
        target: Target,
    },
    /// Give up on claimed records
    #[command(
        long_about = "Give up on each claimed record: finish it and list it as a dead \
                      letter, as the group does once a record runs out of attempts.",
        after_long_help = "Examples:
  felixctl group dead-letters add orders billing 0:15"
    )]
    Add(Claims),
    /// Put dead letters back in the queue
    #[command(
        long_about = "Deliver each dead letter again, its attempt count reset. The group's \
                      cursor does not move.",
        after_long_help = "Examples:
  felixctl group dead-letters redrive orders billing 0:15 1:4"
    )]
    Redrive(Claims),
    /// Stop tracking dead letters
    #[command(
        long_about = "Drop each dead letter from the group's list. The record stays in the \
                      log, but nothing records that it failed any more. Asks first on a \
                      terminal; elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl group dead-letters discard orders billing 0:15 --yes"
    )]
    Discard {
        #[command(flatten)]
        claims: Claims,
        #[command(flatten)]
        confirm: Confirm,
    },
}

/// A group, and the shards a command covers.
#[derive(Debug, Args)]
pub(crate) struct Target {
    /// Stream the group reads
    pub(crate) stream: String,
    /// Group name
    pub(crate) group: String,
    /// Only this shard [default: every shard]
    #[arg(long, value_name = "N")]
    pub(crate) shard: Option<u32>,
}

/// A group and the records a command acts on.
#[derive(Debug, Args)]
pub(crate) struct Claims {
    /// Stream the group reads
    pub(crate) stream: String,
    /// Group name
    pub(crate) group: String,
    /// Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints
    #[arg(value_name = "CLAIM", required = true)]
    pub(crate) claims: Vec<Claim>,
}

/// One record of a group: its shard, offset and, when known, the delivery
/// attempt it was claimed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Claim {
    pub(crate) shard: u32,
    pub(crate) offset: u64,
    pub(crate) attempts: Option<u32>,
}

impl FromStr for Claim {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bad = || format!("expected SHARD:OFFSET or SHARD:OFFSET:ATTEMPTS, not {value:?}");
        let mut parts = value.split(':');
        let shard = parts.next().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
        let offset = parts.next().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
        let attempts = match parts.next() {
            None => None,
            Some(s) => Some(s.parse().map_err(|_| bad())?),
        };
        if parts.next().is_some() {
            return Err(bad());
        }
        Ok(Self {
            shard,
            offset,
            attempts,
        })
    }
}

impl fmt::Display for Claim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.shard, self.offset)?;
        if let Some(attempts) = self.attempts {
            write!(f, ":{attempts}")?;
        }
        Ok(())
    }
}

pub(crate) async fn run(
    command: &GroupCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    // Asked before connecting when the question needs nothing from the
    // brokers. A seek asks once it knows where the cursor is.
    match command {
        GroupCommand::Rm { target, confirm } => ask(
            &format!(
                "Delete group {} on {}, with its cursor, dead letters and claims?",
                target.group,
                rm_scope(target)
            ),
            *confirm,
        )?,
        GroupCommand::DeadLetters(DeadLetterCommand::Discard { claims, confirm }) => ask(
            &format!(
                "Stop tracking {} dead letter(s) of {} on {}?",
                claims.claims.len(),
                claims.group,
                claims.stream
            ),
            *confirm,
        )?,
        _ => {}
    }
    let broker = Broker::connect(settings).await?;
    match command {
        GroupCommand::Create { target, from } => {
            let start = start_position(*from);
            let shards = shards_for(&broker, target, start).await?;
            let mut rows = Vec::with_capacity(shards.len());
            for shard in shards {
                let position = broker
                    .cluster
                    .group_create(
                        &broker.tenant,
                        &broker.namespace,
                        &target.stream,
                        shard,
                        &target.group,
                        start,
                    )
                    .await
                    .with_context(|| format!("create {} on shard {shard}", target.group))?;
                rows.push((shard, position));
            }
            print_positions(out, target, "created", &rows)
        }
        GroupCommand::Describe { target } => {
            let infos = describe(&broker, target).await?;
            if out.json {
                return out.json_value(&json!({
                    "stream": target.stream,
                    "group": target.group,
                    "shards": infos.iter().map(|(s, i)| info_json(*s, i)).collect::<Vec<_>>(),
                }));
            }
            out.text(&describe_table(&infos))
        }
        GroupCommand::Seek {
            target,
            to,
            confirm,
        } => {
            let start = start_position(*to);
            let shards = shards_for(&broker, target, start).await?;
            let infos = describe(&broker, target).await?;
            if seeks_backwards(start, &infos) {
                ask(
                    &format!(
                        "Move {} on {} back to {}? Finished records will be delivered again.",
                        target.group,
                        target.stream,
                        position_text(start)
                    ),
                    *confirm,
                )?;
            }
            let mut rows = Vec::with_capacity(shards.len());
            for shard in shards {
                let position = broker
                    .cluster
                    .group_seek(
                        &broker.tenant,
                        &broker.namespace,
                        &target.stream,
                        shard,
                        &target.group,
                        start,
                    )
                    .await
                    .with_context(|| format!("seek {} on shard {shard}", target.group))?;
                rows.push((shard, position));
            }
            print_positions(out, target, "moved", &rows)
        }
        GroupCommand::Rm { target, .. } => {
            let scope = rm_scope(target);
            let mut existed = false;
            for shard in shards_for(&broker, target, StartPosition::Latest).await? {
                existed |= broker
                    .cluster
                    .group_delete(
                        &broker.tenant,
                        &broker.namespace,
                        &target.stream,
                        shard,
                        &target.group,
                    )
                    .await
                    .with_context(|| format!("delete {} on shard {shard}", target.group))?;
            }
            let text = if existed {
                format!("deleted group {} on {scope}", target.group)
            } else {
                format!("group {} had nothing on {scope} to delete", target.group)
            };
            out.done(
                &text,
                json!({
                    "stream": target.stream,
                    "group": target.group,
                    "shard": target.shard,
                    "deleted": existed,
                }),
            )
        }
        GroupCommand::Poll {
            target,
            max,
            wait_ms,
            visibility_ms,
        } => {
            let options = GroupPollOptions {
                member: None,
                wait: Duration::from_millis(*wait_ms),
                visibility: visibility_ms.map(Duration::from_millis),
            };
            let mut remaining = *max;
            for shard in shards_for(&broker, target, StartPosition::Latest).await? {
                if remaining == 0 {
                    break;
                }
                let records = broker
                    .cluster
                    .group_poll_with(
                        &broker.tenant,
                        &broker.namespace,
                        &target.stream,
                        shard,
                        &target.group,
                        remaining,
                        &options,
                    )
                    .await
                    .with_context(|| format!("poll {} on shard {shard}", target.group))?;
                remaining = remaining.saturating_sub(records.len() as u32);
                for record in &records {
                    out.raw_line(&record_line(out.json, target, shard, record))?;
                }
            }
            if remaining == *max && !out.json {
                eprintln!("no records to claim");
            }
            Ok(())
        }
        GroupCommand::Ack(claims) => settle(&broker, out, claims, Settle::Ack).await,
        GroupCommand::Nack { claims, delay_ms } => {
            let settle_as = match delay_ms {
                Some(ms) => Settle::NackAfter(Duration::from_millis(*ms)),
                None => Settle::Nack,
            };
            settle(&broker, out, claims, settle_as).await
        }
        GroupCommand::Extend {
            stream,
            group,
            claim,
            for_ms,
        } => {
            let record = claimed_record(claim)?;
            let stands = broker
                .cluster
                .group_extend(
                    &broker.tenant,
                    &broker.namespace,
                    stream,
                    claim.shard,
                    group,
                    &record,
                    Duration::from_millis(*for_ms),
                )
                .await?;
            let ms = stands.as_millis() as u64;
            out.done(
                &format!("claim {claim} now stands for {ms} ms"),
                json!({ "stream": stream, "group": group, "claim": claim.to_string(), "visible_ms": ms }),
            )
        }
        GroupCommand::DeadLetters(command) => dead_letters(&broker, out, command).await,
    }
}

async fn dead_letters(
    broker: &Broker,
    out: &Output,
    command: &DeadLetterCommand,
) -> anyhow::Result<()> {
    match command {
        DeadLetterCommand::Ls { target } => {
            let mut claims = Vec::new();
            for shard in shards_for(broker, target, StartPosition::Latest).await? {
                let offsets = broker
                    .cluster
                    .group_dead_letters(
                        &broker.tenant,
                        &broker.namespace,
                        &target.stream,
                        shard,
                        &target.group,
                    )
                    .await
                    .with_context(|| format!("list dead letters on shard {shard}"))?;
                claims.extend(offsets.into_iter().map(|offset| Claim {
                    shard,
                    offset,
                    attempts: None,
                }));
            }
            if out.json {
                let items: Vec<Value> = claims
                    .iter()
                    .map(
                        |c| json!({ "shard": c.shard, "offset": c.offset, "claim": c.to_string() }),
                    )
                    .collect();
                return out.json_value(&json!({
                    "stream": target.stream,
                    "group": target.group,
                    "dead_letters": items,
                }));
            }
            let rows = claims
                .iter()
                .map(|c| vec![c.shard.to_string(), c.offset.to_string(), c.to_string()])
                .collect();
            out.text(&table(&["SHARD", "OFFSET", "CLAIM"], rows))
        }
        DeadLetterCommand::Add(claims) => settle(broker, out, claims, Settle::DeadLetter).await,
        DeadLetterCommand::Redrive(claims) => settle(broker, out, claims, Settle::Redrive).await,
        DeadLetterCommand::Discard { claims, .. } => {
            settle(broker, out, claims, Settle::Discard).await
        }
    }
}

/// What a command does to each claim it is given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settle {
    Ack,
    Nack,
    NackAfter(Duration),
    DeadLetter,
    Redrive,
    Discard,
}

impl Settle {
    /// The past tense printed for it, and the JSON field listing the claims.
    pub(crate) fn verb(self) -> &'static str {
        match self {
            Self::Ack => "acked",
            Self::Nack | Self::NackAfter(_) => "nacked",
            Self::DeadLetter => "dead_lettered",
            Self::Redrive => "redriven",
            Self::Discard => "discarded",
        }
    }
}

async fn settle(
    broker: &Broker,
    out: &Output,
    claims: &Claims,
    settle: Settle,
) -> anyhow::Result<()> {
    let (tenant, namespace) = (broker.tenant.as_str(), broker.namespace.as_str());
    let (stream, group) = (claims.stream.as_str(), claims.group.as_str());
    let cluster = &broker.cluster;
    let mut done = Vec::with_capacity(claims.claims.len());
    for claim in &claims.claims {
        let (shard, offset) = (claim.shard, claim.offset);
        let result = match settle {
            Settle::Ack => {
                cluster
                    .group_ack(tenant, namespace, stream, shard, group, offset)
                    .await
            }
            Settle::Nack => {
                cluster
                    .group_nack(tenant, namespace, stream, shard, group, offset)
                    .await
            }
            Settle::NackAfter(delay) => {
                cluster
                    .group_nack_after(tenant, namespace, stream, shard, group, offset, delay)
                    .await
            }
            Settle::DeadLetter => {
                cluster
                    .group_dead_letter(tenant, namespace, stream, shard, group, offset)
                    .await
            }
            Settle::Redrive => {
                cluster
                    .group_redrive(tenant, namespace, stream, shard, group, offset)
                    .await
            }
            Settle::Discard => {
                cluster
                    .group_discard(tenant, namespace, stream, shard, group, offset)
                    .await
            }
        };
        // The earlier claims were settled; say so, since a retry of the whole
        // command would be refused for them.
        result.with_context(|| {
            let before = if done.is_empty() {
                String::new()
            } else {
                format!(" ({} {})", settle.verb(), done.join(" "))
            };
            format!("claim {claim}{before}")
        })?;
        done.push(claim.to_string());
    }
    let verb = settle.verb();
    out.done(
        &format!("{} {}", verb.replace('_', "-"), done.join(" ")),
        json!({ "stream": stream, "group": group, verb: done }),
    )
}

/// The shards a command covers. An offset names a record on one shard only,
/// so it needs --shard unless the stream has just one.
async fn shards_for(
    broker: &Broker,
    target: &Target,
    start: StartPosition,
) -> anyhow::Result<Vec<u32>> {
    if let Some(shard) = target.shard {
        return Ok(vec![shard]);
    }
    let shards = broker
        .cluster
        .stream_shards(&broker.tenant, &broker.namespace, &target.stream)
        .await?;
    if let StartPosition::Offset(offset) = start
        && shards > 1
    {
        return Err(fail(
            Exit::Usage,
            format!(
                "{} has {shards} shards and offset {offset} names a record on only one; pass --shard",
                target.stream
            ),
        ));
    }
    Ok((0..shards).collect())
}

async fn describe(broker: &Broker, target: &Target) -> anyhow::Result<Vec<(u32, GroupInfo)>> {
    let mut infos = Vec::new();
    for shard in shards_for(broker, target, StartPosition::Latest).await? {
        let info = broker
            .cluster
            .group_describe(
                &broker.tenant,
                &broker.namespace,
                &target.stream,
                shard,
                &target.group,
            )
            .await
            .with_context(|| format!("describe {} on shard {shard}", target.group))?;
        infos.push((shard, info));
    }
    Ok(infos)
}

pub(crate) fn start_position(arg: StartArg) -> StartPosition {
    match arg {
        StartArg::Latest => StartPosition::Latest,
        StartArg::Earliest => StartPosition::Earliest,
        StartArg::Offset(offset) => StartPosition::Offset(offset),
    }
}

fn rm_scope(target: &Target) -> String {
    match target.shard {
        Some(shard) => format!("shard {shard} of {}", target.stream),
        None => target.stream.clone(),
    }
}

fn position_text(start: StartPosition) -> String {
    match start {
        StartPosition::Latest => "the tail".to_string(),
        StartPosition::Earliest => "the earliest record".to_string(),
        StartPosition::Offset(offset) => format!("offset {offset}"),
    }
}

/// Whether moving to `start` would take any shard's cursor back over records
/// the group finished. `earliest` counts whenever a shard has finished
/// anything, since the oldest retained offset is not known here.
pub(crate) fn seeks_backwards(start: StartPosition, infos: &[(u32, GroupInfo)]) -> bool {
    infos.iter().any(|(_, info)| match (start, info.committed) {
        (_, None) | (StartPosition::Latest, _) => false,
        (StartPosition::Earliest, Some(committed)) => committed > 0,
        (StartPosition::Offset(offset), Some(committed)) => offset < committed,
    })
}

/// A [`GroupRecord`] carrying only what an extension checks: the offset and
/// the attempt it was claimed on.
pub(crate) fn claimed_record(claim: &Claim) -> anyhow::Result<GroupRecord> {
    let Some(attempts) = claim.attempts else {
        return Err(fail(
            Exit::Usage,
            format!(
                "extending a claim needs its attempt count, as SHARD:OFFSET:ATTEMPTS \
                 the way poll prints it, not {claim}"
            ),
        ));
    };
    Ok(GroupRecord {
        offset: claim.offset,
        payload: bytes::Bytes::new(),
        attempts,
        skipped_before: 0,
        publisher: None,
        timestamp_micros: None,
    })
}

/// One polled record as printed: `CLAIM<TAB>payload`, or a JSON object.
pub(crate) fn record_line(
    json: bool,
    target: &Target,
    shard: u32,
    record: &GroupRecord,
) -> Vec<u8> {
    let claim = Claim {
        shard,
        offset: record.offset,
        attempts: Some(record.attempts),
    };
    if json {
        let (field, payload) = payload_field(&record.payload);
        let mut object = json!({
            "stream": target.stream,
            "group": target.group,
            "shard": shard,
            "offset": record.offset,
            "attempts": record.attempts,
            "claim": claim.to_string(),
        });
        object[field] = payload;
        if let Some(micros) = record.timestamp_micros {
            object["timestamp_micros"] = micros.into();
        }
        if let Some(publisher) = &record.publisher {
            object["publisher"] = publisher.clone().into();
        }
        return object.to_string().into_bytes();
    }
    let mut line = format!("{claim}\t").into_bytes();
    line.extend_from_slice(&record.payload);
    line
}

pub(crate) fn info_json(shard: u32, info: &GroupInfo) -> Value {
    json!({
        "shard": shard,
        "committed": info.committed,
        "tail": info.tail,
        "lag": info.lag(),
        "in_flight": info.in_flight,
        "owed": info.owed,
        "dead_letters": info.dead_letters,
    })
}

pub(crate) fn describe_table(infos: &[(u32, GroupInfo)]) -> String {
    let rows = infos
        .iter()
        .map(|(shard, info)| {
            vec![
                shard.to_string(),
                info.committed
                    .map_or_else(|| "-".to_string(), |c| c.to_string()),
                info.tail.to_string(),
                info.lag().to_string(),
                info.in_flight.to_string(),
                info.owed.to_string(),
                info.dead_letters.to_string(),
            ]
        })
        .collect();
    table(
        &[
            "SHARD",
            "COMMITTED",
            "TAIL",
            "LAG",
            "IN_FLIGHT",
            "OWED",
            "DEAD_LETTERS",
        ],
        rows,
    )
}

/// Each shard's position after a create or seek. `field` says whether the
/// group was there to move: `created` for a create, `moved` for a seek.
fn print_positions(
    out: &Output,
    target: &Target,
    field: &str,
    rows: &[(u32, GroupPosition)],
) -> anyhow::Result<()> {
    if out.json {
        let shards: Vec<Value> = rows
            .iter()
            .map(|(shard, p)| json!({ "shard": shard, "offset": p.offset, field: p.moved }))
            .collect();
        return out.json_value(&json!({
            "stream": target.stream,
            "group": target.group,
            "shards": shards,
        }));
    }
    let status = |moved: bool| match (field, moved) {
        ("created", true) => "created",
        ("created", false) => "already existed",
        _ => "moved",
    };
    let rows = rows
        .iter()
        .map(|(shard, p)| {
            vec![
                shard.to_string(),
                p.offset.to_string(),
                status(p.moved).to_string(),
            ]
        })
        .collect();
    out.text(&table(&["SHARD", "OFFSET", "STATUS"], rows))
}

#[cfg(test)]
mod tests;
