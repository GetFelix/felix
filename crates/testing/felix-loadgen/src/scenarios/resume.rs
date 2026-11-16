//! Resume at an offset, read a few records, drop the subscription, repeat.
//!
//! This is the cost of a durable subscriber coming back after a disconnect:
//! the broker has to check the offset is still readable (on a Raft shard,
//! confirm it is still the leader or ask for a read index) and then serve
//! history. Something else publishes, so the resumes run against a log that
//! is growing under them.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use felix_wire::StartPosition;
use tokio::task::JoinSet;

use super::connect::cluster;
use super::{Common, start_instant};
use crate::stats::{Samples, fmt_us, report};

/// How long to wait for the publisher to leave enough history to resume into.
const HISTORY_WAIT: Duration = Duration::from_secs(60);
/// A resume that has not delivered its next record by now counts as a short read.
const RECORD_WAIT: Duration = Duration::from_secs(10);
/// History needed before the window opens, in multiples of `span`.
const HISTORY_SPANS: u64 = 20;

/// The `resume`-only flags.
pub struct ResumeOptions {
    /// How long the measured window lasts.
    pub duration: Duration,
    /// Connect, then wait for this wall-clock time before looking for history.
    pub start_at: Option<SystemTime>,
    /// Records read after each resume.
    pub span: usize,
}

/// What one worker saw.
#[derive(Default)]
struct Tally {
    resumes: u64,
    records: u64,
    errors: u64,
    short_reads: u64,
    /// First record delivered below the offset asked for.
    bad_first_offsets: u64,
    resume: Samples,
    replay: Samples,
}

/// A resume offset in `[0, room)`, from a xorshift state advanced in place.
/// Each worker has its own state, so a run replays the same offsets.
fn resume_offset(state: &mut u64, room: u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state % room.max(1)
}

pub(crate) async fn resume(
    common: &Common,
    stream: &str,
    opts: &ResumeOptions,
) -> Result<serde_json::Value> {
    let span = opts.span.max(1);
    let workers = common.concurrency.max(1);
    let client = Arc::new(cluster(common).await?);

    tokio::time::sleep_until(start_instant(opts.start_at, "resume").into()).await;
    let needed = span as u64 * HISTORY_SPANS;
    let history_deadline = Instant::now() + HISTORY_WAIT;
    let tail = loop {
        let probe = client
            .subscribe(&common.tenant, &common.namespace, stream)
            .await
            .context("subscribe to find the tail")?;
        let tail = probe.live_offset().unwrap_or(0);
        if tail >= needed {
            break tail;
        }
        if Instant::now() >= history_deadline {
            bail!(
                "the stream holds {tail} records after {HISTORY_WAIT:?}, need {needed}: is something publishing?"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let room = tail - span as u64;

    let started = Instant::now();
    let deadline = started + opts.duration;
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let (client, tenant, namespace, stream) = (
            Arc::clone(&client),
            common.tenant.clone(),
            common.namespace.clone(),
            stream.to_string(),
        );
        tasks.spawn(async move {
            let mut state = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut tally = Tally::default();
            while Instant::now() < deadline {
                let at = resume_offset(&mut state, room);
                let t0 = Instant::now();
                let opened = client
                    .subscribe_shard(
                        &tenant,
                        &namespace,
                        &stream,
                        0,
                        Some(StartPosition::Offset(at)),
                    )
                    .await;
                let mut sub = match opened {
                    Ok(sub) => sub,
                    Err(_) => {
                        tally.errors += 1;
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                };
                let mut read = 0usize;
                let mut resumed = None;
                while read < span {
                    match tokio::time::timeout(RECORD_WAIT, sub.next_event()).await {
                        Ok(Ok(Some(event))) => {
                            if read == 0 {
                                resumed = Some(t0.elapsed());
                                tally.bad_first_offsets +=
                                    u64::from(event.offset.is_none_or(|got| got < at));
                            }
                            read += 1;
                        }
                        _ => break,
                    }
                }
                let Some(resumed) = resumed else {
                    tally.errors += 1;
                    continue;
                };
                tally.resumes += 1;
                tally.records += read as u64;
                tally.short_reads += u64::from(read < span);
                tally.resume.record(resumed);
                tally.replay.record(t0.elapsed());
            }
            tally
        });
    }
    let mut total = Tally::default();
    while let Some(done) = tasks.join_next().await {
        let t = done.context("resume worker")?;
        total.resumes += t.resumes;
        total.records += t.records;
        total.errors += t.errors;
        total.short_reads += t.short_reads;
        total.bad_first_offsets += t.bad_first_offsets;
        total.resume.merge(t.resume);
        total.replay.merge(t.replay);
    }
    if total.resumes == 0 {
        bail!("no resume completed ({} errors)", total.errors);
    }

    let secs = started.elapsed().as_secs_f64();
    let resume = total.resume.percentiles();
    let replay = total.replay.percentiles();
    let resumes_s = total.resumes as f64 / secs;
    let msg_s = total.records as f64 / secs;
    report!(
        common,
        "resume: workers = {workers}, span = {span}, resumes = {} in {secs:.1} s, errors = {}, short reads = {}, first records below the offset = {}",
        total.resumes,
        total.errors,
        total.short_reads,
        total.bad_first_offsets
    );
    report!(
        common,
        "  resumes = {resumes_s:.1}/s, replayed throughput = {msg_s:.1} msg/s"
    );
    report!(
        common,
        "  resume to first record: p50 = {}, p99 = {}, p999 = {}, max = {}",
        fmt_us(resume.p50_us),
        fmt_us(resume.p99_us),
        fmt_us(resume.p999_us),
        fmt_us(resume.max_us)
    );
    report!(
        common,
        "  resume to {span} records: p50 = {}, p99 = {}",
        fmt_us(replay.p50_us),
        fmt_us(replay.p99_us)
    );
    Ok(serde_json::json!({
        "scenario": "resume",
        "environment": common.environment,
        "stream": stream,
        "workers": workers,
        "span": span,
        "duration_s": secs,
        "tail_at_start": tail,
        "resumes": total.resumes,
        "replayed": total.records,
        "errors": total.errors,
        "short_reads": total.short_reads,
        "bad_first_offsets": total.bad_first_offsets,
        "resumes_per_s": resumes_s,
        "replayed_throughput_msg_s": msg_s,
        "resume_latency_us": {
            "p50": resume.p50_us, "p99": resume.p99_us, "p999": resume.p999_us, "max": resume.max_us,
            "samples": total.resume.len(),
        },
        "replay_latency_us": {
            "p50": replay.p50_us, "p99": replay.p99_us, "p999": replay.p999_us, "max": replay.max_us,
            "samples": total.replay.len(),
        },
    }))
}

#[cfg(test)]
mod tests;
