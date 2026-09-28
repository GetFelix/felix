//! The three things `felix-log-tool` does: write, verify, benchmark.
//!
//! All output is line-delimited JSON on stdout so a test harness or a benchmark
//! script can consume it without parsing prose. Diagnostics go to stderr.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, ReadRange};

use super::args::{BenchArgs, VerifyArgs, WriteArgs};
use super::payload;

type Failure = String;

/// Append records, optionally reporting each acknowledgement.
///
/// With `--records 0` this runs until the process is killed, which is exactly
/// what the crash harness wants: it reads acknowledgements from stdout, picks a
/// moment, and sends `SIGKILL` mid-write.
pub(crate) async fn write(args: WriteArgs) -> Result<(), Failure> {
    let log = DiskLog::open(&args.dir, "log-tool", args.config.clone()).map_err(fail)?;
    let start_offset = log.tail_offset().await.map_err(fail)?;

    let mut stdout = std::io::stdout().lock();
    emit(
        &mut stdout,
        &format!(
            r#"{{"event":"opened","dir":{:?},"start_offset":{start_offset},"fsync":{:?}}}"#,
            args.dir.display().to_string(),
            describe_fsync(args.config.fsync_mode)
        ),
    );

    let mut offset = start_offset;
    let target = (args.records > 0).then(|| start_offset + args.records);
    while target.is_none_or(|target| offset < target) {
        let remaining = target.map(|target| target - offset).unwrap_or(u64::MAX);
        let batch_len = (args.batch as u64).min(remaining) as usize;
        let records: Vec<AppendRecord> = (0..batch_len)
            .map(|index| AppendRecord {
                payload: Bytes::from(payload::payload_for(
                    offset + index as u64,
                    args.payload_bytes,
                )),
                timestamp_micros: now_micros(),
                mark: Default::default(),
            })
            .collect();

        let result = log.append(&records).await.map_err(fail)?;
        offset = result.last_offset + 1;
        if args.report_acks {
            // Printed only after `append` resolves, so under `on_commit` every
            // offset on this line is genuinely on the device. A crash harness
            // can hold the tool to exactly these records.
            emit(
                &mut stdout,
                &format!(
                    r#"{{"event":"ack","first_offset":{},"last_offset":{}}}"#,
                    result.first_offset, result.last_offset
                ),
            );
        }
    }

    if args.clean_shutdown {
        log.shutdown().await.map_err(fail)?;
    }
    emit(
        &mut stdout,
        &format!(
            r#"{{"event":"done","tail_offset":{offset},"durable_offset":{}}}"#,
            log.durable_offset()
        ),
    );
    Ok(())
}

/// Recover a log and report what survived, checking payload integrity.
pub(crate) async fn verify(args: VerifyArgs) -> Result<(), Failure> {
    let opened = Instant::now();
    let log = DiskLog::open(&args.dir, "log-tool", args.config.clone()).map_err(fail)?;
    let recovery_seconds = opened.elapsed().as_secs_f64();

    let tail = log.tail_offset().await.map_err(fail)?;
    let base = log.base_offset();

    // Page through the whole log so memory stays bounded no matter how much
    // survived — the same discipline a follower catching up would use.
    let mut cursor = base;
    let mut count = 0u64;
    let mut mismatches = Vec::new();
    let mut generation_starts = Vec::new();
    while cursor < tail {
        let page = log
            .read_range(ReadRange {
                start: cursor,
                max_bytes: 1024 * 1024,
            })
            .await
            .map_err(fail)?;
        if page.is_empty() {
            return Err(format!(
                "read at offset {cursor} returned nothing while the tail is {tail}"
            ));
        }
        for record in &page {
            if record.mark.is_generation_start() {
                // A leader's marker, not a written record: shown for what it
                // is and kept out of the payload check and the count.
                generation_starts.push((record.offset, generation_of(&record.payload)));
                continue;
            }
            if let Some(payload_bytes) = args.payload_bytes
                && !payload::matches(record.offset, payload_bytes, &record.payload)
            {
                // Report what the payload thinks it is: a record carrying a
                // neighbour's offset points at a very different bug from one
                // carrying garbage.
                mismatches.push((record.offset, payload::claimed_offset(&record.payload)));
            }
            count += 1;
        }
        // Offsets must be contiguous within and across pages.
        for pair in page.windows(2) {
            if pair[1].offset != pair[0].offset + 1 {
                return Err(format!(
                    "offset gap: {} followed by {}",
                    pair[0].offset, pair[1].offset
                ));
            }
        }
        if page[0].offset != cursor {
            return Err(format!(
                "expected offset {cursor} at the start of a page, got {}",
                page[0].offset
            ));
        }
        cursor = page.last().expect("non-empty").offset + 1;
    }

    if let Some((offset, claimed)) = mismatches.first() {
        return Err(format!(
            "{} record(s) had corrupt payloads; offset {offset} carries {claimed:?}",
            mismatches.len(),
        ));
    }
    if let Some(expected) = args.expect_at_least
        && count < expected
    {
        return Err(format!(
            "only {count} records survived, expected at least {expected}"
        ));
    }

    let mut stdout = std::io::stdout().lock();
    for (offset, generation) in &generation_starts {
        let generation = generation.map_or("null".to_string(), |g| g.to_string());
        emit(
            &mut stdout,
            &format!(
                r#"{{"event":"generation_start","offset":{offset},"generation":{generation}}}"#
            ),
        );
    }
    emit(
        &mut stdout,
        &format!(
            r#"{{"event":"verified","base_offset":{base},"tail_offset":{tail},"records":{count},"generation_starts":{},"segments":{},"recovery_seconds":{recovery_seconds:.6}}}"#,
            generation_starts.len(),
            log.segments().len()
        ),
    );
    Ok(())
}

/// The generation a generation-start record names, or `None` when its payload
/// is not the eight bytes it should be.
pub(crate) fn generation_of(payload: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(payload.try_into().ok()?))
}

/// Measure append latency and throughput under one durability policy.
pub(crate) async fn bench(args: BenchArgs) -> Result<(), Failure> {
    let log = Arc::new(DiskLog::open(&args.dir, "log-tool", args.config.clone()).map_err(fail)?);

    // Warm up before measuring: the first appends pay for segment creation,
    // preallocation and page-cache faults that steady state does not.
    if args.warmup_records > 0 {
        run_load(&log, args.warmup_records, args.payload_bytes, args.batch, 1).await?;
    }

    let started = Instant::now();
    let mut latencies = run_load(
        &log,
        args.records,
        args.payload_bytes,
        args.batch,
        args.concurrency,
    )
    .await?;
    let elapsed = started.elapsed();

    // Flush before reporting throughput: a policy that leaves data in the page
    // cache has not finished its work when the last append returns.
    log.sync().await.map_err(fail)?;

    latencies.sort_unstable();
    let appends = latencies.len().max(1) as f64;
    let records = args.records.max(1) as f64;

    let mut stdout = std::io::stdout().lock();
    emit(
        &mut stdout,
        &format!(
            r#"{{"event":"bench","label":{:?},"fsync":{:?},"records":{},"batch":{},"payload_bytes":{},"concurrency":{},"seconds":{:.6},"records_per_second":{:.1},"appends":{appends},"latency_us":{{"p50":{:.1},"p90":{:.1},"p99":{:.1},"p999":{:.1},"max":{:.1}}}}}"#,
            args.label,
            describe_fsync(args.config.fsync_mode),
            args.records,
            args.batch,
            args.payload_bytes,
            args.concurrency,
            elapsed.as_secs_f64(),
            records / elapsed.as_secs_f64(),
            percentile(&latencies, 0.50),
            percentile(&latencies, 0.90),
            percentile(&latencies, 0.99),
            percentile(&latencies, 0.999),
            percentile(&latencies, 1.0),
        ),
    );
    Ok(())
}

/// Drive `records` appends across `concurrency` tasks, returning per-append
/// latencies in microseconds.
async fn run_load(
    log: &Arc<DiskLog>,
    records: u64,
    payload_bytes: usize,
    batch: usize,
    concurrency: usize,
) -> Result<Vec<u64>, Failure> {
    let per_task = records.div_ceil(concurrency as u64);
    let mut tasks = Vec::with_capacity(concurrency);
    for task_index in 0..concurrency {
        let log = Arc::clone(log);
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut written = 0u64;
            while written < per_task {
                let batch_len = (batch as u64).min(per_task - written) as usize;
                // Payload contents do not affect timing, but keeping them
                // distinct per task avoids any accidental deduplication.
                let records: Vec<AppendRecord> = (0..batch_len)
                    .map(|index| AppendRecord {
                        payload: Bytes::from(payload::payload_for(
                            (task_index as u64) << 32 | (written + index as u64),
                            payload_bytes,
                        )),
                        timestamp_micros: now_micros(),
                        mark: Default::default(),
                    })
                    .collect();
                let started = Instant::now();
                log.append(&records).await?;
                latencies.push(started.elapsed().as_micros() as u64);
                written += batch_len as u64;
            }
            Ok::<Vec<u64>, felix_storage::StorageError>(latencies)
        }));
    }

    let mut all = Vec::new();
    for task in tasks {
        let result = task.await.map_err(|err| err.to_string())?;
        all.extend(result.map_err(fail)?);
    }
    Ok(all)
}

/// Nearest-rank percentile over a sorted slice.
fn percentile(sorted: &[u64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1] as f64
}

fn describe_fsync(mode: FsyncMode) -> String {
    match mode {
        FsyncMode::None => "none".to_string(),
        FsyncMode::Periodic { interval } => format!("periodic:{}ms", interval.as_millis()),
        FsyncMode::OnCommit => "on_commit".to_string(),
    }
}

fn emit(out: &mut impl std::io::Write, line: &str) {
    let _ = writeln!(out, "{line}");
    // Flushed per line: a crash harness reads this stream to decide when to
    // kill the process, and a buffered acknowledgement it never sees would make
    // the test claim less durability than the log actually delivered.
    let _ = out.flush();
}

fn fail(err: felix_storage::StorageError) -> Failure {
    err.to_string()
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros() as u64
}

#[cfg(test)]
mod tests;
