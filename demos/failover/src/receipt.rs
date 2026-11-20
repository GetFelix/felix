//! What the checker prints: a line a second, and a receipt.

use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::Result;
use felix_client::Client;

use crate::Args;
use crate::ledger::{Ledger, Verdict, judge};
use crate::reader;

pub(crate) fn print_line(ledger: &Mutex<Ledger>, started: Instant) {
    let ledger = ledger.lock().expect("ledger");
    let c = ledger.counters(Instant::now());
    println!(
        "[{:>4}s] acknowledged {:>6}  in-flight {:>2}  indeterminate {:>2}  delivered {:>6}  gaps {}  duplicates {}",
        started.elapsed().as_secs(),
        c.acked,
        c.in_flight,
        c.in_doubt,
        c.delivered,
        c.gaps,
        c.duplicates
    );
}

/// A receipt while the producers keep running, from a snapshot of what was
/// acknowledged before the re-read began.
pub(crate) async fn interim(
    cluster: &Client,
    args: &Args,
    run_id: &str,
    ledger: &Mutex<Ledger>,
    started: Instant,
) {
    if let Err(err) = receipt(cluster, args, run_id, ledger, started, "interim").await {
        eprintln!("receipt failed: {err:#}");
    }
}

/// The final receipt, after the producers stopped. Whether the invariant held.
pub(crate) async fn finish(
    cluster: &Client,
    args: &Args,
    run_id: &str,
    ledger: &Mutex<Ledger>,
    started: Instant,
) -> Result<bool> {
    receipt(cluster, args, run_id, ledger, started, "final").await
}

async fn receipt(
    cluster: &Client,
    args: &Args,
    run_id: &str,
    ledger: &Mutex<Ledger>,
    started: Instant,
    kind: &str,
) -> Result<bool> {
    let snapshot = ledger.lock().expect("ledger").snapshot();
    let (counts, unexpected, records) = reader::read_back(cluster, args, run_id, ledger).await?;
    let verdict = judge(&snapshot, &counts, unexpected);
    let text = {
        let ledger = ledger.lock().expect("ledger");
        render(
            kind, args, run_id, started, &snapshot, &verdict, &ledger, records,
        )
    };
    println!("{text}");
    if let Err(err) = tokio::fs::write(&args.receipt_file, &text).await {
        eprintln!("could not write {}: {err}", args.receipt_file);
    }
    Ok(verdict.holds())
}

#[allow(clippy::too_many_arguments)]
fn render(
    kind: &str,
    args: &Args,
    run_id: &str,
    started: Instant,
    snapshot: &crate::ledger::Snapshot,
    verdict: &Verdict,
    ledger: &Ledger,
    records: u64,
) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "\n==== failover receipt ({kind}) ====");
    let _ = writeln!(
        out,
        "run                  {run_id}, {} s",
        started.elapsed().as_secs()
    );
    let _ = writeln!(
        out,
        "producers            {} idempotent producers x {}/s, {}-byte payloads",
        args.producers, args.rate, args.payload_bytes
    );
    if !args.config_note.is_empty() {
        let _ = writeln!(out, "cluster              {}", args.config_note);
    }
    let _ = writeln!(out, "sent                 {}", snapshot.sent);
    let _ = writeln!(out, "acknowledged         {}", verdict.acked);
    let _ = writeln!(
        out,
        "in flight            {} (sent, no answer yet)",
        snapshot.in_flight
    );
    let _ = writeln!(
        out,
        "indeterminate        {} (the last attempt errored; {} found in the stream, {} not)",
        snapshot.in_doubt.len(),
        verdict.in_doubt_landed,
        verdict.in_doubt_absent
    );
    let _ = writeln!(
        out,
        "recovered by resend  {} (errored at least once, then acknowledged under the same sequence)",
        snapshot.recovered
    );
    let _ = writeln!(
        out,
        "records read back    {records} (whole stream, from the oldest, after the snapshot)"
    );
    let _ = writeln!(out, "GAPS                 {}", verdict.gaps.len());
    let _ = writeln!(out, "DUPLICATES           {}", verdict.duplicates.len());
    if verdict.unexpected > 0 {
        let _ = writeln!(
            out,
            "UNEXPECTED           {} (this run's records no producer sent)",
            verdict.unexpected
        );
    }
    for key in verdict.gaps.iter().take(10) {
        let _ = writeln!(out, "  missing   producer {} seq {}", key.0, key.1);
    }
    for key in verdict.duplicates.iter().take(10) {
        let _ = writeln!(out, "  repeated  producer {} seq {}", key.0, key.1);
    }
    if ledger.foreign() > 0 {
        let _ = writeln!(out, "other runs' records  {} ignored", ledger.foreign());
    }
    if ledger.refused_producers() > 0 {
        let _ = writeln!(out, "refused producers    {}", ledger.refused_producers());
    }
    let _ = writeln!(out, "reader resubscribes  {}", ledger.reader_resubscribes());
    if ledger.stalls().is_empty() {
        let _ = writeln!(out, "write stalls         none over 500 ms");
    }
    for stall in ledger.stalls() {
        let _ = writeln!(
            out,
            "write stall          at {} s: writes blocked {} ms{}",
            stall.at_secs,
            stall.blocked.as_millis(),
            stall
                .drained
                .map(|d| format!(", every producer writing again after {} ms", d.as_millis()))
                .unwrap_or_default()
        );
    }
    let _ = writeln!(
        out,
        "verdict              {}",
        if verdict.holds() {
            "every acknowledged write is in the stream exactly once; nothing extra"
        } else {
            "VIOLATED: see the missing, repeated or unexpected records above"
        }
    );
    out
}
