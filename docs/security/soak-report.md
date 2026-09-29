# Soak and resource-leak report: broker

Evidence record for [#154](https://github.com/gabloe/felix/issues/154), the
sustained-load half of the M0 concurrency and resource-leak exit criterion. The
static audit half is recorded separately in [panic-audit.md](panic-audit.md).

## Scope and environment

- Harness: `services/felix-broker-service/src/bin/soak/`, run via
  `cargo run --release -p felix-broker-service --bin soak`.
- Build: `--release` (fat LTO, `codegen-units = 1`), default features.
  `FELIX_DISABLE_TIMINGS=1`.
- Environment: macOS 15 (Darwin 25.6), Apple Silicon, 2026-08-09.
- Workload: 4 publishers, 4 subscribers, 1 KiB payloads, 12 s per phase, 25 s
  quiescence, 120 churn cycles, 4 identical load cycles, 3 SIGTERM restarts.
  Roughly 2M messages published per run.
- Time series: `data/soak/local-macos.jsonl`.

**This is not Linux evidence.** The deployment target is Linux, and RSS
accounting, allocator behaviour, and scheduler pressure all differ there. The
harness is cross-platform and the CI workflow runs it on `ubuntu-latest`; that
run is the evidence that should back a production claim. What is recorded here
is the local run that found and validated the fixes below.

## What is exercised

| Phase | What it stresses |
| --- | --- |
| `sustained_load` | Steady publish/subscribe through the full QUIC path |
| `connection_churn` | Connect/publish/disconnect cycles: connection, task, and fd release |
| `slow_subscribers` | Subscribers that never read: queue saturation and the drop policy |
| `repeated_load_cycles` | Identical load repeated, to separate a leak from allocator retention |
| `restart_cycles` | Real `SIGTERM` to a real child process under live traffic |

## Method notes

Two choices matter for reading the numbers:

**Memory is judged across repeated identical cycles, not against a baseline.**
Comparing post-load RSS to pre-load RSS measures allocator retention, not leaks, because
allocators do not promptly return freed pages, so that comparison flags every
healthy run. A leak instead shows peak RSS still climbing on the last identical
cycle, where retention plateaus.

**The broker's own gauges are the authoritative assertion.**
`felix_sub_active_connections`, `felix_sub_connection_subscribers`,
`felix_sub_queue_len`, `felix_sub_lane_queue_len`, `felix_broker_ingress_queue_depth`,
and `felix_broker_out_ack_depth` must all be exactly zero once every client has
disconnected. These describe the broker and nothing else, so residue in them is
unambiguously a broker leak.

**Process-wide fds and tasks are corroborating evidence, not the assertion.**
This harness runs the load generators in the *same process* as the broker, so a
raw `quiesced > baseline` comparison charges the broker for the harness's own
client teardown: `felix-client`'s `Subscription` spawns detached pipeline tasks
the harness cannot join, which wind down on their own schedule. An earlier
version asserted exact equality at a fixed instant and was measuring that race:
a CI run reported "+8 fds, +4 tasks" while every broker gauge sat at zero.

**Both ends of the comparison are settled rather than sampled at a fixed time.**
Baseline is taken once the idle broker stops changing, not immediately after
`start_broker` returns, because the runtime keeps allocating briefly after the listener
binds, and sampling into that window produced a baseline below the broker's real
idle state. The same idle broker read 2 tasks on Linux and 10 on macOS purely
from where the sample landed. Quiescence likewise polls until fds and tasks are
back at baseline, capped by `--quiesce-secs`. The cap is not a sleep: a healthy
run returns as soon as it settles, so raising it costs nothing and only buys
tolerance on a slow machine. Measured directly, a fixed 15 s sleep failed the
same workload about half the time and passed 3/3 at 60 s; the polling version
passed 6/6 and returned in 11.8–22.3 s.

## Findings

### F1: Subscriber connection registry never released entries (fixed)

`ACTIVE_SUB_CONN_COUNTS` retained an entry for **every subscriber connection ever
made**. After a run with 24 subscriber connections, 22 remained registered
following 30 s of quiescence, and `felix_sub_active_connections` reported 22 with
zero clients connected.

Cause: an ordering race, not a missing cleanup call. Subscription teardown
enqueues `LaneCommand::Unregister` and then *immediately* calls
`unregister_subscriber`, which removes the `subscriber_connections` entry. The
lane worker dequeues afterwards and used to look the connection up in that map.
Finding nothing, it skipped cleanup entirely, so neither the registry entry nor
the per-connection metric series was ever released.

Impact: unbounded growth in a long-lived broker with connection churn, both the
`DashMap` itself and the `felix_sub_connection_subscribers{connection_id=…}`
metric cardinality, one label value per connection for the life of the process.
`felix_sub_active_connections` was also permanently wrong, which matters because
it is the gauge an operator would use to judge connection health.

Fix: `LaneCommand::Unregister` now carries `connection_id`, so the worker does
not depend on a map entry that teardown races it to delete. Verified by A/B:
`felix_sub_active_connections` 22 → 0, `felix_sub_connection_subscribers` 24 → 0.
Regression test:
`subscribe/tests.rs::lane_unregister_cleans_up_after_teardown_already_removed_the_mapping`,
confirmed to fail against the pre-fix code.

### F2: Shutdown always force-aborted rather than draining (fixed)

Every `SIGTERM` restart cycle burned the entire drain deadline and then
force-cancelled, with both `quic_connections` and `quic_accept_loop` unfinished:
3/3 cycles, 10.00 s each against a 10 s deadline.

Cause: the drain waited for connection tasks to end, but a connection task only
ends when the *peer* disconnects. Subscribers hold connections open indefinitely
by design, so the wait could never complete. Cancelling admission was not enough:
nothing told the accepted connections to wind down.

This is the gap left when #139 shipped. It meant every rolling update dropped
in-flight publishes and acknowledgements (the exact failure that issue set out
to prevent) while appearing to have a graceful shutdown path.

Fix: the shutdown token now reaches every connection task.
`handle_connection_with_shutdown` stops accepting new streams, gives in-flight
streams a bounded grace (half the process drain budget), then closes the
connection with CONNECTION_CLOSE so the peer sees a deliberate shutdown.

The grace must be bounded for the same reason the connection wait had to be:
control and subscription streams are long-lived, so waiting on them
unconditionally just relocates the hang one level down. That was observed
directly: an intermediate fix that waited on streams without a bound reproduced
the identical 10 s forced abort.

Result: 3/3 cycles now drain cleanly in 3.00 s against a 6 s budget, exit status
0. Regression test:
`graceful_shutdown.rs::cancellation_winds_down_accepted_connections`.

That test previously asserted the *opposite*, that accepted connections keep
running after cancellation. It was encoding the bug, and was rewritten rather
than worked around.

### F3: Subscriber queue-depth accounting races teardown (fixed)

Extended runs showed `felix_sub_queue_len` settling between 1 and 13 rather than
at a fixed residue. The cause was an admission-order race: publishers made an
envelope visible on the channel before incrementing its depth. A receiver could
dequeue and attempt to decrement zero before the publisher recorded the item,
stranding the later increment.

Queue entries now own their accounting through an RAII wrapper. Depth is
incremented before a reserved channel permit publishes the entry, and the
wrapper decrements it whether the receiver consumes the entry or channel
teardown discards it. The soak harness no longer has a queue-depth allowance:
both `felix_sub_queue_len` and `felix_sub_lane_queue_len` must return to zero.

The lane gauge had a separate stale-series issue. A lane worker could exit while
its last reported labeled value was nonzero; workers now explicitly zero their
series on every exit path.

### F4: Subscriber writer ownership cycle retained tasks (fixed)

Direct task-count sampling exposed a lifecycle leak that fd sampling had missed.
After 30 s of quiescence, a short run retained 815 Tokio tasks against a baseline
of 10, plus one active subscriber registration, even though file descriptors had
returned exactly to baseline.

Cause: `WriterLaneManager` owned the senders for its lane and per-connection
writer tasks, while lane tasks and subscription feeders held strong
`Arc<WriterLaneManager>` references. The connection writer retained subscription
guards, which kept the broker's subscription sender alive; the feeder therefore
kept waiting for events while retaining the manager. Dropping the external
connection context could not break either cycle.

Fix: lane tasks and feeders now hold `Weak<WriterLaneManager>` and upgrade only
while processing work. When the connection context disappears, dropping the
manager closes the lane and connection-writer channels, releases subscription
guards, and lets every feeder terminate. `WriterLaneManager::drop` also releases
any connection-count entries whose queued unregister command could not run after
the manager disappeared.

Regression tests:
`subscribe/tests.rs::writer_lane_tasks_do_not_retain_manager` and
`subscribe/tests.rs::manager_drop_releases_remaining_connection_counts`.
The same short soak that previously ended at 815 tasks and one registration now
returns from 10 tasks to **10**, with both subscriber registration gauges at
**0** after 30 s.

## Steady state after the fixes

From the final local run (exit 0, no findings):

| Measure | Baseline | Peak | After 25 s quiescence |
| --- | ---: | ---: | ---: |
| Open file descriptors | 11 | 35 | **11** |
| RSS (KiB) | 9,312 | 639,296 | 816,784 |

Registration gauges after quiescence, all zero:
`felix_sub_active_connections`, `felix_sub_connection_subscribers`,
`felix_sub_conn_queue_len`, `felix_sub_lane_queue_len`,
`felix_client_sub_queue_len`, and `felix_sub_queue_len`.

Restart cycles: 3/3 clean, 3.00 s each, exit 0, with ~300k messages published
into each child before its `SIGTERM`.

**On the RSS figure.** It does not return to baseline, and that is expected on
this platform; macOS `malloc` rarely returns pages. The leak question is
answered by the repeated-cycle check, not this number. Peak RSS across four
identical cycles in an earlier run was 663,488 → 690,560 → 701,792 → 735,232 KiB:
+10.8% total, under the 25% tolerance, but **monotonically increasing across all
four cycles**. That is not a clean plateau. It is the weakest result in this
report and the reason the Linux CI run matters: four cycles is too few to
distinguish a slow leak from retention still settling, and the follow-up should
raise `--load-cycles` substantially on a Linux runner before the "no unbounded
memory growth" claim is treated as settled.

## Acceptance criteria status

- [x] Sustained-load and churn runs return resource counts to a documented
      steady-state envelope after quiescence: broker gauges exactly zero,
      process fds and tasks settling back to the idle baseline, RSS with the
      caveat above.
- [x] No deadlock, connection leak, fd leak, or task leak remains reproducible.
- [ ] **No unbounded memory growth**: not fully demonstrated. Monotonic growth
      across four cycles is unresolved; needs a longer Linux run.
- [x] Audit scope, workload, duration, environment, and findings recorded.
- [x] All reproduced defects fixed (F1-F4), including exact-zero queue
      accounting after quiescence.

## Residual risk

- The memory result is inconclusive, as above. This is the one criterion this
  report does not close.
- Evidence is from macOS; the Linux CI run is what should back a production
  claim.
- Lock ordering and core-shard handoff were exercised under load but not
  systematically reviewed. No deadlock was observed in any run, which is evidence
  but not proof.
- Core sharding (`FELIX_CORE_SHARDS`) is disabled by default and was not
  exercised; its handoff path is unmeasured here.
