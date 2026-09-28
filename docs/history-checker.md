# The history checker

A Jepsen-style check of what clients actually observed. Concurrent clients
append to and read `Quorum` streams on a real three-broker cluster while a
nemesis kills, pauses and partitions brokers, and in the long runs also cuts
links, skews clocks and fails fsyncs. Every operation is recorded with
when it started, when it ended and what it returned. Once the faults are healed,
the checker compares that history with what a replicated append-only log
promises.

It lives in `crates/testing/felix-cluster/src/history/`. The model is Elle's
list-append: each list is a single-shard stream, and an append adds a unique
value to it. Felix hands a reader the offset of every record, so the checker
never has to infer an order the way Elle does. It reads positions directly,
and most rules reduce to lookups against the final read.

## The model

- **append(list, value)** ends in one of three ways:
  - **ok**: acknowledged.
  - **fail**: definitely not written. Only a broker's own typed refusal with
    retry class `retry`, `retry_after` or `redirect` counts as a fail, since
    those classes say the broker applied nothing.
  - **info**: unknown. A timeout, a lost connection, `outcome_unknown`, or any
    error from an idempotent producer. A producer's error can follow an earlier
    attempt that did land, so none of its errors are definite.
- **read(list, from)** subscribes at an offset and reads up to the tail the
  broker reported when the subscription was registered. It records the
  `(offset, value)` pairs it saw. A quarter of reads start at the beginning of
  the list. The rest start somewhere in the last 256 records, which keeps a long
  campaign's history small.
- **The final read** of each list is taken from its leader after every fault is
  healed. It must reach that tail with no holes, or the campaign retries it.
- Records below each list's **base** were in the stream before the run (the
  harness's readiness probe), and the checker ignores them.

Times come from one monotonic clock that every client shares, so "A completed
before B was invoked" means the same thing to every client.

## What it checks

| # | Rule | Broken when |
| --- | --- | --- |
| 1 | No lost acknowledged writes | An ok append is missing from its list's final read |
| 2 | No duplicates | A value sits at two offsets, in one read or across reads |
| 3 | Reads are prefixes of the final log | An offset holds different values in two reads, a read's offsets go backwards, a read sees past the end of the final log, or an append acknowledged at an offset is found at another |
| 4 | Real-time order | A was acknowledged before B was invoked, both landed on the same list, and A's offset is after B's |
| 5 | No phantoms | A read sees a value no append wrote, a value appended to another list, or a value before its append began |
| 6 | Failed writes stay absent | A read sees a value whose append was answered as a definite failure |

Two more findings concern the harness rather than the broker.
`incomplete-final-read` means the final read has a hole. When that happens,
rule 1 cannot be trusted for that list. `malformed-history` means the recorder
itself is wrong, for example because one value was appended twice.

A read that is *missing* a record in the middle is not a violation. Subscribers
may drop records by design (`DropNew`), and the offsets show the gap. What a
read does hold has to agree with the final log.

For rule 4, A must be acknowledged, because an unknown append can land at any
time. B can have any outcome, as long as it landed: a record cannot be written
before it is sent.

**Consistency levels.** A list is either `Quorum` or `Leader`. Under `Leader`,
a failover may drop an acknowledged suffix and write new records at those
offsets (see [semantics](semantics.md), "Consistency"). So for `Leader` lists the
checker skips rule 1 and the parts of rule 3 that compare offsets across reads.
The campaign only runs `Quorum` streams. Under `Quorum`, readers stop at the
quorum mark, so a read that sees past the end of the final log is a real
violation.

> `random_linearizable_histories_are_valid` — histories generated from a
> correct log, with concurrency, unknown outcomes and dropped records, pass.
> `breaking_a_valid_history_is_caught` — losing, duplicating or failing one
> acknowledged value in such a history is caught.
> `an_acknowledged_append_missing_from_the_final_read_is_lost`,
> `a_value_twice_in_the_final_read_is_a_duplicate`,
> `a_read_that_disagrees_with_the_final_log_is_not_a_prefix`,
> `an_append_acknowledged_first_must_sit_first`,
> `a_value_no_append_wrote_is_a_phantom`,
> `a_definitely_failed_append_must_not_appear` — one hand-built violation per
> rule, each reported under that rule and no other.

## The campaign

`Campaign::run` starts six clients, three plain and three idempotent, on three
single-shard `Quorum` streams (`history-0..2`) replicated across all three
brokers. The control plane's placement loop runs every 500ms so failovers
happen without anyone stepping them. The nemesis then loops:

1. Wait 1-4s.
2. Pick a fault. It targets a list leader 75% of the time and any broker
   otherwise.
3. Hold the fault for 2-6s.
4. Heal it.

Faults never overlap, so a majority is always one fault from whole. That is
also what makes a broker clock at half speed safe to inject: it runs slow only
while its heartbeats keep landing, never alongside a partition.

`RandomNemesis::process_faults` picks from the first three kinds below, and is
what the per-PR run uses. `RandomNemesis::all_faults` picks from all of them,
and is what a run given `FELIX_HISTORY_DURATION_SECS` uses, the nightly one
included.

| Family | Kind | Injected | Healed |
| --- | --- | --- | --- |
| Process | `Kill` | `SIGKILL` | Restart on the same data directory |
| Process | `Pause` | `SIGSTOP` | `SIGCONT` |
| Process | `Partition` | Partition file, both ways, to every peer | File removed |
| Link | `DropOutbound` | Everything the broker sends its peers is lost | Link restored |
| Link | `DelayOutbound` | Everything the broker sends its peers is 250ms late | Link restored |
| Link | `DropControlPlaneReplies` | The control plane's replies to the broker are lost | Link restored |
| Clock | `ClockRate` | The broker's lease clock runs at 0.5x or 20x | Back to 1x, drift kept |
| Clock | `ControlPlaneClockStep` | The control plane's wall clock jumps 15s forward | Stepped back to the true clock |
| Disk | `SlowFsync` | Each of the broker's flushes waits 200ms | Delay removed |
| Disk | `FsyncFailOnce` | The broker's next flush fails with `EIO` | Fault removed, broker restarted |

The non-process kinds go through the harness's `Cluster::inject` (see
[the cluster harness](cluster-harness.md), "The fault API"). There are two
types named `Fault`: `felix_cluster::Fault` is the harness's vocabulary, and
`felix_cluster::history::Fault` is the campaign's, a fault with its target
chosen and a `Display` for the timeline. The campaign's clock rate is stored
in permille so it can stay `Eq`.

A broker's clock is never stepped back: its lease runs on `CLOCK_BOOTTIME`,
which cannot go backwards, and the harness refuses such a step. Healing a
failed fsync restarts the broker because a failed fsync poisons the log until
the process restarts; healing the disk alone would leave it refusing every
write. Healing the control-plane step is a backward step. That leaves
every heartbeat stamp in the future, and the expiry sweep pulls them back to
its clock, so a broker that dies right after it still goes down within one
window (`a_control_plane_clock_stepped_back_still_expires_a_dead_broker`).

`Campaign::cluster_config` takes the nemesis and starts the cluster for it:
with proxied links when it may pick a link fault, and with
`FELIX_DURABLE_FSYNC_MODE=on_commit` and `FELIX_ACK_ON_COMMIT=true` when it may
pick a disk fault, so a failed fsync can fail an acknowledgement rather than
happen behind one.

- **Plain clients** use `ClusterClient::publish`, which never re-sends a publish
  that may have landed. Only these clients ever record a definite failure, so
  they are what exercise rule 6.
- **Idempotent clients** re-send the same batch under the same sequence, up to
  three times, and record an unknown outcome if all three fail. A cancelled
  publish leaves the producer refusing all later sends, so the client replaces
  it.
- **All clients** read 20% of the time, from a random broker. They follow one
  `not_leader` hop. A client reconnects from a fresh address book after three
  failures in a row, because a restarted broker listens on new ports.

**To add a fault**, add a `FaultKind` variant (with its family), a `Fault`
variant, and its arms in `Fault::inject`, `Fault::heal` and `Display`
(`history/nemesis.rs`). A fault the harness already has as a
`felix_cluster::Fault` needs only an arm in `Fault::harness_faults`. If it
needs something of the cluster's configuration, say so through `Nemesis`'s
`needs_*` methods. To drive a
campaign with something other than a random schedule, such as a replay of the
faults a failing run printed, implement `Nemesis`.

> `a_fault_campaign_keeps_quorum_histories_valid` — a 45-second campaign of
> kills, pauses and partitions leaves a valid history, with at least 50
> acknowledged appends and at least one fault injected and healed.
> `every_fault_family_is_injected_and_healed_in_a_campaign` — a 75-second
> campaign that goes round every kind in a fixed order leaves a valid history
> and injects and heals at least one fault of each family.
> `all_faults_never_steps_a_broker_clock_back` — the nemesis never asks for a
> step the harness would refuse.

## Running it

It runs as part of `cargo test -p felix-cluster`, and so of `task test`.

```bash
cargo build -p felix-broker-service --bin felix-broker   # the harness runs the prebuilt binary
cargo test -p felix-cluster --test history -- --nocapture
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `FELIX_HISTORY_SEED` | a fixed seed | The schedule's seed: a number, or `random` |
| `FELIX_HISTORY_DURATION_SECS` | `45` | How long the nemesis runs. When set, the main campaign uses every fault family |

```bash
FELIX_HISTORY_SEED=1234 FELIX_HISTORY_DURATION_SECS=600 \
    cargo test -p felix-cluster --test history -- --nocapture
```

The seed fixes the fault schedule and each client's choice of operation and
list. It does not fix the interleaving, which depends on the scheduler and the
brokers. A failing seed makes the failure likely to recur, not certain. Run it
a few times.

On a small disk, set `FELIX_DURABLE_PREALLOCATE=false` so each broker does not
reserve full segments up front.

The per-PR run uses the fixed seed and takes about a minute per test, cluster
start-up included. The nightly workflow (`.github/workflows/history.yml`) runs
it for 20 minutes with a random seed and every fault family, and prints the
seed first, so a red night can be replayed. Setting
`FELIX_HISTORY_DURATION_SECS` is what switches the main campaign to every
family, so replay a nightly seed with it set.

## Reading a violation

A failing run prints the seed, a one-line summary, each violation, and the
fault timeline:

```text
seed 1234: the history broke the rules; rerun with FELIX_HISTORY_SEED=1234
1 violation(s): 1 lost-write x1
  [1 lost-write] history-2: op #412 client 0 append 377 to history-2 -> ok [20113.4ms..20131.9ms] was acknowledged but the final read (offsets 1..=164) does not have it; op #430 had read it at offset 97
fault timeline:
  18002.1ms start pause broker-2
  22950.7ms healed pause broker-2
```

Each violation names:

- **The rule**, numbered as in the table above.
- **The list.**
- **The operations involved**, as `op #N`, an index into the history. Each is
  shown with its client, value, outcome and `[invoke..complete]` span in
  milliseconds since the run began.
- **The offsets** that disagree.

Put the spans next to the fault timeline. A loss whose acknowledgement arrived
just before a leader was paused or partitioned points at the acknowledgement
path. A duplicate from an idempotent client points at sequence handling across
a leader change. The brokers' logs are printed too, because the test fails
while the cluster is still up.
