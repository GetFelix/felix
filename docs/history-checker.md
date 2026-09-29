# The history checker

A Jepsen-style check of what clients actually observed. Concurrent clients
append to and read `Quorum` streams, and put to and get keys of a `Quorum`
cache, on a real three-broker cluster while a
nemesis kills, pauses and partitions brokers, and in the long runs also cuts
links, skews clocks and fails fsyncs. Every operation is recorded with
when it started, when it ended and what it returned. Once the faults are healed,
the checker compares that history with what a replicated append-only log
promises.

It lives in `crates/testing/felix-cluster/src/history/`. The model is Elle's
list-append: each list is a single-shard stream, and an append adds a unique
value to it. Felix hands a reader the offset of every record, so the checker
never has to infer an order the way Elle does. It reads positions directly,
and most rules reduce to lookups against the final read. Cache keys are
checked as registers (`history/register.rs`).

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
- **put(key, value)** stores a unique value under a cache key. It is **ok**
  when acknowledged and **info** otherwise: a cache put has no answer that
  says it applied nothing, so no put is ever a definite failure.
- **get(key)** returns the value under the key, or a miss. A get that failed
  observed nothing and is not recorded. Keys start absent and are never
  deleted.

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
| 7 | No stale cache reads | A get returns the value of an ok put `u` although, before the get began, a value `w` whose put began after `u`'s was acknowledged was already in effect; or a get misses although some value was already in effect |

Rule 5 covers cache gets too: a get that returns a value no put to that key
wrote, or returns it before its put began, is a phantom.

A value `w` is known to be in effect once its put is acknowledged, or once a
get that returned it completes. Every linearization then orders `u` before
`w` before the get, so returning `u` is stale. Rule 7 is sound, not complete:
it reports only what no linearization explains and does not search for one.
A deposed leader serving a value one write behind its successor is what it
is for.

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
> `random_linearizable_register_histories_are_valid` — cache histories from a
> correct register, with overlapping and unknown puts, pass.
> `a_planted_stale_get_is_caught` — a get returning the value one
> acknowledged put behind is caught in such a history.
> `a_get_of_an_overwritten_value_is_stale`,
> `a_get_older_than_an_earlier_get_is_stale`,
> `a_miss_after_an_acknowledged_put_is_stale`,
> `a_value_nobody_put_is_a_phantom` — one hand-built violation per case.

## The campaign

`Campaign::run` starts six clients, three plain and three idempotent, on three
single-shard `Quorum` streams (`history-0..2`) and three keys (`k0..k2`) of a
single-shard `Quorum` cache (`history-cache`), all replicated across all three
brokers. The control plane's placement loop runs every 500ms so failovers
happen without anyone stepping them.

### Modes

`FELIX_HISTORY_MODE` picks which replication path the brokers take.
`Campaign::start` starts the cluster and, in `lease-free` mode, finalizes the
`generation_start`, `majority_ack` and `lease_free_reads` fleet features, then
waits until every broker reports all three on
(`felix_broker_fleet_feature_enabled`). A broker that does not turn them on
fails the run before any fault, so a lease-free run cannot quietly test the
lease. The features are described in
[replication design](replication-design.md).

| Mode | Stream writes are acknowledged | Cache reads confirm leadership |
| --- | --- | --- |
| `lease` | Once the control plane stored a majority report, with the lease re-checked | With the lease |
| `lease-free` | Once a majority answers at the leader's generation | With a majority round after taking the value |

Unset, the main campaign runs `lease-free` and the every-family campaign runs
`lease`, so every PR exercises both paths. Set, it applies to both.

The nemesis then loops:

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
- **All clients** also put or get a cache key 20% of the time, half each,
  through a random broker, which forwards the operation to the key's leader.

The nemesis also counts the cache's leader among the list leaders it targets.

**To add a fault**, add a `FaultKind` variant (with its family), a `Fault`
variant, and its arms in `Fault::inject`, `Fault::heal` and `Display`
(`history/nemesis.rs`). A fault the harness already has as a
`felix_cluster::Fault` needs only an arm in `Fault::harness_faults`. If it
needs something of the cluster's configuration, say so through `Nemesis`'s
`needs_*` methods. To drive a
campaign with something other than a random schedule, such as a replay of the
faults a failing run printed, implement `Nemesis`.

> `a_fault_campaign_keeps_quorum_histories_valid` — a 45-second lease-free
> campaign of kills, pauses and partitions leaves a valid history, with at
> least 50 acknowledged appends, 10 acknowledged cache puts, 10 cache gets and
> at least one fault injected and healed.
> `every_fault_family_is_injected_and_healed_in_a_campaign` — a 75-second
> campaign on the lease that goes round every kind in a fixed order leaves a valid history
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
| `FELIX_HISTORY_MODE` | `lease-free` for the main campaign, `lease` for the every-family one | `lease` or `lease-free`; see "Modes" |

```bash
FELIX_HISTORY_SEED=1234 FELIX_HISTORY_DURATION_SECS=600 FELIX_HISTORY_MODE=lease-free \
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
it for 20 minutes with a random seed and every fault family, in `lease-free`
mode unless a manual run picks `lease`, and prints the seed and mode first, and
in the job summary, so a red night can be replayed. Setting
`FELIX_HISTORY_DURATION_SECS` is what switches the main campaign to every
family, so replay a nightly seed with it and the mode set. A manual run:

```bash
gh workflow run history.yml -f mode=lease-free -f duration_secs=1200
```

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
