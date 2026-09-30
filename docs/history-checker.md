# The history checker

A Jepsen-style check of what clients actually observed. Concurrent clients
append to and read `Quorum` streams, and put to and get keys of a `Quorum`
cache, on a real three-broker cluster while a
nemesis kills, pauses and partitions brokers, and in the long runs also cuts
links, skews clocks, fails fsyncs, moves shards and drains brokers. An
adversarial nemesis overlaps faults, forces failovers, cuts moves and
restarts short, cuts the power to every broker and crashes the control plane
with work in flight. Every operation is recorded with
when it started, when it ended and what it returned. Once the faults are healed,
the checker compares that history with what a replicated append-only log
promises. During the run, after every heal, the campaign also checks that the
cluster serves again (see "Liveness").

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
- **commit(list, value)** is an [atomic commit](atomic-commit.md): one record
  that appends `value` to the list and sets the list's state to `value`. It is
  recorded as an append too, with the same outcomes, so rules 1 to 6 cover
  its event. A commit is never re-sent.
- **Live subscribers.** One per list, subscribed from the list's base for the
  whole run, recording every event it is delivered with its offset. See
  "Live subscribers".
- **commit read(list)** reads the list to its tail, then the list's state
  (the value and the offset of the commit that wrote it), then the list again
  from that offset. A commit read whose first read fell short of its tail, or
  whose state read failed, is not recorded.

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
| 8 | No partial commits | A commit read's first read saw a commit's event and the state it read next is older than that commit (the event without the state); or the state names a commit whose event is not at the state's version in the read that followed (the state without the event), or a value no commit wrote |
| 9 | Delivery order | A live subscriber is delivered an offset at or below one it already had, in the same session or after a resume, or below where it started |
| 10 | No lost deliveries | A live subscriber is delivered a record the final log does not hold at that offset: another value is there, no record is, or the log ends before it |
| 11 | No missing deliveries | A record in the final log sits at an offset a subscriber was told holds no event (`skipped_before`), or past the last offset the subscriber was delivered once it had time to catch up |

Rule 8 leans on the order of the three looks. Every commit event the first
read saw was committed before the state was read, so the state must be at
least that commit; a commit landing in between can only make the state newer,
which is allowed. The state's version is an offset, so the read after it must
hold that commit's event there, whatever landed since.

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

> `random_linearizable_histories_are_valid`: histories generated from a
> correct log, with concurrency, unknown outcomes and dropped records, pass.
> `breaking_a_valid_history_is_caught`: losing, duplicating or failing one
> acknowledged value in such a history is caught.
> `an_acknowledged_append_missing_from_the_final_read_is_lost`,
> `a_value_twice_in_the_final_read_is_a_duplicate`,
> `a_read_that_disagrees_with_the_final_log_is_not_a_prefix`,
> `an_append_acknowledged_first_must_sit_first`,
> `a_value_no_append_wrote_is_a_phantom`,
> `a_definitely_failed_append_must_not_appear`: one hand-built violation per
> rule, each reported under that rule and no other.
> `random_linearizable_register_histories_are_valid`: cache histories from a
> correct register, with overlapping and unknown puts, pass.
> `a_planted_stale_get_is_caught`: a get returning the value one
> acknowledged put behind is caught in such a history.
> `a_get_of_an_overwritten_value_is_stale`,
> `a_get_older_than_an_earlier_get_is_stale`,
> `a_miss_after_an_acknowledged_put_is_stale`,
> `a_value_nobody_put_is_a_phantom`: one hand-built violation per case.
> `a_reader_that_sees_both_halves_is_valid`,
> `the_event_without_the_state_is_a_partial_commit`,
> `the_state_without_the_event_is_a_partial_commit`: rule 8, both ways round.
> `a_whole_delivery_across_a_resume_is_valid` and
> `a_visible_drop_is_counted_not_reported`: a subscriber that had everything,
> or dropped a record the offsets show, passes.
> `offsets_delivered_out_of_order_break_delivery_order`,
> `a_resume_that_redelivers_breaks_delivery_order`,
> `a_delivered_record_the_final_log_lacks_is_lost`,
> `a_skip_over_a_record_is_a_missing_delivery` and
> `a_subscriber_that_stops_short_is_a_missing_delivery`: rules 9 to 11, one
> case each.

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
[replication design](replication-design.md). Both modes also finalize
`atomic_commit`, which changes no acknowledgement or read path and only lets
the clients commit.

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

5. Wait until every shard serves again (see "Liveness").

The single fault kinds never overlap, so a majority is always one fault from
whole. That is also what makes a broker clock at half speed safe to inject: it
runs slow only while its heartbeats keep landing, never alongside a partition.
The compound kinds below break that rule on purpose.

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
| Assignment | `MoveShard` | An operator moves one of the workload's shards to another broker | Wait for the move to finish |
| Assignment | `Drain` | An operator drains the broker | Put back to live, wait for the moves in flight |

The link, clock and disk kinds go through the harness's `Cluster::inject` (see
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

The assignment kinds go through the control plane's operator API, as an
operator would, while the placement loop keeps running. `MoveShard` picks a
stream or cache shard the target broker leads (any shard, if it leads none)
and starts a move to another broker with `POST /v1/shard-moves`. Every shard
is on all three brokers, so the destination is always one of its replicas and
the move goes through the planned handoff: staged, fenced, cut over. `Drain`
marks the broker draining, so placement moves its leaderships off it. It
keeps its seat as a follower of the shards it only followed, since there is no
fourth broker to reseat them on, but each shard it led cuts over without it.
Healing marks it live again. Both heals then wait up to 60s (times `FELIX_TEST_TIMEOUT_SCALE`)
for every shard to have no move in flight, placement's own included, so the
next fault never lands mid-move. A move still running by then fails the
campaign as a stall.

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

### Compound faults

`RandomNemesis::adversarial` picks from the kinds below. They cover what a
single fault healed before the next cannot reach: failures at the same time, a
leader lost while clients write, replica sets that change, moves cut short,
recovery from a half-finished state, partitions beside slow links, brokers
restarted over and over, a whole cluster losing power, and a control plane
that crashes in the middle of a change. Their code is in
`history/nemesis/compound.rs`.

| Kind | Injected | Healed |
| --- | --- | --- |
| `Isolate` | A leader is cut off from its peers (partition file) and from the control plane (proxy, both ways), then the nemesis waits up to 20s for the control plane to fail its shards over | Links restored |
| `KillTwo` | Two brokers killed at once | Both restarted |
| `PartitionAndDelay` | One broker partitioned, and everything another sends its peers 250ms late | Both healed |
| `Overlap` | Two faults at once on different brokers, from kill, pause, partition, a dropped or delayed link, slow fsync, a move and a drain | Both healed, the move or drain last |
| `InterruptedMove` | A move of one of the workload's shards starts, and 300ms later its source or its destination is killed | Restarted, then wait for moves to finish |
| `RestartLoop` | A broker is killed | Restarted and killed again 500ms later, twice, then restarted for good |
| `TornWrite` | The broker's next segment write lands half its batch and fails with `EIO` | Fault removed, broker restarted |
| `Drain` | As above | As above |
| `PowerLoss` | Every broker loses power: each builds the storage a reboot would find, where writes no flush covered are lost, torn or zeroed as the seed decides, and dies; that image replaces its storage. They stay down for the hold | Every broker started again at once |
| `ControlPlaneCrash` | A move, a drain or the kill of a leader starts, and 300ms later the control plane stops with its state kept | Control plane restarted on the same address and store, placement running again; then the move or drain waits to finish, or the killed broker is restarted |

A `PowerLoss` is only as strong as what it keeps, so the campaign runs it with
brokers that flush on every commit and acknowledge only after the flush
(`FELIX_DURABLE_FSYNC_MODE=on_commit`, `FELIX_ACK_ON_COMMIT=true`). Under that
configuration every acknowledged write the workload makes is flushed first:
stream appends on the leader and on each follower before it acknowledges the
leader, atomic commits, which are a single record on the same path, and cache
puts. Replica state and the generation history are written to a temporary
file, flushed, renamed and their directory flushed. So the lost-write rule
applies unchanged: an acknowledged append or cache put missing after a power
loss is a write acknowledged before it was durable.

The model is the storage crate's power-loss layer (`io/power_loss.rs`, the one
the storage power-loss suite uses), armed in a debug broker by
`FELIX_STORAGE_POWER_LOSS_ROOT` and triggered through the storage fault file;
see [the cluster harness](cluster-harness.md). It only sees flushes that go
through the storage crate's I/O seam, which every broker write the workload
depends on does. It needs Linux, since it reads flushed files through
`/proc/self/fd`. Elsewhere a `PowerLoss` is logged as a fault that could not be
injected. The broker writes the image and dies while holding the model's
lock, so no flush can finish, and nothing can be acknowledged, after the image
was taken.

A `ControlPlaneCrash` is the harness's control plane stopped and started
again over the same in-memory store and keys, which is a restart with its
database intact. The brokers keep serving on the leases they hold until they
lapse, and nothing is placed while it is down. A move or follower replacement
it had started resumes or is dropped once it is back, and the heal waits up
to 60s for no shard to be moving, as it does after a `MoveShard` or `Drain`.
A killed leader's shards fail over only once it is back.

The adversarial nemesis runs on four brokers (`Campaign::with_spare_broker`)
with every list and the cache on three of them. The fourth broker matters in
two ways. A drain now has somewhere to move a follower copy, so it changes the
shard's replica set through a follower replacement (the `joining` path in
[the control plane](control-plane.md)). And two brokers down at once take a
majority of some shards and not of others, so some keep serving while the rest
stop.

An `Isolate` that does not cause a failover within 20s is logged as a fault
that could not be injected, with the shards the broker still leads, and then
healed. A healed `Isolate` in the timeline is therefore a partition that did
cause one. That is the difference from `Partition`, which leaves the broker
heartbeating: the control plane keeps it as leader, and writes to its shards
stall until the heal.

A compound fault is healed in reverse order, except that a move or drain is
healed last, because waiting for its moves to finish needs the brokers the
other fault took away. If one part fails to heal, the rest are still healed
and the first error is reported.

### Liveness

The checker judges safety once the run is over. Liveness is judged during it:
after every heal, `history/liveness.rs` waits up to 60s (times
`FELIX_TEST_TIMEOUT_SCALE`) for each of the workload's shards to meet all of
these:

- its leader is a running broker;
- no move, cut-over or follower replacement is in flight for it;
- if it has replicas, its leader has reported one caught up at the current
  generation, so it could fail over again;
- no running broker lists one of its replicas in `/replication/halted`, since
  a halted replica is out of every quorum until it is rebuilt;
- its leader takes an append (a list) or answers a get (the cache).

The probe append and get are recorded in the history under a client number of
their own, one past the workload's clients, so the checker holds them to the
same rules. After each heal, each shard is probed until it answers once.
Nothing in the check draws from the seed, so it does not change which faults a
seed picks.

When the bound passes, the run stops with one line per stuck shard, naming its
leader, its generation and what it is stuck in, followed by the fault timeline
and the state dump:

```text
liveness: 1 shard(s) not serving 60s after the heal:
  cache history-cache/0 (leader broker-2, generation 9): replacing a follower with broker-3 (drain)
```

The timeline records how long each recovery took, as `every shard serving
1840.2ms later` after each heal.

One stall is known, and it shows up as a halted replica: a stream replica on
a move's destination killed mid-move can refuse the new leader's rebuild
(issue 878). Until it is fixed, an adversarial run may fail on it. The cache
version of this stall (issue 863) is fixed.

### Live subscribers

Reads stop at the tail they were given, so they cannot tell a record that
was never delivered from one that was not there yet. Rules 9 to 11 need a
reader that stays. Once the bases are set, the campaign starts one subscriber
per list (`history/subscriber.rs`), subscribed with
`ClusterClient::subscribe_from` at the list's base, and leaves it running
until the end. Every event it is delivered is recorded with its offset and
`skipped_before`.

Its `ClusterSubscription` follows shard moves and lost connections on its
own. When it gives up with an error, or the broker ends the subscription, the
subscriber connects a fresh client from the current address book and resumes
at one past the last offset it was delivered, retrying every 250ms. Each
resume is recorded with when it happened, where it started and why the
previous session ended, and a violation names the session it happened in.

After the clients stop and the final reads are taken, the campaign waits up
to 60s for each subscriber to be delivered the last offset of its list's final
read, then stops them. A subscriber that has not caught up by then does not
fail the campaign. It is in the history, and rule 11 reports it as stopping
short.

A gap in a subscriber's offsets is a drop when the final log has a value at
one of the skipped offsets and `skipped_before` did not claim it. Drops are
allowed (`DropNew`), and the summary line counts them with the deliveries and
resumes. A gap over offsets that hold no value in the final log, such as
generation-start records, is not a drop. A gap that `skipped_before` claims
holds no event, when the final log has a value there, is a violation: that
record was skipped without the subscriber being able to see it.

Rules 10 and 11 apply to `Quorum` lists only, like rule 1, and deliveries
below a list's base are ignored.

### Acknowledged offsets

Rule 3 includes "an append acknowledged at an offset is found at another", and
that part can only fire for an append whose acknowledgement named an offset.
A publish acknowledgement does not carry one on the wire, so plain and
idempotent appends are recorded without it. Atomic commits do: `CommitOk`
returns the commit's offset, and the workload records it. About one operation
in ten is a commit, and every campaign test fails unless at least five appends
were acknowledged at a known offset, so the rule is exercised on every run.

**To add a fault**, add a `FaultKind` variant (with its family), a `Fault`
variant, and its arms in `Fault::inject`, `Fault::heal` and `Display`
(`history/nemesis.rs`). A fault the harness already has as a
`felix_cluster::Fault` needs only an arm in `Fault::harness_faults`. If it
needs something of the cluster's configuration, say so through `Nemesis`'s
`needs_*` methods. To drive a
campaign with something other than a random schedule, such as a replay of the
faults a failing run printed, implement `Nemesis`.

> `a_fault_campaign_keeps_quorum_histories_valid`: a 45-second lease-free
> campaign of kills, pauses and partitions leaves a valid history, with at
> least 50 acknowledged appends, 10 acknowledged cache puts, 10 cache gets and
> at least one fault injected and healed.
> `every_fault_family_is_injected_and_healed_in_a_campaign`: a 75-second
> campaign on the lease that goes round every kind in a fixed order leaves a valid history
> and injects and heals at least one fault of each family. The order puts a
> shard move fourth and a drain sixth, so both run on every PR.
> `adversarial_faults_heal_and_every_shard_serves_again`: a 150-second
> lease-free campaign on four brokers that goes round the compound kinds
> leaves a valid history, every shard serves again after each heal, and at
> least one `Isolate`, one `KillTwo` and one `RestartLoop` are injected and
> healed. The healed `Isolate` means a partition caused a failover.
> `a_power_loss_or_a_control_plane_crash_loses_nothing_acknowledged`: a
> 60-second lease-free campaign that alternates the two leaves a valid
> history, and injects and heals at least one `ControlPlaneCrash` and, on
> Linux, one `PowerLoss`.
> `a_power_loss_needs_on_commit_flushes_and_the_model` and
> `a_control_plane_crash_interrupts_a_move_a_drain_or_a_failover`: a power
> loss runs only with on-commit flushes, and a control plane crash lands
> during each kind of work it can interrupt.
> `all_faults_never_steps_a_broker_clock_back`: the nemesis never asks for a
> step the harness would refuse.
> `overlapping_faults_are_aimed_at_different_brokers`,
> `an_isolation_is_aimed_at_a_leader`, `an_interrupted_move_kills_one_end_of_it`
> and `assignment_faults_are_healed_last`: the compound kinds pick what they
> say they pick.
> `a_move_goes_from_the_leader_to_another_broker`: a move names one of the
> workload's shards, its current leader, and a different broker.

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
| `FELIX_HISTORY_NEMESIS` | `all` | Which faults the main campaign injects when `FELIX_HISTORY_DURATION_SECS` is set: `all` for every single kind on three brokers, `adversarial` for the compound kinds on four |

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
it for 20 minutes as two jobs, one per nemesis (`all` and `adversarial`), each
with its own random seed, in `lease-free` mode unless a manual run picks
`lease`. Each prints its seed, mode and nemesis first, and in the job summary,
so a red night can be replayed. Setting `FELIX_HISTORY_DURATION_SECS` is what
switches the main campaign off the per-PR schedule, so replay a nightly seed
with it, the mode and the nemesis set. A manual run takes `nemesis=both`,
`all` or `adversarial`:

```bash
gh workflow run history.yml -f mode=lease-free -f duration_secs=1200 -f nemesis=adversarial
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

After the timeline comes a dump of the cluster's state, taken while it is
still up:

```text
cluster state:
  broker-0: running, live, lease held 1, heartbeat age s 0.1, watch checkpoint 87, slowest follower lag 0, halted followers 0, quorum marks withheld 2
  ...
  stream history-0/0: leader broker-1, replicas [broker-0, broker-2], generation 9, assigning
    report at generation 9 (212ms old): leader log end 164, majority at 164; broker-0 at 164 caught up, broker-2 at 161 caught up
  cache history-cache/0: leader broker-2, replicas [broker-0, broker-1], generation 5, assigning
    ...
```

- **Each broker**: whether its process is running, its lifecycle in the
  control plane, and a few of its own metrics. A paused broker shows
  "metrics did not answer".
- **Each of the workload's shards**: the control plane's assignment, with its
  leader, replicas, generation and state, and any move in flight (`moving to`,
  `joining`, and why).
- **The last replica report** its leader sent: the generation it was sent at,
  its age, the leader's log end, and each replica's position. "Majority at" is
  the offset a majority of the copies had reached by that report, which is
  as far as the leader could have acknowledged. Brokers do not export
  per-shard positions or quorum marks (their replication metrics are not
  labelled by shard), so the report is the per-replica view.

The dump is also printed when the campaign cannot finish, for example when a
heal fails or a list has no complete final read. The error then carries the
fault timeline too, since the checker never ran. `Campaign::state_dump` takes
it from any test.

Put the spans next to the fault timeline. A loss whose acknowledgement arrived
just before a leader was paused or partitioned points at the acknowledgement
path. A duplicate from an idempotent client points at sequence handling across
a leader change. The brokers' logs are printed too, because the test fails
while the cluster is still up.
