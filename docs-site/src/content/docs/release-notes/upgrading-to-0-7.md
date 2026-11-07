---
title: Upgrading to 0.7
description: What an operator moving a cluster from 0.6.2 to the 0.7 line needs to know. 0.7 is not released, so this page is a working draft.
---

:::caution[0.7 is not released]
This page covers `main`, which is ahead of the newest preview,
`0.7.0-preview.2`. Items marked *(main)* are not in a preview yet. The page is
finalized when 0.7.0 is tagged. Do not plan a production upgrade from it yet.
The released versions are in the [release notes](/release-notes/).
:::

Read [Upgrades and compatibility](/deployment/upgrades/) for the rules these
notes rely on: the five compatibility surfaces, the upgrade order, fleet
features and rollback.

## Breaking changes

- **Lease-token templates take over braces in RBAC objects** *(main)*. A policy object
  that holds `{` or `}` is now a lease template. A grant on a literal cache key
  with braces, such as `cache:t1/prod/c/{leases}`, stops granting that key. It
  instead lets the holder of key `K` in cache `t1/prod/leases` get a lease
  token for `c/K`. Before upgrading, list your grants for objects with braces.
  Rename those keys so they hold none, and update the grants. The control plane
  also refuses to write a malformed template.
- **Rust client, renamed.** `ClusterClient` is now `Client`, and the old
  single-connection `Client` is now `Connection`. Rename in this order, or the
  second rename undoes the first: `Client` to `Connection`, then `ClusterClient`
  to `Client`. Code that connected to one address with the old `Client` should
  use the new `Client` with that address as its only seed.
- **Rust crates that embed Felix.** `StreamLog::base_offset` is now
  `physical_base`, beside `readable_base`. `Broker::allow_head_trims_when`
  replaces `set_replication_enabled`. `SubscribeCursorError` gains
  `trim_headers` and is no longer `Copy`. On `main`, the `felix-wire` message types also gained fields
  (`record_headers`, `client_flags_ext`, `server_flags_ext`), so struct literals
  and exhaustive matches need them. The wire bytes for a client that
  does not use these are unchanged.
- **Storage format 7 and 8.** `FORMAT_VERSION` was 6 in 0.6.2, is 7 in
  preview.2 and is 8 on `main` *(v8: main)*.
  - Version 7 holds the head-trim record. Version 8 holds a record's key and
    headers.
  - A broker moves a log onto a version only to hold such a record: v7 at the
    first trim (in a cluster, once `head_trim` is finalized), v8 at the first
    publish with headers. Until then the log stays readable by 0.6.2.
  - After that, 0.6.2 refuses to open the log. There is no rollback except
    restoring a backup.
  - `FELIX_RECORD_HEADERS=false` keeps v8 off. It does not affect v7.
  - A single broker accepts headers from the start. A cluster accepts them
    once it finalizes `record_headers` (see
    [`record_headers`](/deployment/upgrades/#record_headers)).
  - A single broker allows trims from the start and writes a `head-trimmed`
    marker. That data directory cannot join a cluster until the cluster has
    finalized `head_trim` (see [`head_trim`](/deployment/upgrades/#head_trim)).

## Wire protocol *(main)*

Record headers are negotiated, so 0.6.2 clients and brokers keep working. A
frame flag, `FLAG_EXTENDED`, opens an extension word, and `Auth` and `AuthOk`
carry `client_flags_ext` and `server_flags_ext` to agree on
`EXT_RECORD_HEADERS`. A peer that does not offer it exchanges the same bytes as
before. The Rust client offers it; the Python, TypeScript, C and C# clients do
not yet. Header names starting with `felix.` are reserved.

A pipelining client can also offer `FEATURE_PUBLISH_ANSWER_ON_COMPLETION`
*(main)*. A broker that grants it a publish window then answers plain acked
publishes as they complete and keeps request order only among idempotent
ones, so a shard stalled during a failover no longer holds back the answers
of every other shard on its stream. Older clients and brokers exchange the
same frames as before. The Rust client offers it; nothing to configure.

## New settings

None of these is required. Each default keeps the 0.6.2 behavior, except
`FELIX_SHARD_STUCK_LEADER_MS` and `FELIX_UNIFIED_FENCE`, which are on by
default.

| Setting | What it does |
|---|---|
| `FELIX_RECORD_HEADERS` *(main)* | Single broker only. `false` refuses publishes that carry headers, which keeps logs at v7 or older. Turning it off later leaves stored headers readable. A cluster follows the `record_headers` fleet feature instead. |
| `FELIX_KAFKA_RECORD_HEADERS` *(main)* | `true` stores a Kafka producer's keys and headers with the record, which moves the logs to v8. Off by default, so the Kafka listener alone never does. |
| `FELIX_MQTT_LISTEN`, `FELIX_MQTT_WS_LISTEN` *(main)* | Bind the MQTT listener, plain and over WebSockets. Unset leaves it off, and the broker is then unchanged. Devices can publish; subscribing arrives in 0.8.0. Related `FELIX_MQTT_*` limits are in [the reference](/reference/environment-variables/). |
| `FELIX_LEASE_TOKEN_KEY_FILE`, `FELIX_LEASE_TOKEN_MAX_TTL_SECONDS` *(main)* | Key file and longest lifetime for lease tokens. Without the key file a broker neither mints nor accepts them. Every broker needs the same file. |
| `FELIX_INTERNAL_ELECTION` | Broker. `true` makes the broker offer replica elections. Every broker needs it before `joint_fence` and `replica_election` can be finalized, and keeps needing it after. See [Turning on failover without the control plane](#turning-on-failover-without-the-control-plane). |
| `FELIX_SHARD_ELECTION_HOLD_MS`, `FELIX_SHARD_PENDING_SET_TIMEOUT_MS` | Control-plane timers for `replica_election`. Nothing uses them until that feature is finalized. |
| `FELIX_SHARD_STUCK_LEADER_MS` | Control plane, 70 s by default. A `Quorum` shard whose leader is on a live broker but shows no progress at its generation for this long is named again one generation up, on the same set. In 0.6.2 such a shard stayed stuck until something else changed its assignment. `0` turns it off. See "The control plane's backstop" in `docs/replication-design.md`. |
| `FELIX_SHARD_SUSPECT_AFTER_MS` | Broker, 120 s by default, used only under `replica_election`. A follower whose copy holds a generation that never started, and that has heard nothing from the shard's leader for this long, may stand for that one shard though the leader's broker is up. `0` turns it off. |
| `FELIX_UNIFIED_FENCE` | Broker, on by default. A broker promoted to lead a shard counts its fence by what each replica attests about its copy: whether it kept its ballots and whether it holds every record it acknowledged. That lets a failover open past a copy doubted after a crash, which the older rules could leave waiting, as long as the trusted copies still cover every majority. Answers from 0.6.2 brokers count as they always did, so a rolling upgrade is safe in either order. A memory-only broker counts as before. A broker started without the control plane (`FELIX_START_WITHOUT_CONTROLPLANE`) does not vote or count toward a promotion until it reaches the control plane, so a failover that needs its vote waits for the control plane. `0` turns it off and restores the older pin-based rule, under which that vote counts. See "Fencing a promotion" in `docs/replication-design.md`. |

In a cluster, headers are refused until the fleet finalizes `record_headers`,
whatever `FELIX_RECORD_HEADERS` says.

## Upgrade order

Before you start, list your RBAC grants for objects with braces (see the first
breaking change).

The ordinary order applies: control plane, then brokers one at a time, then
clients. Back up the data directory first if you may use headers or head trim,
because both write formats 0.6.2 cannot read. The first MQTT publish to a mounted stream also stores record headers, so it counts as using headers.

The `head_trim` and `record_headers` fleet features are new. They stay off after
the brokers are upgraded, and nothing needs them unless you want to trim a
stream or store record keys and headers in a cluster. To turn one on, once
every broker runs the new build:

```bash
felixctl features finalize head_trim --dry-run
felixctl features finalize head_trim --yes
```

Use `record_headers` in place of `head_trim` for headers. The two do not
depend on each other or on the fleet features from 0.6.

The dry run names any live or draining broker that lacks the feature. A broker
that is down is not counted and is refused later if it returns on the old
build, so replace or retire it first. After the finalize, watch
`felix_broker_fleet_feature_enabled{feature="head_trim"}` (or `record_headers`) reach 1 on each
broker, or read `GET /v1/fleet/features`.

Finalizing is one-way. A cluster created on the new build finalizes it at the
first broker registration. See [`head_trim`](/deployment/upgrades/#head_trim)
and [`record_headers`](/deployment/upgrades/#record_headers)
for what each writes and what an older broker does with it, and
[Rollback](/deployment/upgrades/#rollback) for the limits above.

A rollback can also trip on a shard's replica set file. A 0.7 broker that has
not yet read whether `joint_fence` is finalized, for example right after a
restart, keeps older replica sets in that file, because dropping them is unsafe
if the fleet did finalize it. An older build refuses such a file and fails to
open that shard's log. Start the broker on 0.7 again, let it learn that
`joint_fence` is off and write the shard's next replica set, which drops them,
then roll it back.

## Control plane changes

A leader promoted by a failover now reports, for each follower it has not
reached yet, the storage identity that its own promotion named the follower by.
The next failover names the follower by that identity, so a second failover
soon after the first no longer leaves the shard closed (#1574).

- With the Postgres backend, the control plane runs migration `0037` when it
  starts. Nothing else is needed.
- With the Raft backend, the new field is kept only once every member is at
  metadata level 20. Until the last member is upgraded, a follower named this
  way does not have its fence answer counted, as in 0.6.2, and a shard that
  needs that answer waits. Upgrade every control-plane member before the
  brokers, and see [Rollback](/deployment/upgrades/#rollback) for the limit on
  going back once level 20 entries are written.

The control plane can now keep a cut-over's new replica set pending until
the leader reports it on both the old and the new set, and adopt a leader
its replicas elected, both of which `replica_election` needs (#1009).
Nothing uses them until that feature is finalized.

- With the Postgres backend, the control plane runs migration `0038` when it
  starts. Finish upgrading every control-plane instance before finalizing
  `replica_election`: the control plane cannot see another instance's version,
  and one that predates this drops a pending set when it writes the shard.
- With the Raft backend, both need every member at metadata level 21.
  Finalizing `replica_election` needs level 26 (below).

Later control-plane changes are on `main`:

- Every placement write names each member's registered storage id
  (`member_storage_ids`): Postgres migration `0039`, Raft metadata level 22.
- A copy whose storage was replaced is a learner until it is re-admitted, and
  a leader reports where its generation started (`holds_start`): migration
  `0040`, level 23. Finalizing `joint_fence` needs level 25 (below), and
  `generation_start`, `stream_incarnations` and `cache_incarnations`
  finalized first. A broker reports `joint_fence` only with
  `FELIX_INTERNAL_ELECTION=true`, so it is part of turning on failover
  without the control plane (below).
- A stream can carry an MQTT mount: migration `0041`, level 24.
- A leader that could not serve a generation because it had promised it to
  another node reports the refusal, and placement names it again at a higher
  generation (#1730): migration `0042`, level 25.
- After `joint_fence` is finalized, a leader restarted at a generation that
  started before the finalize cannot learn the replica set that generation
  replaced. It reports the generation refused, and placement names it again
  one generation up. The report uses the `refused` field above with no node
  named, so it needs migration `0042` and level 25, and finalizing
  `joint_fence` under Raft waits for level 25. Rolling restarts after the
  finalize need no extra step. A new shard a broker first sees in a snapshot
  opens on the snapshot's `first_assignment` mark, which a 0.7 control plane
  sends. With the usual upgrade order (control plane first) every broker gets it.
- An elected leader is adopted on the replica set it was elected on: level
  26, with no Postgres migration. Finalizing `replica_election` needs that
  level. Level 27 does the same when the elected leader is the one the
  control plane already names; below it, the same report moves the shard
  past the election at once, and the next report is only a fallback.
- A leader that answered another candidate's fence, when the control plane
  never adopted that candidate, reports the candidate's ballot with its refusal, and
  placement names it again one past that ballot (#1951): migration `0043`,
  level 28. Below it the leader is named one generation up, as before, each
  rename waits the same 10 s, and it can take several renames to get past
  the ballot.
- A placement write that lands more than one generation up records the
  generation it replaced (`previous_generation`, #2024), so a leader renamed
  past a withdrawn candidate's ballot does not leave its joint fence pending
  for minutes: migration `0045`, level 29. Below it the broker learns no set
  for the skipped generation, as before.
- Postgres refuses a mount on a non-durable stream in the schema itself:
  migration `0044`, with no Raft level.
- When the leader of a set that re-admitted a replaced copy reports it caught
  up, the control plane records that on the assignment (`readmitted`):
  migration `0046`, level 30. Below level 30 nothing is recorded. No broker
  reads it yet.

In short, Postgres runs migrations `0037` to `0046` on start (`0039` to
`0046` are on `main` only), and under Raft these features wait for metadata
levels 20 to 30. Upgrade every control-plane member before the brokers.

## Turning on failover without the control plane

In 0.7.0 a shard's replicas can elect a new leader themselves when the old one
dies while the control plane is down. It stays off after the upgrade. What it
does and its limits are in [Failover without the control
plane](/deployment/failover-without-control-plane/). To turn it on, after the
upgrade above:

1. Finish upgrading every control-plane member. Under Raft the finalizes below
   check it (level 25 for `joint_fence`, 26 for `replica_election`). Under
   Postgres nothing checks it, so make sure no instance runs an older build.
2. Set `FELIX_INTERNAL_ELECTION=true` on every broker and restart them one at
   a time. Nothing changes yet.
3. Finalize, if they are not already, `generation_start`, `majority_ack`,
   `lease_free_reads`, and `fenced_caches` if you want caches elected. Each
   has its own cautions in [Upgrades and
   compatibility](/deployment/upgrades/#runbook-rolling-out-a-fleet-feature).
4. Finalize `stream_incarnations` and `cache_incarnations`. `joint_fence` is
   refused until both are finalized.
5. Finalize `joint_fence`, dry run first:

   ```bash
   felixctl features finalize joint_fence --dry-run
   felixctl features finalize joint_fence --yes
   ```

   From here a broker without `FELIX_INTERNAL_ELECTION=true` is refused at
   registration, so keep it set on every broker, new ones included.
6. Finalize `replica_election` the same way.
7. Check that `felix_broker_fleet_feature_enabled{feature="replica_election"}`
   is 1 on every broker. A broker with `FELIX_LEADER_SUSPECT_AFTER_MS=0` still
   never stands, so leave that setting at its default or above zero.

A cluster created on 0.7.0 has already finalized steps 3 and 4 at its first
broker registration, so it needs steps 2, 5, 6 and 7 only.

## Clients

The Rust client has the rename above. The other clients are additive: every
binding takes four reconnect options (an attempt count, a deadline for one
retried call, and the backoff and max backoff between attempts), unset values
keep the Rust defaults, and `trim_before` is new. On `main`, cursor errors in every binding also carry
the reason, the nearest available offsets and the trim's headers. The
C struct `FelixConnectOptions` gained `reconnect_attempts`,
`reconnect_deadline_ms`, `reconnect_backoff_ms` and `reconnect_max_backoff_ms`
after `token_release`; a program built against the older `felix.h` keeps the
defaults, since zero means default.

Cache and consumer-group calls on the Rust `Client` now follow
`ReconnectPolicy`, as publishes already did. The limits bound the whole call,
and a write is sent again only after a refusal that says nothing was applied.

A `Connection` opens its pooled connections as its traffic needs them
*(main)*. A
subscriber with default settings now holds 5 broker connections instead of 20,
so about 100 fit under the default `FELIX_MAX_CONNECTIONS_PER_IP=512` instead
of about 25. No action is needed; the pool sizes are ceilings.

## What else is new

A client can trim a stream's oldest records at the tail it expects
(`trim_before`, `felixctl stream trim`); a cursor error now says where to resume
and carries the trim's headers; a single broker accepts and delivers record
headers, on a cluster once it finalizes `record_headers`; the Rust client has
lease tokens; and a broker can serve MQTT, where a device can publish at QoS 0
and 1 (the last four are on `main`). See
[what Felix is for](/getting-started/what-felix-is-for/) for the status of each.

## Packaging

Each GitHub release now carries prebuilt `libfelix` C libraries, shared and
static, with `felix.h` and a `LINKING.txt`, for Linux, macOS and Windows on x64
and arm64. See [the C client](/clients/c/).
