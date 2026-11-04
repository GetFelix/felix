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
  - Headers work on a single broker only for now, since replication does not
    ship them.
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

## New settings

None of these is required. Each default keeps the 0.6.2 behavior.

| Setting | What it does |
|---|---|
| `FELIX_RECORD_HEADERS` *(main)* | Single broker only. `false` refuses publishes that carry headers, which keeps logs at v7 or older. Turning it off later leaves stored headers readable. |
| `FELIX_MQTT_LISTEN`, `FELIX_MQTT_WS_LISTEN` *(main)* | Bind the MQTT listener, plain and over WebSockets. Unset leaves it off. Related `FELIX_MQTT_*` limits are in [the reference](/reference/environment-variables/). |
| `FELIX_LEASE_TOKEN_KEY_FILE`, `FELIX_LEASE_TOKEN_MAX_TTL_SECONDS` *(main)* | Key file and longest lifetime for lease tokens. Without the key file a broker neither mints nor accepts them. Every broker needs the same file. |
| `FELIX_SHARD_ELECTION_HOLD_MS`, `FELIX_SHARD_PENDING_SET_TIMEOUT_MS` | Control-plane timers for `replica_election`, which no broker offers yet. Nothing uses them today. |

In a cluster, headers are refused whatever `FELIX_RECORD_HEADERS` says, because
the `record_headers` fleet feature is not implemented yet.

## Upgrade order

Before you start, list your RBAC grants for objects with braces (see the first
breaking change).

The ordinary order applies: control plane, then brokers one at a time, then
clients. Back up the data directory first if you may use headers or head trim,
because both write formats 0.6.2 cannot read.

The `head_trim` fleet feature is new. It stays off after the brokers are
upgraded, and nothing needs it unless you want to trim a stream. To turn it on,
once every broker runs the new build:

```bash
felix-controlplane admin features finalize head_trim --dry-run
felix-controlplane admin features finalize head_trim
```

The dry run names any live or draining broker that lacks the feature. A broker
that is down is not counted and is refused later if it returns on the old
build, so replace or retire it first. After the finalize, watch
`felix_broker_fleet_feature_enabled{feature="head_trim"}` reach 1 on each
broker, or read `GET /v1/fleet/features`.

Finalizing is one-way. A cluster created on the new build finalizes it at the
first broker registration. See [`head_trim`](/deployment/upgrades/#head_trim)
for what a trim writes and what an older broker does with it, and
[Rollback](/deployment/upgrades/#rollback) for the limits above.

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
Nothing uses them until that feature is finalized, and no broker offers it
yet.

- With the Postgres backend, the control plane runs migration `0038` when it
  starts. Finish upgrading every control-plane instance before finalizing
  `replica_election`: the control plane cannot see another instance's version,
  and one that predates this drops a pending set when it writes the shard.
- With the Raft backend, both need every member at metadata level 21, and so
  does finalizing `replica_election`.

Later control-plane changes are on `main`:

- Every placement write names each member's registered storage id
  (`member_storage_ids`): Postgres migration `0039`, Raft metadata level 22.
- A copy whose storage was replaced is a learner until it is re-admitted, and
  a leader reports where its generation started (`holds_start`): migration
  `0040`, level 23. Finalizing `joint_fence` needs level 25 (below), and
  `generation_start`, `stream_incarnations` and `cache_incarnations`
  finalized first.
- A stream can carry an MQTT mount: migration `0041`, level 24.
- A leader that could not serve a generation because it had promised it to
  another node reports the refusal, and placement names it again at a higher
  generation (#1730): migration `0042`, level 25.
- After `joint_fence` is finalized, a leader that cannot learn the replica
  set its generation replaced reports that generation refused, and placement
  names it again one generation up. This happens after a broker restart at a
  generation that started before the finalize, and for a new shard a broker
  first sees in a snapshot. The report uses the `refused` field above with no
  node named, so it needs migration `0042` and level 25, and finalizing
  `joint_fence` under Raft waits for level 25. Rolling restarts after the
  finalize need no extra step.

In short, Postgres runs migrations `0037` to `0042` on start (`0039` to
`0042` are on `main` only), and under Raft these features wait for metadata
levels 20 to 25. Upgrade every control-plane member before the brokers.

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
headers; the Rust client has lease tokens; and a broker can serve MQTT (the last
four are on `main`). See
[what Felix is for](/getting-started/what-felix-is-for/) for the status of each.

## Packaging

Each GitHub release now carries prebuilt `libfelix` C libraries, shared and
static, with `felix.h` and a `LINKING.txt`, for Linux, macOS and Windows on x64
and arm64. See [the C client](/clients/c/).
