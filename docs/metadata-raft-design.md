# Metadata Raft

**Decision: control-plane metadata becomes a Raft-replicated state machine
hosted inside the control-plane instances themselves, built on openraft,
served through the existing store traits as a third backend alongside memory
and Postgres. Brokers and clients cannot tell which backend answered. SWIM
for node liveness is rejected for now — that decision is recorded in
[control-plane.md](control-plane.md#why-liveness-stays-centralized-swim-considered),
because it stands on its own whether or not Raft ships.**

Recorded for [#333](https://github.com/gabloe/felix/issues/333). The
alternatives and what would overturn each choice are below, in the same
spirit as [replication-design.md](replication-design.md) — which decided the
*opposite* for stream payloads, and whose argument this document must not
quietly contradict.

## Why now

[ha-postgres.md](ha-postgres.md) names the triggers for reconsidering the
deferred Raft option. The one that fired: Felix should be deployable where no
database platform exists. M7 made the control plane highly available as N
stateless instances over an HA Postgres; that is the right trade wherever a
managed database is available, and it remains supported. Raft is for
everywhere else — and for removing the last external dependency from a
self-contained cluster.

## What has to be true

**The broker contract is frozen.** Brokers consume snapshots + change feeds,
send heartbeats, and receive leases. None of that may change shape: a broker
must not know or care which backend the control plane runs. The whole design
is behind `ControlPlaneStore`/`AuthStore`, exactly where the Postgres/memory
split already lives.

**Acknowledged metadata writes survive losing a minority.** This matches the
contract ha-postgres.md demands of the database platform (synchronous
replication), for the same reason: a rolled-back shard-assignment
`generation` can be reused for a different owner, and brokers de-duplicate
ownership changes by generation. Raft gives this by construction — an
acknowledged write is committed on a majority.

**The M7 signal must keep holding.** Rolling restart of every instance, and
a kill of any one of three, with zero failed broker watch or heartbeat calls.
The existing `rolling_restart.rs` test is the yardstick; it gets a Raft
variant with no Postgres underneath.

**Writes are rare and small; reads are constant.** Tenants, streams,
assignments, membership — kilobytes that change on operator action, plus
heartbeats on a fixed cadence. The read side (broker watches, snapshots,
routing) dwarfs the write side. The design must put its cost on the path
that can afford it.

## The objection this design must answer first

[replication-design.md](replication-design.md) rejected per-shard Raft
partly because **Raft requires truncating a follower's divergent uncommitted
log suffix**, and the storage layer's load-bearing invariant is that records
are never rewritten. Does that argument not kill a metadata Raft log too?

No, and the distinction is worth stating precisely: that invariant belongs to
`felix-storage`'s segment log, and the metadata Raft log **never touches
`felix-storage`**. It is a separate, purpose-built, kilobyte-scale log whose
semantics are Raft's — suffix truncation included — owned by the control
plane, on the control plane's own volume. The "keep two logs" objection in
that document was about writing every *payload* record twice; here there is
no payload, and the second log holds only metadata commands. Reusing the
segment store for the Raft log was considered and rejected for exactly the
reason per-shard Raft was: it would need a truncate-uncommitted-suffix
operation the segment format deliberately does not have.

## The selected design

### Shape

Every control-plane instance embeds a Raft node. The group is the
control-plane replica set — three for production, one (single-node group)
for development. Each instance persists the Raft log and periodic snapshots
on its own volume; this is the PVC-per-pod StatefulSet shape
[control-plane.md](control-plane.md#kubernetes-deployment-model) has
sketched since the beginning.

```
            writes (forwarded to leader, proposed, committed on majority)
   brokers ──────────────────────────────────────────────┐
      │                                                   ▼
      │  reads (served locally)     ┌─────────┐    ┌───────────┐
      ├────────────────────────────▶│ follower│◀──▶│  leader   │
      │                             └─────────┘    └───────────┘
      └────────────────────────────▶┌─────────┐         ▲
                                    │ follower│◀────────┘
                                    └─────────┘   log + snapshots
                                                  on each node's own volume
```

### The state machine is the in-memory store

`InMemoryStore` already implements every store trait and holds the entire
metadata state. The Raft state machine is that store plus an `apply(command)`
entry point: commands mutate it, snapshots serialize it, and reads are served
from it directly on whichever instance received them. This is not a
convenience — it is the design's main simplification. The state machine does
not need inventing; it needs a command log in front of something that
already exists and is already contract-tested against Postgres.

**Commands are API-shaped, not statement-shaped.** One command per logical
mutation: `CreateStream`, `RegisterNode`, `Heartbeat`, `PlaceShard`,
`BootstrapTenantAuth`, and so on — roughly one per mutating store-trait
method, not one per row touched. Two things fall out:

- **Multi-step atomicity is free.** M7 made tenant bootstrap a single
  Postgres transaction so N racing instances produce exactly one winner.
  Under Raft the same guarantee is a single `BootstrapTenantAuth` command:
  the log totally orders the racers, the first applies, the rest observe
  `already initialized`. Every check-then-act race the Postgres backend
  closes with row locks, the log closes with ordering.
- **Apply must be deterministic.** Three instances apply the same commands
  and must reach identical state. So nothing inside `apply` may read a
  clock, generate randomness, or consult anything outside the command and
  the state. Every timestamp is stamped at *propose* time (the heartbeat
  rule "the recorded time is the control plane's own clock" becomes "the
  leader's clock, carried in the command" — same authority, same property:
  a broker still cannot postpone its own expiry). Every generated value —
  signing-key material, kids — is generated at the API layer and carried in
  the command. Signing keys already live in Postgres rows today; carrying
  them in log entries on the same class of volume changes their exposure
  surface by nothing, but it is stated here so nobody discovers it in a
  review.
- **A conditional write is decided at apply.** Placement's assignment writes
  are `PutShardAssignmentIf`, carrying the generation the planner read; the
  state machine compares it with the generation it holds as the entry applies,
  so every replica reaches the same answer and a stale write is answered with
  `StaleAssignment` rather than applied. It is a separate command, not an
  optional field on `PutShardAssignment`: a follower from before it existed
  must refuse the entry, as it refuses any unknown `op`, rather than ignore the
  field, apply the write unconditionally, and diverge from the replicas that
  skipped it. Entries already in the log are unaffected.

### Writes: forwarded, proposed, applied

Any instance accepts a mutating HTTP request; a follower forwards it to the
leader over the control plane's internal channel, the leader proposes,
commits on a majority, applies, and answers. Clients keep talking to one
load-balanced URL. Forwarding is invisible, exactly as it is for
broker-to-broker publishes on the data plane.

### Liveness is leader soft state

Heartbeats do not go through the log. Each one would be an fsync on a
majority for a fact that is stale seconds later. The leader keeps the last
heartbeat per node in memory, aged on its monotonic clock, and answers a
heartbeat only after a read-index round confirms it still leads, so a
partitioned ex-leader cannot extend a broker's lease. A follower forwards
heartbeats to the leader (`/internal/raft/leader`). If the leader is an older
build without that route, the follower falls back to the old log command.

The log carries only the consequences. `ExpireNodes` names the nodes the
leader judged stale, each at an incarnation. `CheckpointHeartbeats`, written
at most every 5 s for the whole fleet, keeps node listings on followers
roughly current. The leader judges every node by a monotonic age alone, never
by the log's stamp: that stamp is a checkpoint of an earlier moment and, after
a wall-clock step back or on a leader whose clock is behind the last one's, it
lies in the future. A node heard from this term is aged from that heartbeat. A
new leader starts knowing nothing and treats its own start as a heartbeat from
every node, and a registration during the term as a heartbeat from that node.
So it expires nobody until a full window has passed under it, and a broker it
never hears from goes down one window after the election, however far ahead
its stamp is. Its view resets on every term change. Listings served by the
leader show a stamp ahead of its clock as "now". The placement
lease works the same way: renewals are soft state, only a change of holder is
written, and a new leader counts the recorded holder as renewed when it took
over.

### Reads: local, because the contract already allows it

Broker snapshot/changes polling is *pull-based and eventually consistent by
contract* — a broker behind by one poll is the normal case the resnapshot
rules already handle. So follower-served reads change nothing for brokers,
and they are what keeps read load off the leader.

The change feeds keep their per-entity sequence numbers and eviction
windows, maintained by the state machine exactly as `InMemoryStore`
maintains them today, so all three resnapshot signals
(`first seq > since`, empty-but-advanced, `next_seq < since`) survive
unchanged. Sequence numbers are state-machine state, identical on every
instance at the same applied index — a broker can fail over between
instances mid-poll and the numbers still mean the same thing, which is
better than today, where an in-memory control plane restarting resets them.

Admin reads that feed decisions (the sweep's view before claiming an expiry,
placement's reads) run on the leader, which serves them from applied
state — leader-local reads after `ReadIndex`-style confirmation where
staleness would change a decision. The expiry sweep and the placement
reconciler run **only on the leader**, which replaces M7's
"each node claimed by exactly one sweep" cross-instance coordination with
something strictly simpler: there is one sweep because there is one leader,
and its conclusions are commands like everything else.

### Leases come from the leader

[replication-design.md](replication-design.md) leases are granted by "the
control plane"; under Raft that means **the Raft leader**, and the
`t + L + margin` safety arithmetic gets one addition: a new Raft leader must
not grant generation `G+1` earlier than the old leader could have promised
`G` was still valid. Lease grants and their timestamps are in the log, so
the new leader knows every outstanding promise and simply waits out the
same margin. The fencing story does not change; it gains a second
well-defined epoch (Raft term) underneath the one it already has
(assignment generation).

Replica reports go through the log like everything else placement decides
on — `RecordReplicaReport`, restamped with the leader's clock on the way in,
exactly as a heartbeat is — so every member holds them and a new Raft leader
promotes from what the old one knew rather than waiting for leaders to report
to *it*. They are in snapshots too (`replica_reports`, left out while
empty, so an older snapshot without the field still loads). A live leader
would replace them within one reporting interval, but a dead one never
reports again, and its last report is exactly what failover needs to promote
a replica. Without it, a member restored from a snapshot taken after that
report could not replace the leader, and its state would differ from its
peers' in a way the byte-for-byte snapshot comparison could not see.

### Snapshots, compaction, recovery

State is small, so snapshots are cheap: serialize the whole store at a log
threshold, truncate the log behind it. An instance that lost its volume
rejoins empty and is caught up by snapshot install plus log replay, without
a vote until it has (see [Rejoining after a lost volume](#rejoining-after-a-lost-volume))
— the Raft-native answer to the question backup/restore answers for Postgres. For
disaster recovery beyond quorum loss, the same snapshot format doubles as an
export: the import path below reads either a Postgres database or a snapshot
file.

### Group membership and bootstrap

Initial members come from configuration — for the StatefulSet shape, the
ordinal peers (`felix-controlplane-{0,1,2}`) via the headless service that
control-plane.md already reserves for exactly this. Growing the group is
learner-first: a new instance joins as a non-voting learner, catches up by
snapshot, and is promoted; shrinking is the reverse. A single-member group
serves development with no ceremony, replacing the in-memory backend's role
without its amnesia.

### Rejoining after a lost volume

Raft is only safe if a voter never forgets its vote or its log, and openraft
will grant a vote from an empty log. So a wiped member that simply came back
as a voter could lose acknowledged writes: with the leader wiped and the only
other holder of a write frozen, the member that missed the write wins on the
wiped member's vote, and truncates the write from the frozen member when it
returns. `tests/raft_chaos.rs` reproduces exactly that.

A member that starts with no Raft state therefore withholds its vote: it
refuses vote requests and does not stand for election. It asks its peers
(`GET /internal/raft/standing`) which case it is in:

- **The group exists** — some peer's log is past the initial membership
  entry. The member follows the leader like a learner: it takes the log or a
  snapshot but has no vote. It asks the leader for its last log index
  (`GET /internal/raft/catch-up-target`, answered only after a read-index
  round confirms the leadership) and votes again once it has applied that
  far. Readiness keeps it out of rotation until then.
- **First boot** — a majority, this member included, answers and is empty,
  *and* the operator set `FELIX_RAFT_INITIAL_CLUSTER_STATE=new`. Each such
  member initializes the configured group, which openraft allows. A
  single-member group decides this alone. With the default, `existing`, an
  empty majority waits and logs why instead: a member cannot tell a first
  boot from a lost volume, and only the operator knows which it is.
- **Neither** — it waits and asks again, so a lone wiped member never forms a
  group of its own.

The withheld vote is written to the member's store before any of this, so a
crash half-way through catching up restarts still withholding it, not as a
voter with half a log. Membership never changes: a demote-and-promote would
need every remaining voter for the joint configuration, so one other member
being down would stall writes until it came back.

Losing a majority's volumes at once is quorum loss, and the snapshot
restore path is the answer. What the member must not do is paper over it:
two empty members of three that can reach each other but not the third
used to form a new, empty group on their own, with new signing keys and
every generation back to zero. Now they form one only under `new`, which
belongs to a cluster's first start and nowhere else.

**Cluster id.** Every member is configured with `FELIX_RAFT_CLUSTER_ID`,
etcd's cluster token under another name. The first start records it in the
member's store, and a member refuses to start on a store recorded under
another id. Every peer request carries it and is refused (403) when it
names another cluster, so a member whose peers map points at some other
group's members never counts them as empty or as holding the group.

### Migration from Postgres

Dual backends, then an offline cutover. The tool is `felix-controlplane
migrate`, riding the same binary so every image that runs the control plane
carries it; the interchange format is the state machine's own exported
snapshot, produced **through the store traits** so it contains exactly what
the API serves. The ceremony:

1. **Stand up the Raft group**, fresh and empty, with the same peers map on
   every member. Its import guard refuses a group that already holds state,
   so pointing the tool at the wrong cluster is an error message, not a
   catastrophe. *Abort here: tear the group down; nothing has changed.*
2. **Freeze metadata writes** — flip the Postgres-backed instances out of
   rotation (their readiness during a drain already does this). Brokers
   keep serving on their catalogs and leases, exactly as during any
   control-plane blip. *Abort here: put the old instances back in
   rotation; nothing has changed.*
3. **Export**: `felix-controlplane migrate export-postgres state.json`
   (with `FELIX_CONTROLPLANE_POSTGRES_URL` pointing at the frozen
   database). The tool prints a summary — counts per entity — for the
   before/after comparison.
4. **Import**: `felix-controlplane migrate import state.json
   http://<any-member-peer-address>` — one `ImportState` command proposed
   through the group: atomic on every member, forwarded to the leader from
   whichever address you gave. The tool talks to the peer listener as a
   peer, so run it with the group's `FELIX_RAFT_CLUSTER_ID` and
   `FELIX_RAFT_PEER_TOKEN` (and `FELIX_RAFT_TLS_*` under peer mTLS; the URL
   is then `https://`). *Abort here: the group either applied all of it or
   none; tear it down and put Postgres back.*
5. **Verify and repoint**: compare the import summary against the export's,
   spot-check snapshots and feed heads, then point brokers and operators at
   the new instances and retire the database.

Sequence continuity is the part brokers feel: the export carries every
change feed's **high-water mark with an empty retained window**, so a
broker whose checkpoint is at the head continues without noticing, and one
behind the head gets the ordinary "checkpoint predates the window" signal
and resnapshots exactly once. Dragging Postgres's change rows along to
avoid even that single resnapshot was considered and skipped: the signal
path is a contract brokers already honour, and exercising it beats
carrying migration-only code to avoid it.

The freeze window is minutes, and brokers tolerate it by design. Online
dual-write migration was considered and rejected: two sources of truth
during the window is precisely the class of bug this whole design exists to
remove, and the workload (rare writes, pull-based readers) does not need
zero-write-downtime cutover.

**Disaster recovery beyond quorum loss** uses the same two commands, and
this is deliberate: the export file *is* the DR artifact, and
`migrate import ... --overwrite` onto a fresh group is the restore. The
`--overwrite` flag is the loud warning made mechanical — it discards
whatever the target group holds, and consumers' checkpoints with it, so it
belongs in a runbook and nowhere else. Take exports on a schedule the way
database backups are taken, with two caveats:

- **An export is not a consistent read.** It is many separate queries with no
  transaction around them, so an export taken while metadata is being written
  can pair records from different moments (an assignment for a stream the same
  file no longer lists, a change-feed head behind a record). Only an export of a
  frozen database is a state the cluster has actually been in.
- **The file is a credential.** It holds every tenant's Ed25519 signing-key
  seeds, current and previous, as plaintext JSON: whoever reads it can mint a
  valid token for any tenant. Store and transfer it the way you would the
  signing keys themselves. Refresh tokens are not in it (the store keeps only
  their hashes, and the export leaves them out), so a restore invalidates every
  refresh token issued before it.

Two things about the export file. It is **not a consistent read** unless
metadata writes are frozen: it is many queries through the store traits, so
an export of a live store can mix states. And it holds **every tenant's
signing-key seed** in plain JSON (refresh tokens only as hashes), so whoever
reads it can mint tokens for any tenant. The tool writes it mode `0600`;
store it encrypted, as you would the database it came from.

### Probes

- `/v1/system/live` — unchanged: process-local, never touches consensus.
  An instance that lost quorum must not be restarted into the same lost
  quorum.
- `/v1/system/ready` — ready when this instance knows a leader and its
  applied index is within a freshness bound of the leader's commit. A
  follower serving watches is ready; an instance partitioned from the group
  is not; during an election the group is briefly all-unready for writes,
  which is the truthful answer and lasts an election timeout, not a cache
  window. The M7 drain semantics (fail readiness first, predrain hold,
  bounded drain) apply as-is.

## Library

**openraft, pinned to the stable 0.9 line** (0.9.25 at time of writing;
the 0.10 line is still alpha), wrapped behind a small crate-local seam so
openraft types never appear in the store traits or handlers — the same
discipline as the storage layer's `AppendOnlyLog` seam, and the insurance
against openraft's documented pre-1.0 API instability.

- **openraft** — async, tokio-native, snapshot/learner/membership machinery
  included, proven as the metadata consensus of Databend among others.
- **raft-rs (TiKV)** — rejected: a sync core that requires hand-building
  the tick loop, transport, storage, and snapshot orchestration openraft
  ships; that is most of the risk for none of the fit.
- **Hand-rolled** — rejected. ha-postgres.md put it as "every line of
  consensus code Felix does not carry is one it cannot get wrong"; that was
  an argument for deferring, and now that the work is scheduled it is an
  argument for a maintained implementation with an existing test corpus.
  Felix's inventiveness budget here goes to the state machine and the
  migration, which nobody else can write.

### Transport and peer authentication

Raft RPCs are JSON over HTTP on a **peer listener of their own**
(`FELIX_RAFT_BIND_ADDR`), never the public API port. On the API port,
`propose` would be open to anyone who can reach the API, and one
`ImportState{overwrite}` replaces the whole store, signing keys included.

Every peer request must carry:

- `x-felix-raft-cluster-id` naming this group (403 otherwise), and
- `Authorization: Bearer <FELIX_RAFT_PEER_TOKEN>`, compared in constant
  time (401 otherwise).

The token is the cluster-admin credential. Members hold it, and so does the
operator running `migrate import`; brokers and API clients never see it.
`propose` needs nothing more because holding the token already is
cluster-admin. A member refuses to start in Raft mode without a token of at
least 32 characters unless `FELIX_RAFT_INSECURE_PEERS=true` says otherwise,
which is for throwaway local groups.

With `FELIX_RAFT_TLS_CERT`, `FELIX_RAFT_TLS_KEY` and `FELIX_RAFT_TLS_CA`
set, the peer listener terminates mTLS and refuses the handshake to any
client without a certificate from that CA, and the member's own peer client
presents the same certificate, trusts only that CA, and checks the server
name against the address in `FELIX_RAFT_PEERS`. The token stays required
under mTLS. What mTLS does not do yet is bind a certificate to a member id:
any certificate from the cluster CA can speak for any member, so issue that
CA's certificates to control-plane members only.

The network layer resolves a member's address from `FELIX_RAFT_PEERS` by
id before falling back to the address recorded in the membership, so moving
the peer port is a config change, not a membership change.

### Upgrading

Two rules, both about mixed versions.

**Moving to the authenticated peer listener** is not a rolling change: an
old member sends unauthenticated RPCs to the API port, and a new one only
answers authenticated ones on the peer port. Upgrade every member at once
(scale the StatefulSet's pods down and up, or delete them together). Writes
pause for the restart; brokers keep serving on their catalogs and leases as
during any control-plane blip. The recorded membership still names the old
API addresses, which is fine: the configured peers map wins.

**Commands newer than some member are not proposed.** Every command
variant has a level (`MetaCommand::version`), and each build reports the
highest level it can apply (`METADATA_VERSION`) on the peer `standing`
route. A member proposes a command only when every member of the current
membership, learners included, reports at least that command's level, in
the manner of KRaft's `metadata.version`. A member that reports nothing
predates this and counts as level 0; one that cannot be reached counts at
what it last reported, or 0 if it never has. The member asking gives its
own level on that probe, so every member has heard the leader's: when the
leader dies, the member elected next still counts it at its level. Without
that, a dead leader was an unknown to its successor, and the new leader fell
back to heartbeats and expiry through the log for as long as it stayed down.
The group's level is
recomputed at most every two seconds, so it rises shortly after the last
member restarts on the new build. The rollout order therefore does not
matter: a StatefulSet can upgrade the leader first.

Level 1 is rule removal and signing-key rotation (`remove_rbac_policy`,
`remove_rbac_grouping`, `stage_signing_key`, `activate_signing_key`,
`retire_signing_key`) and leader soft-state liveness (`expire_nodes`,
`checkpoint_heartbeats`). Until every member is at level 1:

- the routes that remove RBAC rules or stage, activate or retire signing
  keys answer 409 saying the group is not yet at the version they need;
- the leader declines leader-only requests, so heartbeats, expiry and the
  placement lease go through the log (`record_node_heartbeat`,
  `expire_stale_nodes`, `take_placement_lease`) as on the older build.
  When the group reaches level 1 the leader starts a fresh soft-state
  window, so nothing is expired at the switch.

Level 2 is fleet features (see [Fleet features](control-plane.md#fleet-features)):
`register_node_in_fleet`, a registration that keeps the broker's features
and refuses one lacking an enabled feature, and `finalize_fleet_feature`,
which enables one if every serving broker supports it. An older member would
apply the first as a plain registration and accept a broker this build
refused, so until every member is at level 2 brokers register with
`register_node` and their features dropped, and a finalize is refused. A
broker reports its features again when it next registers. The enabled set is
part of the snapshot (`fleet_enabled`, omitted while empty).

**Fields have levels too.** An older member decodes a newer entry with
serde, which drops any field it does not know, so a field is as much a
change to the command set as a variant. Every key path a command or the
snapshot can serialize is listed with the level that introduced it in
`store/raft/command/fields.txt`, and a command's level is the higher of its
variant's and that of the newest field it actually carries. Optional fields
are left out at their default, so a jump-hash stream needs level 3 and a
modulo stream needs nothing. Below a field's level the proposer either drops
the field itself, on every member alike, or refuses the command:

| Field | Level | Below it |
|---|---|---|
| refresh token `narrowing` | 1 | the refresh token is not stored and the exchange fails, since without it a refresh could widen the exchange's rights |
| replica report `leader` | 1 | the report is proposed without it and applies unchecked everywhere, as on the older build |
| snapshot `refresh_tokens` | 1 | snapshot only |
| node `zone` | 2 | the node registers without it, like its fleet features |
| node `features`, snapshot `fleet_enabled` | 2 | as above |
| stream `routing` | 3 | a jump-hash stream is refused with 409, and so is finalizing `jump_hash_routing` |
| snapshot `replica_reports` | 3 | snapshot only |

**A member refuses what it would drop.** From level 3 on, a member that
decodes an entry carrying a non-empty field it does not know answers
`Unsupported` and counts it, exactly as for an unknown variant, instead of
applying what is left. A snapshot carrying one stops the member at restore.
This is the rollback case: a member rolled back below a level already in
use would otherwise read the newer data and quietly lose the field, and for
`routing` that turns a jump-hash stream back into modulo and moves keys
between shards. Builds before level 3 do not have this check, so rolling
back past level 3 once a jump-hash stream exists silently loses its routing.
Do not roll a member back below the group's level; roll it forward.

A release that adds a variant or a field gives it the next level and raises
`METADATA_VERSION`. `store/raft/command/fields/tests.rs` builds every
replicated type from full struct literals and compares the paths they
serialize with `fields.txt`, so a new field does not compile until it has a
sample value, and the test fails until it has a level. If `felix_meta_raft_unsupported_commands_total` still
moves on a member (something proposed through the raw peer `propose`
route, or a member added on an older build after the level was computed),
upgrade it, then wipe its volume and let it rejoin: it rebuilds from the
leader's snapshot as in
[rejoining after a lost volume](#rejoining-after-a-lost-volume).

## Failure modes

| Failure | Behaviour |
| --- | --- |
| One instance of three dies | Leader (if it was the leader) re-elected in one election timeout; writes pause for that long, reads keep serving; M7 signal holds |
| Instance loses its volume | Rejoins without a vote, catches up by log or snapshot, then votes again ([above](#rejoining-after-a-lost-volume)); no operator data surgery |
| A majority loses its volumes | The empty members wait rather than form a new, empty group, unless `FELIX_RAFT_INITIAL_CLUSTER_STATE=new`; recovery is the snapshot restore |
| Something other than a member reaches the peer port | Refused before any route runs: wrong cluster id 403, missing or wrong peer token 401, and under peer mTLS no TLS handshake without a certificate from the cluster CA |
| Network partition, leader in minority | Old leader steps down (cannot commit), majority elects; minority instances fail readiness rather than serve writes that cannot commit |
| Quorum lost (2 of 3 down) | Writes and readiness fail on survivors; brokers keep serving on catalogs and leases as during any control-plane outage; recovery = restore instances, or restore-from-snapshot ceremony documented with appropriately loud warnings |
| Clock skew between instances | Irrelevant to Raft safety (term-based). Liveness expiry does not depend on it either: heartbeats are stamped and judged by one clock — the store's, which under Postgres is `clock_timestamp()` — so two instances comparing their own `SystemTime` is not a thing that can happen |
| Disk full on one instance | That instance fails writes → falls out of quorum participation → fails readiness; group continues on the majority |

## Testing

- **Determinism harness**: apply the same command sequence to two state
  machines, assert byte-identical snapshots — the cheap test that catches
  the expensive bug (a clock or a HashMap iteration order leaking into
  apply).
- **The contract suites run against the Raft backend** as they run against
  memory and Postgres (`contract::nodes`, `contract::shards`,
  `contract::signing_keys`, `contract::placement` including the expiring
  lease, `contract::refresh_tokens`) — that is what the trait seam is for.
  There are no contracts for tenants, streams, or RBAC on any backend yet.
- **`rolling_restart.rs`, Raft variant**: three instances, no Postgres,
  same zero-failed-calls assertion, plus a hard kill of the leader
  specifically.
- **Chaos via the cluster harness**: partition the leader from the group
  under broker traffic; assert no metadata write is lost and no shard gets
  two leaders across the transition (the lease safety interval test, now
  with a moving grantor).
- **Snapshot/restore**: wipe one instance's volume mid-traffic; assert it
  rejoins and converges.

## What would overturn this

- **Heartbeat write volume dominating the log** at broker counts Felix
  actually reaches — the fix is decentralizing liveness (the SWIM decision
  gets reopened), not abandoning Raft for the metadata that is actually
  rare.
- **A multi-region metadata requirement.** One Raft group in one region is
  this design; metadata with region-local write latency everywhere is a
  different problem (and was already out of scope for Postgres HA too).
- **openraft 0.10 stabilizing with a materially better storage API** — the
  seam exists so that upgrade is a contained event, not a redesign.

## Sequencing

Tracked as milestone M13; the issue breakdown mirrors this document's
sections — Raft core and storage, state machine and command set, the store
backend and forwarding, migration tooling, probes and packaging, and the
chaos/conformance pass. [#333](https://github.com/gabloe/felix/issues/333)
is the umbrella.

### Implementation status

| Piece | Issue | State |
| --- | --- | --- |
| Raft core: seam, redb log/vote/snapshot store, HTTP transport, group lifecycle | [#337](https://github.com/gabloe/felix/issues/337) | **Landed** — `services/felix-controlplane-service/src/raft/`. The store passes openraft's own storage conformance suite; group tests cover election, replication, restart-as-rejoin, wiped-volume rebuild by snapshot, and learner-first growth. Nothing serves metadata from it yet. |
| Metadata state machine | [#338](https://github.com/gabloe/felix/issues/338) | **Landed** — `store/raft/command.rs` (the versioned, API-shaped command set) and `store/raft/state_machine.rs` (`MetadataStateMachine`, the in-memory store behind the seam). The determinism harness applies a full-coverage script to two machines and requires byte-identical snapshots; a real three-node group settles eight concurrent bootstraps by log order alone with byte-identical replicas. Landing it surfaced and fixed real iteration-order leaks: multi-node expiry and cascading deletes published change events in HashMap order. Nothing serves API traffic from it yet. |
| Store backend, forwarding, read semantics | [#339](https://github.com/gabloe/felix/issues/339) | **Landed** — `store/raft.rs` (`RaftStore`), the third backend behind the store traits: reads from local applied state, writes proposed through the seam with follower→leader forwarding inside it, sweep and placement gated to the leader by a linearizable read-index check, and `StorageBackend::Raft` selectable via `FELIX_RAFT_NODE_ID` / `FELIX_RAFT_DATA_DIR` / `FELIX_RAFT_PEERS`. Passes the same node/shard contract suites as memory and Postgres; a binary-level test serves the HTTP API with no database and keeps its metadata across a restart. Finding recorded below. Probes are minimal (leader-known) until #341. |
| Migration from Postgres | [#340](https://github.com/gabloe/felix/issues/340) | **Landed** — `felix-controlplane migrate export-postgres/import`, the generic trait-level export (works against any backend, doubling as the DR artifact), the `ImportState` command with its used-store guard and `--overwrite` restore path, and the ceremony above. The pg-tests E2E migrates a populated Postgres into a Raft group over the real propose route and verifies records, sequence heads, generations, and auth state; a broker at the head continues without a resnapshot. |
| Probes, packaging, configuration | [#341](https://github.com/gabloe/felix/issues/341) | **Landed** — readiness answers from consensus state (leader known, apply-lag bounded, and a leader counts only while a quorum has acknowledged it within 5s — a quorumless leader leaves rotation, proven by test); liveness stays process-local. Timings are tunable (`FELIX_RAFT_HEARTBEAT_MS`, `FELIX_RAFT_ELECTION_TIMEOUT_MIN/MAX_MS`, snapshot/write knobs) with unworkable combinations refused at startup. Consensus position ships as `felix_meta_raft_*` gauges plus forwarded-proposal and write-timeout counters. Kubernetes shape documented on the docs-site page. Known fact below. |
| Chaos and conformance | [#342](https://github.com/gabloe/felix/issues/342) | **Landed** — `tests/raft_chaos.rs`: three real binaries, no database, broker-shaped traffic and metadata writes flowing while every member is SIGTERM-restarted, the leader is SIGKILLed, the leader is frozen (SIGSTOP) past several elections and thawed, and a follower's volume is wiped. Verdict per run: zero failed calls, election gaps bounded, and **every acknowledged write present on every member** once each has applied a later marker write — the milestone's completion signal, met. It caught four real bugs before landing (below). A second test wipes the *leader* while the only other holder of a write is frozen, the case in which a wiped member voting from an empty log loses that write ([above](#rejoining-after-a-lost-volume)). |

One deliberate deviation from the sketch above, made while landing #337: the
Raft log lives in **redb** (an embedded, crash-safe, single-file ACID store)
rather than hand-rolled files. Consensus durability plumbing — votes and
entries that must never be acknowledged and then lost — is the last place
Felix should be inventive, and openraft's storage suite now enforces the
semantics against the real store on every test run.

Four findings from landing #342 — each one a bug the chaos suite caught
that no earlier test could see:

- **`loosen-follower-log-revert` is not optional.** A member rejoining with
  a wiped volume reports a log that went backwards; without that openraft
  feature the *leader* trips a debug assertion in its replication-progress
  tracking when the member returns — and a release build would carry the
  inconsistent progress state silently. The feature is now on, with the
  reasoning at the dependency declaration.
- **A restart is not done until the state machine is.** A restarted member
  learned the leader within a heartbeat and reported ready while its
  volatile state machine was still replaying the log — serving a world
  missing entries it had itself committed. Startup now blocks until the
  replay reaches the committed index persisted on its own disk.
- **A wiped member's apply-lag reads zero.** Lag is measured against the
  member's *own* log, which is exactly the blind spot for a member that has
  none of the group's state yet. Readiness now refuses a follower that
  knows a leader but holds an empty log — it has joined an established
  group and nothing has replicated into it yet. (A leader is exempt; a
  genuinely new cluster is leaderless, so formation is never blocked.)
- **One hung hop must not eat the whole write budget.** A leader that is
  frozen — not dead — accepts the forwarded connection and stalls, and a
  single forward could consume the entire proposal budget, leaving nothing
  for the retry after the group elected a successor. Every attempt is now
  individually capped well below the budget.

One known fact recorded while landing #341, inherent to **pre-0.10
openraft**: there is no leadership-transfer API, so a rolling deploy that
restarts the current *leader* pauses metadata writes for one election
timeout (~1.2s at defaults) while a successor elects itself. Reads keep
serving throughout, followers restart with no pause at all, and brokers are
unaffected by construction. openraft 0.10 adds `transfer_leader`; adopting
it is a contained change because the seam owns the shutdown path — until
then this is a documented bound, not a bug.

One finding from landing #339: **openraft's write path waits indefinitely**
— a leader that has lost quorum queues proposals forever rather than
failing them. The seam now owns an overall write deadline (default 10s,
elections and forwarding included), so "no quorum" reaches callers as an
error rather than a hang; the quorum-loss test is what surfaced it. The API
answers that error `503 unavailable`, not `500`: the group is electing or has
lost quorum, which a retry can outlast. The outcome is unknown rather than
failed, since the entry may still commit, so a retried create can meet its
own earlier write as `409`.

Two findings from landing #338, recorded because they are the design's
predictions coming true:

- **The iteration-order leak was real.** Multi-node expiry and the
  tenant/namespace cascade deletes published their change events in HashMap
  iteration order — harmless on one instance, state-forking on replicas,
  because each event takes a sequence number as it publishes. They now
  publish in sorted order, and the determinism harness is what holds that
  door shut.
- **Key generation moved to propose time for every backend.**
  `TenantAuthSeed` now carries the candidate signing keys; the API layer
  generates them, and both the Postgres transaction and the state machine
  install them only when the tenant has none. The store layer is now free of
  randomness end to end, not just under Raft.
