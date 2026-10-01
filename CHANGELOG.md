# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions. See [delivery semantics](docs-site/src/content/docs/architecture/semantics.md)
for what the current release guarantees.

## [Unreleased]

### Security

- **A refresh cannot widen its exchange.** The exchange's `requested`,
  `resources` and `audience` are stored with the refresh chain and re-applied
  on every refresh; a refresh naming another audience is a `400`. Postgres
  gains a nullable `refresh_tokens.narrowing` column; older tokens refresh as
  before. On Raft, roll every member before relying on it.
- **Re-pointing an IdP issuer takes cluster rights.** Changing an existing
  issuer's `jwks_url`/`discovery_url`, or deleting an issuer, needs
  `tenant.manage:cluster:*`. IdP
  fetches no longer follow redirects and refuse private, link-local and
  unique-local addresses unless `FELIX_CONTROLPLANE_OIDC_ALLOW_PRIVATE_IDP`
  is set.
- **Control-plane authorization can shrink.** `DELETE .../rbac/policies` and
  `.../rbac/groupings` remove rules (same scope as adding them);
  `POST /v1/tenants/{t}/refresh-tokens/revoke` ends a principal's refresh
  chains; `/v1/tenants/{t}/signing-keys` stages, activates and retires tenant
  signing keys. On Raft these are new commands: roll every member first.
- **IdP groups are scoped by issuer** (`group:{issuer}#{name}`), so an IdP a
  tenant admin registers cannot claim another IdP's groups. Bare
  `group:{name}` groupings need migrating; see `docs/auth.md`
  (`FELIX_CONTROLPLANE_LEGACY_UNSCOPED_GROUPS` bridges the migration).
- **The control-plane API refuses broker-audience tokens.** Exchange and
  refresh take `"audience": "felix-controlplane"` for API callers, including
  broker node credentials; `FELIX_CONTROLPLANE_ACCEPT_BROKER_AUDIENCE` bridges
  the migration.
- **IdP discovery and JWKS URLs must be HTTPS** (plain HTTP on loopback, or
  with `FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP`), and a discovery
  document must name the configured issuer.
- Token exchange `resources` now narrow grants instead of passing a broader
  grant through unchanged.
- Tenant, namespace, stream and cache names are validated at creation; `*`,
  `/`, `:` and similar are refused.
- Postgres `delete_node` and assignment writes lock the node row, so a racing
  delete can no longer leave an assignment to a deleted node; a node that is
  still a replica can no longer be deleted.
- Live tokens no longer appear in `Debug` output, and migration exports are
  written mode `0600`.

### Added

- **Publish acknowledgements say where the batch landed.** A client that
  offers the new frame flag `FLAG_BINARY_PUBLISH_ACK_OFFSET` (`0x1000`) gets
  the offset of the batch's first record on a successful binary ack, and as
  `offset` on a JSON `publish_ok`. The broker sends it only when it answers
  after the write; a client that did not offer the bit gets byte-identical
  frames. A duplicate idempotent batch reports the original's offset, and a
  forwarded batch reports the owner's, through a new peer capability
  `FORWARD_OFFSETS`. `Publisher::publish`, `publish_batch` and friends,
  `ClusterClient::publish` and `IdempotentProducer::publish` now return
  `Result<Option<u64>>` instead of `Result<()>`; the Python and Node clients
  return the offset too. The history checker now checks every acknowledged
  append against the offset it was acknowledged at (#876).

- **Placement sees halted replicas (#874).** A leader's replica report names
  the followers it has stopped shipping to, with the reason (`diverged` or
  `needs_bootstrap`), as an optional `halted` field that is omitted when empty.
  The control plane stores them with the report, keeping when each halt began:
  Postgres gains `replica_reports.halted` (migration 0023), and on Raft the
  field is metadata level 4, dropped until every member is upgraded. Placement
  never picks a halted copy as a move, replacement or restore destination,
  gives up a move whose destination halts (step `halted`) and places it
  elsewhere, counts a halted copy as missing, and replaces one halted for
  `FELIX_SHARD_RESTORE_AFTER_MS`. An operator's move to one is refused with
  `destination_halted`, and a drain with nowhere else to go says which copy is
  halted. `GET /v1/placement/replication` and `felix-controlplane admin
  replication` list halted copies, and `felix_shard_replicas_halted` counts
  them.

- **Placement restores a shard's replication factor.** A follower whose broker
  has been down or gone for `FELIX_SHARD_RESTORE_AFTER_MS` (`shard_restore_after_ms`,
  five minutes by default, `0` to turn it off) is replaced by a copy on a live
  broker. A replica set a failover left short of its factor is topped up once a
  broker is free, which covers a cache or `Leader` stream whose failover dropped
  the dead leader. The copy joins and is seated the way a drain's replacement
  is, so a `Quorum` stream keeps the old set's majority rule. The restore is
  stored in the assignment (move reason `restore`) and resumes across
  control-plane restarts, and a destination that dies mid-copy is replaced.
  A short set grows before a lost follower in it is replaced, nothing starts
  until the leader has reported at its generation, and a failover keeps a copy
  that was joining a set of two, since the leader counted it.
  New: `felix_shards_under_replicated`, `felix_shard_replicas_missing`,
  `GET /v1/placement/replication` and `felix-controlplane admin replication`.
  The move step `Seat` now names the follower it replaces as optional, and
  `MoveStep::Restore` and `MoveReason::Restore` are new. A control plane that
  predates `restore` cannot read an assignment with that move reason, so
  upgrade every control-plane instance before relying on it.

- **Adversarial history campaign and liveness checks.** The history
  checker's nemesis gains compound faults on four brokers: two brokers killed
  at once, a leader isolated from its peers and the control plane until it
  fails over, a partition beside a delayed link, two random faults together,
  a move whose source or destination is killed, restart loops, torn segment
  writes, and drains that replace follower copies. After every heal the
  campaign now waits for each shard to serve again and fails naming any shard
  that is stuck and why. `FELIX_HISTORY_NEMESIS` picks the nemesis, and the
  nightly workflow runs both.
- **Power loss, control-plane crashes and delivery checks in the history
  campaign.** The adversarial nemesis can cut power to every broker at once,
  keeping only what each one flushed, and crash the control plane while a
  move, drain or failover is in flight. Live subscribers now follow each list
  for the whole run, and three new rules check that delivery follows offset
  order, that nothing delivered is later lost, and that every record is
  either delivered or skipped visibly. Debug and `fault-injection` brokers
  take the power loss from `FELIX_STORAGE_POWER_LOSS_ROOT` and the storage
  fault file, on Linux. The harness keeps broker storage in `storage/` under
  each node's data directory.
- **Atomic commits on one shard.** `Client::commit(tenant, namespace,
  entity_key, [publish | enqueue, put, delete])` writes an event and state
  updates as one record on the stream shard `entity_key` routes to, and
  `state_get` reads that state with the commit's offset as its version. No
  reader sees part of a commit, across failover and promotion. Operations on
  another stream are refused with `CommitError::NotOnOwningShard`, never
  split. New wire requests `commit`/`state_get` behind
  `FEATURE_ATOMIC_COMMIT`; storage format v5 (a segment is written at v5
  only to hold a commit record); replication mark byte `4`; fleet feature
  `atomic_commit`, which a cluster must finalize before commits are accepted.
  See `docs/atomic-commit.md`.
- **Jump-hash stream routing once the `jump_hash_routing` fleet feature is
  finalized.** A stream's key-to-shard mapping (`routing`: `modulo` or
  `jump_hash`) is fixed at creation; streams created after the finalize get
  jump consistent hashing, and existing streams keep modulo, so no key moves.
  Postgres gains a nullable `streams.routing` column (migration 0022). Shard
  assignments and `stream_shards_view` carry `routing` only for jump-hash
  streams, the broker resolves keyed publishes by it, and `ClusterClient`
  routes by it. `Client::stream_routing` is new; `felix_router::Placed` gains
  a `routing` field.
- **Readers and consumer groups on `Quorum` shards without the lease** once
  `lease_free_reads` is finalized (#885). Subscriptions, replay, Kafka fetches
  and cache watches on a replicated `Quorum` shard keep going when the lease
  lapses, since they see only the committed mark. They end when a follower
  refuses the leader for a newer one, or when no majority has confirmed it for
  a lease duration (a round runs every quarter lease while the lease is
  lapsed). Group polls, acks, nacks and dead-letter changes skip the lease and
  are confirmed by a read-index round after they are durable and before they
  are acknowledged; a refused round answers `leadership_lost`. Kafka produce
  now takes its lease check from the shard fence, like a native publish, so a
  `majority_ack` shard takes Kafka writes without the lease too.
  `BrokerCluster::with_writes` loses its lease argument. TLA+:
  `FelixShardSessions.tla`, with `Subscriber` and `GroupRound` passing
  `NoLostDelivery` and `NoStaleGroupCommit`, and `PastMark`, `GroupNoRound`
  and `GroupLease` violating them. See `docs/replication-design.md`, "Readers
  and group sessions without the lease".
- **`Quorum` cache reads without the lease once the `lease_free_reads` fleet
  feature is finalized** (with `majority_ack` and `generation_start`). A get
  or counter get on a replicated `Quorum` cache, local or forwarded, takes its
  value and then confirms leadership read-index style: the promotion fence at
  the broker's own generation goes to every replica, and the read is answered
  once a majority, the broker included, has taken it. Concurrent reads share
  rounds, only ever one that started after them. A cache replica also refuses
  the round when its counter log accepted a newer leader. A same-generation
  fence is now counted as `fence_confirmed` and not logged. New
  `FELIX_QUORUM_READS` (`majority` or `lease`) keeps a broker on the lease. No
  wire change. TLA+:
  `FelixShardReads.tla`, with `FelixShardReadsRound` passing `NoStaleRead`
  under drift and no margin and `FelixShardReadsNoRound` /
  `FelixShardReadsLease` violating it; `check_tla.sh` takes `TLC_WORKERS`.
  See `docs/replication-design.md`, "Reads without the lease".
- **`Quorum` streams acknowledge by their followers once the `majority_ack`
  fleet feature is finalized.** A write is acknowledged when a majority of the
  replica set, the leader included, has answered `ReplicateOk` for it at the
  leader's generation; the control-plane report goes out behind the mark for
  placement only, and the lease is off the write's path (admission, commit and
  acknowledgement). A leader counts itself only while it has accepted no newer
  generation, and a promoted leader never opens a stream shard on the lease.
  Needs `generation_start` finalized too. A broker running with
  `FELIX_INTERNAL_FENCE=false` does not report the feature. `Leader` streams,
  caches, group state and reads keep the lease. TLA+: `HeldAtGen` counts
  follower answers (`confirmed`), and `FelixShardFigure8FollowerAcks` /
  `FelixShardFigure8FollowerAcksNoStartRecord` join `check_tla.sh`;
  `FelixShardFencedAck` now includes the start record. See
  `docs/replication-design.md`, "Acknowledging by the followers".
- `scripts/check_tla.sh` takes configuration names to check only those.
- **Fleet features: cross-broker behaviour turns on only when an operator
  finalizes it.** A broker reports the features it implements at
  registration (`features`, optional), and the control plane tracks the set
  every live or draining broker supports. Support enables nothing:
  `POST /v1/fleet/features/{feature}/finalize` (or `felix-controlplane admin
  features finalize <feature> [--dry-run]`) enables one, refused until every
  serving broker supports it. Registration and every heartbeat answer with
  the enabled set as `fleet_features`; `GET /v1/fleet/features` and `admin
  features` show supported and enabled. Brokers check it through
  `felix_common::fleet::FleetGate` and export
  `felix_broker_fleet_feature_enabled`. Before a finalize any broker can be
  rolled back; after it, finalizing is one-way and a broker without the
  feature is refused at registration (409). No feature uses the gate yet.
  Postgres gains `nodes.features` and a `fleet_features` table; on Raft the
  control plane keeps features and accepts a finalize only once every member
  is at metadata version 2.

- **Operator moves report their zone impact.** `POST /v1/shard-moves` answers
  `zones_before` and `zones_after` when brokers report zones, takes
  `"dry_run": true` to decide a move without starting it, and logs a move that
  narrows a shard's spread as a warning instead of refusing it.
  `felix-controlplane admin move` gains `--dry-run` and prints the zones.
- **The Helm chart runs brokers per zone.** `broker.zones` renders one broker
  StatefulSet per zone, pinned to its nodes and setting `FELIX_NODE_ZONE`, with
  one disruption budget across all of them.
- **Pipelined publishes.** A client that offers `FEATURE_PUBLISH_PIPELINE` is
  granted a per-connection `publish_window` in `auth_ok`
  (`FELIX_BROKER_PUBLISH_WINDOW`, default 256, `0` to turn off). The broker
  answers that client's acked publishes on each stream in request order and
  stops reading its publishes while the window is full. The Rust client offers
  it and caps each connection at the window, and
  `IdempotentProducer::publish_batches` keeps up to 64 batches in flight,
  re-sending from the first unanswered one after a failure. Older clients see
  no change.
- **Connection-fault conformance scenarios.** A catalogue scenario can carry a
  `step` that drops, resets or stalls the client's link mid-publish or
  mid-subscribe. `felix_conformance::link` is the UDP interposer that does it;
  `felix-cluster client-fixture` serves one at `link_addr`, breaks it on
  `POST /link`, and lists the steps under `faults`. The protocol suite runs
  them with the Rust client, and the Python and TypeScript suites run them in
  CI. A subscription must resume with no gap or raise; ending as if the stream
  had finished fails.
- **Placement spreads a shard's copies across zones.** A broker registers
  the failure domain it is in with `FELIX_NODE_ZONE` (the node's new optional
  `zone`). Followers go to zones the shard has no copy in, drains and
  rebalances never narrow a shard's zones when they can avoid it, and a
  follower sharing a zone is replaced by one in a missing zone once a broker
  there has room. Where the copies cannot be spread they are placed anyway,
  with a warning and `felix_shards_zone_unspread`. A broker without a zone
  shares one with nobody, so clusters that report none are placed as before.
  Postgres gains a nullable `nodes.zone` column.
- **A promoted leader fences a majority before it serves.** A broker promoted
  to lead a stream shard now waits in a `fencing` phase: it sends `Fence` to
  every replica, opens once a majority (itself included) has persisted its
  generation, and first takes the log of the replica furthest ahead by
  (last generation, length) with the new `ReplicateFetch` (kind 31,
  capability `TAIL_FETCH`). A deposed leader's ships are then refused by that
  majority even where the control plane never reached the follower. Applies
  only when every replica offers both capabilities; otherwise the shard opens
  on the lease as before. `felix_broker_promotions_opened_total{path}` says
  which. Acknowledgements are unchanged: still the report and the lease.
  Each peer connection opens one more stream, for the handshake and the
  fence's requests only, so they never wait behind a forwarded publish.
- **Replicas answer a promoted leader's fence.** A new peer message, `Fence`,
  makes a replica persist the leader's generation before it answers with its
  log end, commit offset and last record's generation, and refuse every older
  leader from then on. It is negotiated: brokers offer capability bits in the
  peer handshake (`HelloCapable`, kinds 27 and 28), and a peer that did not
  offer `FENCE` is never sent one. `FELIX_INTERNAL_FENCE=false` withdraws the
  offer. No leader sends the fence yet.
- **Clients can offer the `felix/1` ALPN.** `felix_client::quic_client_config`
  builds the QUIC TLS config and offers `felix/1` when asked; the Python
  (`offer_alpn=True`) and TypeScript (`offerAlpn`) clients expose the same
  switch. Off by default, since a broker older than ALPN support refuses a
  client that offers one. Before this no shipped client could connect to a
  broker with `FELIX_TLS_REQUIRE_ALPN=true`.
- **The licence split is checked on the dependency graph.**
  `scripts/check_license_graph.py`, run by `task publish:check`, fails if an
  Apache-2.0 or MIT first-party crate has a normal or build dependency, direct
  or transitive, on an AGPL one.
- **Advisories reach the vendored `kafka-protocol`.** A patched-in path crate
  has no registry source, so cargo-deny skipped it. `vendor/VENDORED.toml`
  records the upstream version (0.18.0) and `task deny` checks advisories
  against it, failing if the record and the vendored copy disagree.

- **Kafka producers can write to durable streams.** `Produce` (v3-9) on the
  Kafka listener decodes v2 record batches, compressed with gzip, snappy, lz4
  or zstd, and publishes them through the broker's publish path on the shard's
  leader, with `stream.publish` checked as for a QUIC publish; any other broker
  answers `NOT_LEADER_OR_FOLLOWER`. `acks=all` waits for the stream's own
  consistency: a majority on a `Quorum` stream, the leader on a `Leader` one.
  `InitProducerId` gives idempotent producers a Felix producer id, and the new
  `Broker::publish_records_idempotent` stores each record as a one-record
  producer batch so Kafka's per-record sequences are the log's own: a batch
  re-sent after a failover or a move is answered with its original offset.
  Transactions are refused with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` and a
  message; legacy v0/v1 message sets with `UNSUPPORTED_FOR_MESSAGE_FORMAT`.
  Keys, headers and producer timestamps are dropped and counted. New metrics:
  `felix_kafka_produce_records_total`, `felix_kafka_produce_bytes_total`,
  `felix_kafka_produce_duplicate_records_total`,
  `felix_kafka_produce_errors_total` and `felix_kafka_produce_dropped_total`.
  `FELIX_KAFKA_ANONYMOUS_TENANT` now allows writes as well as reads. The
  docs-site page "Reading with Kafka Clients" is now "Kafka Compatibility".

- **Kafka consumers can read durable streams.** A broker started with
  `FELIX_KAFKA_LISTEN` serves the Kafka protocol, read-only, for consumers that
  assign their own partitions: kcat, librdkafka programs and Java
  `KafkaConsumer`s. A durable stream is topic `<namespace>.<stream>`, partition
  N is shard N, and offsets are Felix's log offsets. It speaks `ApiVersions`,
  SASL/PLAIN (tenant id as username, Felix token as password, reads checked as
  `stream.subscribe`), `Metadata`, `ListOffsets` and a long-polling `Fetch`,
  and a consumer follows a partition to its new leader after a move or
  failover. `FELIX_KAFKA_ADVERTISE_ADDR`, `FELIX_KAFKA_TLS` (on by default,
  with the broker's certificate), `FELIX_KAFKA_ANONYMOUS_TENANT`,
  `FELIX_KAFKA_DEFAULT_NAMESPACE` and `FELIX_KAFKA_MAX_CONNECTIONS` configure
  it. Each broker registers its advertised address as the node's new
  `kafka_addr` field so any broker can name any partition's leader; migration
  `0018_node_kafka_addr.sql` adds the column. Consumer groups are refused:
  `FindCoordinator` answers `GROUP_AUTHORIZATION_FAILED` with a message saying
  to assign partitions, so a group consumer fails with a reason instead of
  hanging. `Produce` is refused with `POLICY_VIOLATION`. Metrics are under
  `felix_kafka_*`. `docs/kafka-compatibility.md` replaces the spike write-up,
  and the spike crate under `spikes/` is removed.

- **Streams can be kept in a region.** A stream created with `"region"` has its
  leader and every replica only on brokers in that region, or in one the new
  directional allowlist `FELIX_REGION_BRIDGES` (`source>dest` pairs) bridges it
  to: on first placement, rebalancing and drain moves, follower replacement and
  failover. A copy found outside, after a broker's region changes or a bridge is
  removed, is moved back in; with no broker in an allowed region the shard is
  left unplaced rather than placed elsewhere. An operator move out of the
  region is refused with `region_not_allowed`. Brokers read the same
  `FELIX_REGION_BRIDGES` and forward only to leaders in bridged regions,
  refusing the rest as `shard_unavailable` with the new reason
  `region_not_routable` instead of `owner_unavailable`. Streams without a
  region, and every cache, are placed as before. Migration
  `0017_stream_region.sql` adds a nullable `streams.region` column.

- **Consumer-group calls follow the shard's leader.** `ClusterClient` gains
  `group_poll`, `group_poll_wait`, `group_ack`, `group_nack`,
  `group_dead_letters`, `group_discard` and `group_redrive`, which follow the
  broker's `NotLeader` redirect to the shard's leader and remember it. The
  Python and TypeScript clients' group calls now use them, so they no longer
  fail with `ShardUnavailableError` (`not_leader`) when connected to a broker
  that does not lead the shard, or after a shard move. `Client`'s group calls
  still return `NotLeaderError` rather than follow it.

- **A broker hands its shards off before it stops.** On SIGTERM a
  clustered broker turns readiness off, drains itself through the control
  plane and keeps serving until it leads no shard, then shuts down as
  before, so a rolling restart moves shards instead of failing them over: no
  publish is refused and subscriptions follow the shard. Bounded by
  `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` (default 30 s, `0` turns it off);
  skipped when there is nobody to hand to or the control plane does not
  answer, and ended early by a second signal. Metrics:
  `felix_broker_shutdown_handoffs_total{outcome}`,
  `felix_broker_shutdown_handoff_shards_total`,
  `felix_broker_shutdown_handoff_duration_ms`. The Helm chart adds
  `broker.shutdown.handoffTimeoutMs` and counts it in the derived grace
  period; a grace period set by hand must now cover it too.
- **Cache, counter and consumer-group operations are no longer refused during
  a shard move.** A cache put, delete or read and a counter add or read that
  arrives between a move's fence and its cut-over is held and sent to the new
  owner, as a publish already was, on the broker it reached and on an owner a
  forward reached. A consumer-group poll, ack, nack or dead-letter change is
  held and then answered with `NotLeader` naming the new owner, which
  `ClusterClient::group_sharded` follows; group operations are still served
  only by the shard's leader. A group poll waiting for records when its shard
  moves away now answers with no records instead of an error. Bounded by the
  same `FELIX_SHARD_MOVE_HOLD_MS` and `FELIX_SHARD_MOVE_HOLD_MAX`.
- **`routable` in the node listing.** `GET /v1/nodes` now says whether other
  brokers may send a node requests for the shards it leads: live or draining,
  heartbeat inside the window. Brokers forward on it instead of on
  `eligible`, so writes through another broker to a draining broker's shards
  are no longer refused before each shard's turn to move.

- **Idempotent producers keep their sequences across a leader change**
  (#608). On a durable stream each record of a producer's batch is now stored
  with its producer id and sequence, and replicated with them, so a leader
  promoted after a failover, a planned move's destination and a restarted
  broker answer a re-send of the batch in flight from the records they hold,
  and the producer carries on instead of being refused as `unknown_producer`.
  A batch whose leader stopped partway through it is finished by the re-send
  rather than written twice. A producer is remembered while any of its
  batches is in the log, so retention now decides when one is forgotten, on
  every replica alike. In-memory streams keep sequences in the leader's
  memory as before. Opening a shard reads no more than before: the producer
  state is saved at each rollover and the rest comes from the active segment
  recovery already scans. New metric `felix_storage_producer_state_rebuilt_total`
  counts opens that had to read sealed segments to rebuild it.
- **Subscriptions follow a moved shard.** A broker that stops serving a shard
  now ends each of its subscriptions and cache watches with a final
  `shard_moved` frame: the offset to resume from, the broker taking the shard
  when known, and the generation that moved it. Only a client that offers
  `FEATURE_SHARD_MOVED` (`0x1000`) gets it; any other sees the stream end byte
  for byte as before. For a durable stream `resume_from` is exact, so resuming
  at `max(last delivered + 1, resume_from)` neither repeats nor skips a record.
  A `ClusterClient` subscription does that itself: it resubscribes on the new
  owner, retrying until the move has cut over, and an in-memory stream resumes
  at the new owner's tail. A `Client` subscription ends with
  `Subscription::shard_moved()` set, and cache watches deliver
  `CacheWatchItem::ShardMoved`. Sharded subscriptions and watches report
  `ShardEvent::ShardMoved` and `ShardedCacheWatchItem::ShardMoved`. Python and
  TypeScript subscriptions follow too, and surface the move as `ShardMoved` /
  `CacheWatchShardMoved` (Python) and `shardMoved` (Node). See "Shard moves" in
  `docs/protocol.md`.
- **Publishes are not refused while their shard moves.** A publish that
  reaches a broker between a move's fence and its cut-over is held, before it
  is accepted, until the broker's routes show the new owner, and is then sent
  there. It is refused only if the move takes longer than
  `FELIX_SHARD_MOVE_HOLD_MS` (default 2000) or `FELIX_SHARD_MOVE_HOLD_MAX`
  (default 1024) publishes are already waiting, with `shard_unavailable` and
  the new reason `moving` (retry class `retry`, with a `retry_after_ms` hint).
  New metrics `felix_broker_shard_move_held_total`,
  `felix_broker_shard_move_hold_seconds` and
  `felix_broker_shard_move_hold_refused_total{reason}`.
- **A move's destination does not count toward the quorum while it copies.**
  A `Quorum` publish waits for a majority of the replicas the stream asked for,
  not for the copy, and the copy is shipped in slices so it never holds a
  replication pass for long.
- **Shard moves are paced.** Every copy holds a move slot, including a
  follower being replaced on a draining broker, which is now copied in beside
  the follower it replaces (named `joining`) and seated once it has caught
  up, so a shard never drops below its replication factor while it fills.
  `FELIX_SHARD_MOVES_MAX_PER_NODE` bounds copies into or out of one broker.
  Drains get slots before rebalancing. A move fences once its destination is
  within `FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS` (default 1000) of the
  leader's tail rather than exactly level, which under steady writes it may
  never be; replica reports carry the leader's tail (`leader_offset`) for
  this. A move that has not reached its fence within
  `FELIX_SHARD_MOVE_TIMEOUT_MS` (default 30 min) is abandoned as a
  `timed_out` step, counted in `felix_shard_moves_timed_out_total`, and waits
  behind other shards for its next slot; a fenced move is always finished.
  On the broker, `FELIX_SHARD_MOVE_BYTES_PER_SEC` limits what a leader ships
  to move destinations that the quorum does not need
  (`felix_broker_replication_move_throttled_bytes_total`). Migration
  `0014_shard_move_pacing` adds the assignment and report columns.
- **Operators can steer shard moves.** `GET /v1/shard-moves` lists the moves
  in progress with their step, the reason they started (`drain`, `balance`,
  `operator`, `replace`, stored as `move_reason` on the assignment), start
  time and lag; `GET /v1/placement/plan` shows what the next placement pass
  would do without doing it. `POST /v1/shard-moves` starts a move to a named
  broker, held to the move limits. `DELETE
  /v1/shard-moves/{tenant_id}/{namespace}/{name}/{shard}` cancels one: before
  the fence the destination is dropped; after it the fenced leader serves
  again at a new generation (`retake`) with every write it accepted, and held
  publishes and followed subscriptions find it again; after the cut-over it is
  a 409. `POST /v1/placement/pause` and `/resume` stop and restart
  placement's own moves on every instance; moves in flight finish, and new
  shards and failovers are still placed. Reads take `node.view:cluster:*`,
  changes `node.manage:cluster:*`. `felix-controlplane admin` does the same
  from a shell (`moves`, `plan`, `move`, `cancel`, `pause`, `resume`, with
  `--json`). Migration `0015_operator_moves` adds `move_reason` and the
  `placement_settings` table; the Raft backend gains a `set_moves_paused`
  command, which an older member refuses. `felix_shard_move_steps_total`
  gains the `cancel` and `retake` steps.

- **Faster shard move switch-over, control-plane side.**
  `GET /v1/shard-assignments/changes` takes an optional `wait_ms`: with
  nothing newer than `since`, the request waits up to that long (capped at
  25 s) and answers as soon as a change lands. It holds no store connection
  while waiting, and without `wait_ms` it behaves exactly as before. A replica
  report that a move is waiting for (a drained leader, or a caught-up staged
  successor) now wakes placement at once instead of at its next tick; wakes
  coalesce and one pass runs at a time. New histograms
  `felix_shard_move_duration_seconds` and `felix_shard_move_fence_seconds`
  time moves from the steps each control-plane instance writes.

- **Typed error codes on the client wire.** A client that offers
  `FEATURE_ERROR_CODES` (`0x0800`) gets a `code`, a `retry` class and an
  optional `detail` on every `error` and `publish_error`, and on a failed binary
  publish ack when it also offers `BINARY_PUBLISH_ACK_CODE` (`0x0200`). The
  retry class says whether the request may have been applied: a quorum timeout
  is now `quorum_timeout` with `outcome_unknown`, distinct from a refusal, and
  `shard_unavailable` names its reason. An unknown code decodes with its retry
  class instead of failing the frame. Clients that do not offer the bit get
  byte-identical frames. A broker that is draining now answers `auth` on a new
  control stream with `draining` for clients that offered the bit. The Rust
  client offers it and exposes the code on `felix_client::BrokerError`. See
  "Error codes" in `docs/protocol.md`.

- **The Rust `ClusterClient` acts on the broker's error codes.** Retry,
  reroute or fail is decided by the retry class instead of by matching the
  message. A cached shard owner that answers `shard_unavailable` (including
  `fenced`), `draining` or `not_leader` is forgotten and the publish goes to
  the entry broker at once, from `publish` too, since nothing was applied.
  `outcome_unknown` is never re-sent by `publish`; `publish_at_least_once` and
  the idempotent producer re-send it. `retry_after` honours `retry_after_ms`,
  and `not_found` is retried for 5 s from the first one rather than for the
  whole attempt budget. A fatal code such as `invalid_request` is no longer
  retried. `publish` reconnects only on `draining` or a dead connection, not on
  every coded refusal. A subscribe or cache-watch refusal is now a typed
  `BrokerError` (it was the debug text of the frame), and a subscribe, cache
  watch or group request whose redirect target answers `shard_unavailable`
  goes back to the entry broker once. Peers that did not negotiate codes get
  the old handling.

- **Python and TypeScript errors carry the broker's error code.** Both have
  `code`, `retry` and `detail`. The class is chosen from the code when
  the broker sent one, and from the message only when it did not. New classes:
  `ShardUnavailableError` (`shard_unavailable`, `not_leader`; retryable),
  `OverloadedError`, and `OutcomeUnknownError` (`quorum_timeout`,
  `leadership_lost`, `unacknowledged`, or anything sent as `outcome_unknown`);
  a draining broker is a `ConnectionError`. In TypeScript, `retryable` follows
  the broker's retry class when there is one, so a `NotFoundError` sent as
  `retry_after` is now retryable. Two conformance scenarios,
  `error.shard_unavailable_is_retryable` and
  `error.quorum_timeout_is_outcome_unknown`, hold both bindings and the Rust
  client to this, against faults `felix-cluster client-fixture` now serves on
  a localhost control endpoint (`POST /fence`, `/partition`, `/heal`).

- **Online shard rebalancing** (#130). A shard whose leader is alive is now
  moved instead of reassigned. The control plane stages the destination as a
  replica and lets the leader catch it up. Once the copy is level it fences the
  leader (the assignment goes `draining`, and a broker never serves a draining
  assignment), waits for the leader to report that its log has stopped
  growing, and only then names the destination leader at a new generation.
  Each step is an assignment write, so any control-plane instance resumes a
  half-done move from the store.

  Two triggers. A drained broker (`POST /v1/nodes/{id}/drain`) hands off
  everything it leads and is replaced as a follower wherever it holds a copy,
  so it can then be deleted. A broker leading more than its share of shards
  gives them to one under its share, which is what makes a broker that joins
  a running cluster take work. A cluster where a control-plane restart left
  every shard on the first broker to register now corrects itself. Moves only
  go from over share to under share and count moves in flight as done, so
  placement converges instead of oscillating. `FELIX_SHARD_MOVES_MAX_CONCURRENT`
  bounds moves in flight, one by default.

  A destination that dies before it leads is passed over; a leader that dies
  mid-move is an ordinary failover with the destination as a candidate.
  Publishes to the shard are refused, not lost, between the fence and the new
  owner opening. Real-process tests cover drain, join, a destination dying
  mid-transfer, a drain and a join at once, and publishes arriving throughout.

  The fence is model-checked. `docs/formal/FelixShardHandoff.cfg` adds the
  planned move to the TLA+ spec and explores 2.7M states without a
  violation; `FelixShardHandoffNoWait.cfg`, which cuts over as soon as the
  fence is written, finds two brokers serving one shard in seven steps.

  The assignment carries an optional `successor`, the replica report an
  optional `drained`; both are omitted when unset, so nothing changes on the
  wire for a cluster that never moves a shard. Postgres gains migration
  `0012_shard_moves`. `felix_shard_move_steps_total{step}` and
  `felix_shard_moves_waiting` report progress. The harness gained `add_node`,
  `drain_node`, `undrain_node` and `drain_until_empty`.

### Changed

- **aws-lc-rs is the only TLS crypto provider.** `ring` is gone from the
  build: quinn, rcgen and reqwest now use aws-lc-rs, which rustls and
  jsonwebtoken already did. The broker and control plane install it as the
  process-wide rustls default at startup. reqwest moves to 0.13, so HTTPS
  calls to the control plane and to IdPs are verified against the system
  trust store (plus `FELIX_CONTROLPLANE_CA`) instead of a bundled copy of
  the Mozilla roots; the container images already ship `ca-certificates`.

- **Replication and the broker-to-broker transport are their own crate,
  `felix-replication`.** `felix_broker_service::peer` is now
  `felix_replication::peer`, and `felix_broker_service::replication::*` is at
  the root of `felix_replication`. `ShardKey` and `ShardKind` are defined there
  and still re-exported from `felix_broker_service::shards`. No behaviour
  change.

- **`ClusterClient` holds one connection per broker.** Every role a broker
  plays (entry, shard owner, redirect target, producer leader) shares one
  client, and every stream to it is multiplexed on one QUIC connection, where
  each role used to open a full `Client` of 20 connections. More open only
  when the streams on it saturate, up to `cluster_conn_pool`
  (`FELIX_CLUSTER_CONN_POOL`, default 8); `cluster_streams_per_conn`
  (`FELIX_CLUSTER_STREAMS_PER_CONN`, default 1024) sets the stream budget.
  `ClusterClient::connections_per_node` and `Client::connection_count` report
  the counts. `Client::listeners_in_use` now returns a `Vec`.

- **`felix-conformance` is AGPL-3.0-only.** It links the broker, storage and
  authz crates to run its suite, so a build of it was AGPL whatever its label
  said. It is not published; running `verify` over a client's results puts no
  obligation on that client.

- **OTLP tracing export is off unless an endpoint is set.** The broker and the
  control plane used to install the OpenTelemetry layer unconditionally, so a
  deployment with no collector exported to `localhost:4317` and logged a
  failed export for every batch. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (or
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) to turn it on.

- **IdP group claims are always prefixed.** A group claim value `X` now maps to
  the RBAC subject `group:X` even when `X` already starts with `group:`, so an
  IdP group named `group:operators` is no longer treated as the `operators`
  group. Where an IdP was set up to emit pre-prefixed values, rename the
  groupings (`group:group:<name>`) or emit the bare name.

- **Token exchange and refresh refusals are counted.** Both now land in
  `felix_controlplane_auth_rejected_total`; unusable refresh tokens use the new
  `refresh_refused` reason. The refresh replay, bad-secret, revoked and issued
  counters are now in the control plane's metrics table.

- **`MovePolicy` is no longer `Copy`.** It carries the region allowlist
  (`regions: Arc<RegionRouter<String>>`); clone it where it was copied. The
  control-plane `Stream` model gains `region: Option<String>`, and the broker's
  `MembershipConfig` gains `region_bridges`.

- **Cache watches follow a moved shard.** `ClusterClient::watch_cache` and
  `watch_cache_retained` now return a `ClusterCacheWatch` (and take
  `self: &Arc<Self>`). When the shard moves it hands out
  `CacheWatchItem::ShardMoved` as a notice and reopens the watch on the new
  owner at `max(offset after the last change, resume_from)`, so the caller
  no longer reopens it and sees no change twice or missing. A sharded cache
  watch follows each shard the same way; `ShardedCacheWatchItem::ShardMoved`
  now means the shard is being followed, and a `ShardClosed` follows if it
  could not be. The Python and TypeScript watches follow too. A `Client`
  watch still ends with the frame.
- **A drain moves leaders before it replaces followers.** Move slots on a
  draining broker go to the shards it leads first, then to its follower
  copies, so with the default limit of one a follower's copy no longer holds
  the slot while the broker's leaderships wait.
- **Online rebalancing is marked done** on the status page. The row now
  cites the tests behind each claim and names what is left: cache, counter
  and group writes are refused briefly during a switch-over, the move limits
  hold per planner, and stopping a broker is a failover rather than a
  handoff. Load-aware placement has its own row. Pages that still said a
  move refuses publishes or ends subscriptions rather than letting them
  follow were corrected, and the docs-site semantics page no longer says a
  cache cannot declare `Quorum` or that metadata lives only in Postgres.
- **Storage format v3.** A record's length word now carries two flag bits
  and, for the first record of an idempotent producer's batch, a 20-byte tag
  (see `docs/storage-format.md`, "Producer marks"). v2 segments are still read
  and an unmarked record is byte for byte a v2 record, but new segments are
  written as v3, which a v2 build refuses to open: downgrading past this needs
  the data directory discarded. A shard directory may also hold a `producers`
  snapshot beside `epochs`.
- **Internal protocol kind 25, `ReplicateMarkedRecords`**, ships records
  together with their producer marks. A batch without marks still travels as
  `ReplicateRecords`, unchanged; a follower that predates the kind refuses it
  and is halted rather than storing records without their marks. The API
  changed with it: `ReplicateRecords` has a `marks` field, `batch_checksum`
  takes the marks, `AppendRecord` and `LogRecord` have a `mark`, and
  `felix_broker::replication::apply` takes the marks.
- **Breaking for Rust callers of the control-plane crate:** `AppState` has a
  `move_policy` field, `ShardAssignment` a `move_reason` field, `MovePolicy` a
  `paused` field, and `ControlPlaneStore` the `moves_paused` and
  `set_moves_paused` methods. `Default::default()` fills the first three.
- **Breaking for Rust callers: `ClusterClient::subscribe` and `subscribe_from`
  take `self: &Arc<Self>` and return a `ClusterSubscription`** instead of a
  `(client, Subscription)` pair, so the subscription can follow its shard.
  `ShardEvent` is `#[non_exhaustive]`, and `CacheWatchItem` and
  `ShardedCacheWatchItem` have a new `ShardMoved` variant, so exhaustive
  matches need an arm. On the broker, `Broker::end_subscriptions` and
  `CacheWatchHub::end_shard` take an optional argument saying where the shard
  went.
- **`max_shards` caps roles when choosing a move's destination.** It was
  compared against leaders only, so a node full of follower roles could be
  given a shard to lead. Placement's load now counts existing followers too.
- **A replica report leaves out followers the leader could not reach**, as it
  already left out halted ones, so an unreachable move destination is never
  fenced on its last position.
- **`MovePolicy` gained fields** (`max_per_node`, `fence_max_lag_records`,
  `timeout_millis`); the control-plane config holds it as `shard_moves`.

- **Breaking for TypeScript callers: `err.code` is now the broker's error
  code.** It used to hold this client's own kind (`FELIX_AUTH`,
  `FELIX_CONNECTION`, …), which moves to `err.kind`. `code` now matches the
  Python exceptions and the protocol (`forbidden`, `shard_unavailable`, …) and
  is `undefined` when the broker sent no code, as are `retry` and `detail`;
  `kind` is always set. Code that switched on `err.code === "FELIX_AUTH"`
  should switch on `err.kind`, or on the class.

- Fresh placement bounds leaders as well as roles. With a replication factor
  equal to the node count every node holds a role for every shard, so the
  role bound was satisfied with every leader on one node, and the new
  rebalancer would then move them apart again. A cluster placed from scratch
  now needs no move to be balanced.
- A `draining` assignment is reachable from `assigning` as well as `active`,
  since nothing reports `active` yet and the fence should not cost a move an
  extra generation. A drained shard still leaves only through a fresh
  `assigning`.
- A node marked draining no longer has its shards reassigned on the next
  placement pass to brokers that hold none of their log. Its shards are moved
  instead, which takes a few passes; the harness's `move_shard` steps
  placement until the move completes.
- `felix_router::shard` is no longer public. Everything in it was already
  re-exported at the crate root, so `felix_router::ShardRouter` and friends
  are the paths to use.
- `felix-common` drops what nothing used: `NodeConfig`, `LimitsConfig`,
  `Error::Config`, and every id type but `RegionId`.
- `felix-broker`'s `cache_watch`, `consumer_groups`, `dead_letters`,
  `durable`, `group_delivery` and `group_reader` modules are private. Their
  public types are at the crate root (`felix_broker::ConsumerGroups`,
  `felix_broker::GroupReader`, `felix_broker::DurableStorage`, ...), and the
  group tracker that `group_delivery` exposed is internal. `replication` and
  `timings` stay public modules. `ClaimedPublish` is now exported, since
  `Broker::claim_publish` returns it. The registry keys (`CacheKey`,
  `NamespaceKey`, `StreamKey`, `TopicKey`) are no longer public; nothing
  outside the broker used them.
- **The workspace is grouped by role.** Crates live under
  `crates/{protocol,server,sdk,testing}/` and the service packages are renamed
  `felix-broker-service` and `felix-controlplane-service` (binaries unchanged),
  so `cargo test -p broker` is now `cargo test -p felix-broker-service`. Crate
  internals are reorganised by domain; `CONTRIBUTING.md` has the rules.
- `felix-storage` paths: `CacheOp` is `felix_storage::cache::CacheOp`, `Epoch`
  is `felix_storage::log::Epoch`, the `Corruption*` types are only at the crate
  root, and the modules nothing outside the crate used (`commit_order`,
  `segment::io`, `disk_log::{epochs, recovery, retention, segments, sync}`) are
  private.
- `felix-controlplane-service` paths: the router and `AppState` are in `api`
  (was `app`), readiness is `api::readiness`, TLS is `server::tls`,
  `membership` and `placement` (with `ReplicaPositions`) are under `cluster`,
  the Raft store is `store::raft` with `command` and `state_machine` beneath it,
  metadata export is `store::export`, and `now_millis` is `clock::now_millis`.
  Two integration tests are renamed: `readiness_pg` is `pg_readiness` and
  `meta_raft` is `raft_state_machine`.
- `felix-broker-service` modules are grouped by job, so their paths changed:
  - `quic`/`transport::quic`, `auth` (with `auth_demo` as `auth::demo`) and
    `core_shards` are under `serving::`, as is forwarding to a shard's owner
    (`serving::forward`, from `peer::forward` and `peer::handler`).
  - `credential`, `membership`, `lease`, `node_catalog` and `client_endpoints`
    are under `cluster::`, and `controlplane` is `cluster::catalog_sync`.
  - `shard_watch`, `shard_lifecycle` and `shard_routing` are `shards::watch`,
    `shards::lifecycle` and `shards::routing`.
  - `peer::replica` is `replication::replica`, `peer::dispatch` is
    `node::peer_dispatch`, `durable_config` is `config::durable`, and `timings`
    is `observability::timings`. `peer` is now only the broker-to-broker
    transport.
  - Startup moved out of the binary into `node::run_with_shutdown`.
    `cache_routing` and `group_ops` are no longer public.
  - Log targets follow module paths, so a `RUST_LOG` filter such as
    `felix_broker_service::quic=debug` becomes
    `felix_broker_service::serving::quic=debug`.
- Only `felix-wire`, `felix-transport` and `felix-client` are published to
  crates.io. The server crates were only there because of the `in-process`
  feature below.

### Removed

- **`felix-client`'s `in-process` feature and `InProcessClient`.** It wrapped
  an embedded `felix_broker::Broker` for two smoke tests and nothing else used
  it, but it made `felix-broker` and `felix-storage` optional dependencies of
  the client, and so forced them onto crates.io. Test against a broker over
  QUIC instead; `felix-cluster` starts one.
- **`Broker::in_flight_publishes`.** It counted claimed publishes for the
  drained report, which now reads the broker service's per-shard write fence
  instead.

### Fixed

- **A power loss on a fresh node keeps its store roots (#888).** The stream,
  group, dead-letter, cache and counter roots were created without syncing the
  directory that holds them, so a power loss before anything else synced it
  could drop a root and the acknowledged records in it. Every new directory is
  now synced into its parent, and opening an existing root syncs it again.
- **A new copy of a trimmed shard is placed without a rebuild.** The leader's
  first batch opened an empty log at 0 on the new broker, which then refused
  the leader's bootstrap offer as if it held records. The copy waited for a
  rebuild slot, or for ever with `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT=0`.
  An empty log now takes the offered base.
- **A subscription follows a moved shard off a draining entry broker.** A
  `ClusterClient` reader told `shard_moved` with no new owner named (a lapsed
  lease or a deposed leader ends readers that way) asked only its entry
  broker. When that was the old owner shutting down, it answered `draining`
  and then nothing, and the reader failed with "the new owner did not take it
  before the deadline". The follow now moves to another known broker when the
  entry broker answers `draining` or not at all, and a reconnect tries the
  broker it was using last. See `docs/multi-node-client.md`.
- **A broker handed a shard back after dropping held batches takes writes
  (#881).** Liveness only: no acknowledged write was lost. A lease-mode leader
  that lost its lease with `Quorum` batches still held dropped them, followed
  the new leader, and answered a batch shipped again with that batch's end.
  Its stream's commit order moved back below records already in its log, so
  once the shard moved back to it every publish timed out with
  `publish commit timeout`. The commit order now never moves backwards on a
  replicated batch. See `docs/replication-design.md`, "Replication".
- **A new leader names no follower short of the log it inherited (#882).**
  Until a majority held a record of the new generation, a leader measured its
  replica report against offset 0 and named every follower caught up,
  answered or not. In lease mode, where promotion trusts the report, two
  failovers in quick succession could promote a follower missing a record the
  first leader acknowledged. Stream shards promoted with the fence were
  covered by it; `Quorum` caches and unfenced stream promotions were not.
  The report, and a cache's counter level, now never name a follower short
  of where the leader's generation begins. Right after a failover, a new
  leader that dies before any follower has copied its inherited log leaves
  the shard unplaced until a broker holding that log returns. See
  `docs/replication-design.md`, "Who may be promoted".

- **A leader counts a follower only for what it has answered (#878).** The
  quorum mark and the replica report counted a follower the leader had not yet
  heard from at the offset its cursor starts at, one record below where the
  leader's own generation begins. A new leader, or a move's source, that could
  reach none of its followers then published a mark over records it inherited
  and no follower held: its commit offset covered them, and readers could be
  handed them as committed. Deposed, the broker refused to drop them and
  refused every rebuild, so it never rejoined. Both now count
  `FollowerCursor::confirmed`, what the follower answered holding at this
  generation. No acknowledged write was affected: a publish waiting on the
  mark is dropped when its generation ends. See `docs/replication-design.md`,
  "Replica reports and the committed mark".

- **A `Quorum` failover no longer promotes a move's destination.** Its broker
  opened the shard as the move's cut-over, without the promotion fence, and
  under `majority_ack` the report that named it caught up could predate a
  record the old leader acknowledged on its followers, so that record was
  lost. Failover on a durable `Quorum` stream now ends the move instead. A
  promoted leader that gets a new generation while still fencing also keeps
  replication paused until it reopens, rather than shipping its unfenced log
  at the old generation in between.
- **A cache leader that loses its leadership rejoins as a follower (#863).**
  Cache leaders now record where their generation began, on the cache and
  counter logs, and accept the generation on both. Before, a cache leader that
  died holding a record no majority had came back with no generation history,
  halted on that record, and refused the new leader's rebuild for the rest of
  the generation, so a later drain could wait out the 30-minute move timeout.
  A rebuild now keeps the follower's records below its commit offset and drops
  only what lies past them; the leader re-ships from its base, so those records
  are compared as they arrive, and a committed record that disagrees halts
  loudly (`below_commit`) and refuses further rebuilds from that leader. A
  refused rebuild is asked again after a backoff (5 s, doubling, capped at
  5 min) instead of never. `FollowerCursor::rebuild_refused` is now an
  `Option<RebuildBackoff>`. See `docs/replication-design.md`, "Rebuilding a
  halted follower".

- **A `Quorum` failover keeps its replica set.** Failover used to rebuild the
  followers from live brokers, dropping the dead leader and adding a broker
  that had never held the shard. With `majority_ack` finalized the promoted
  leader fences a majority of that new set, which it could make with the
  newcomer alone, or with nobody when no other broker was live, and so open
  without a write the old leader and another follower acknowledged. A
  durable `Quorum` stream now keeps the previous set, the dead leader in it,
  so the fence needs a majority of the set that acknowledged. The dead
  member stays a follower until it returns or is drained. Upgrade the
  control plane before finalizing `majority_ack`.
- **Replacing a `Quorum` follower keeps what the old set acknowledged.** A
  drain seats the replacement and drops the old follower once the
  replacement is within the move lag bound, or once a report names it caught
  up, which a leader does for every follower at a new generation before
  anything is counted there. Seated early, the replacement and a lagging
  follower were a majority of the new set without a record the leader and
  the old follower acknowledged, and a failover then lost it. On a durable
  `Quorum` stream the seat now also waits until the replacement holds what a
  majority of the set holds.

- **A subscription's `live_offset` is reachable after a leadership change.**
  It was the raw log tail, so when the log ended in a new leader's
  generation-start record, which is never delivered, a reader waiting to reach
  `live_offset` waited until the next client write. It now stops short of
  trailing generation-start records, never below `start_offset`.
- **A cache write the store refused is no longer acknowledged.** A put or
  delete whose log write failed (a failed fsync poisons the shard) was
  logged and answered as a success, so a `Quorum` cache could acknowledge
  writes that later reads did not return. It is now a storage error, and a
  failed read is an error rather than a miss. `felix_storage::StorageApi`'s
  `put`, `get` and `delete` return `Result`.
- **A Raft leader elected after a clock step back expires a dead broker in
  one window.** The leader judged a broker it had not heard from by the
  log's stamp when that was later than its own start, so a stamp left in the
  future by a step back, or by an old leader whose clock ran ahead, kept a
  dead broker placeable until real time caught up. It now ages every broker
  on its monotonic clock only, from its election or the broker's
  registration, and lists a stamp ahead of its clock as now. Separately, a
  leader that died was counted at metadata level 0 by its successor, which
  then fell back to heartbeats and expiry through the log for as long as the
  old leader stayed down. The member probing a peer's level now states its
  own, so every member knows the leader's.
- **A record a new leader inherited is no longer lost after it was
  acknowledged** (Raft's Figure 8), once an operator finalizes the new fleet
  feature `generation_start`. A leader then appends a generation-start record
  whenever it starts leading a stream shard (after a promotion's fence, at
  either end of a move, on a cancelled move's hand-back) and its quorum mark
  counts a majority only once it reaches a record of the leader's own
  generation, so an inherited record is acknowledged only when a later fence
  cannot prefer a log without it. Acknowledged inherited records are readable
  at the new leader without waiting for a client write. Until the finalize,
  brokers count as before. Readers skip the record's offset; see
  `docs/protocol.md` for how a subscriber tells it from a drop. Finalize it
  only once every broker runs this version; it is one-way (see the upgrades
  page).
- **Storage format v4.** The generation-start record is flag bit 29 of a
  record's `payload_len`, and only a v4 segment may hold one. Segments stay
  v3 until a log's first record, so an upgrade stays reversible until
  `generation_start` is finalized; after that an older build cannot open the
  logs. v2 and v3 segments are still read. No migration is needed.

- **Opening a `ClusterClient` subscription waits out a shard that is still
  opening.** `subscribe`, `subscribe_from` and `subscribe_sharded` failed at
  once on `shard_unavailable`/`not_ready`, which a leader answers while it
  fences its replicas after a promotion, including at cluster start. They now
  retry `retry`-class refusals with the `ReconnectPolicy` attempts and
  backoff, as a publish does; a `fatal` refusal still returns at once.
- **A cache or counter shard that an older build left mid-compaction opens
  whole again.** Up to 0.6.0-preview, compaction swapped a shard directory
  with `<shard>.retired` and `<shard>.compacting` siblings. Background
  compaction dropped that swap and its recovery, so on upgrade a shard
  stopped between the two renames opened empty. Opening a shard now settles
  the old swap first: it restores `<shard>.retired` when the shard directory
  is missing, and it deletes leftover siblings when the directory is there.
  The cleanup assumes that a shard directory next to `<shard>.retired` is
  complete, and deletes `.retired`. That holds for every release. It fails
  only for someone who ran an unreleased main build from between #789 and
  #797 on a half-swapped shard: that build opened the missing directory as a
  new, empty shard, and `.retired` held the only copy of the data. If you did,
  replace the shard directory with its `.retired` sibling by hand before
  starting this build.

- **A power loss during a replication truncation or reset no longer leaves a
  log that refuses to open.** Truncation unlinked the segments past the cut
  (sealed ones newest first, the active one last) and synced the directory once
  at the end. A reset created its new segment before deleting the old ones.
  So a power loss could leave segments that do not meet, and recovery
  reported corruption. Both now delete newest first with a directory sync
  after each unlink, and a reset creates its new segment only after the old
  ones are gone. An interrupted pass leaves a longer log, which replication
  cuts again. A head gap that an older build's retention left behind still
  needs a manual fix; see "A gap at the head left by an older build" in
  `docs/durable-storage.md`.

- **A power loss during a retention sweep no longer leaves a log that refuses
  to open.** Retention unlinked a sweep's segments without syncing the
  directory, so the device could keep a newer unlink and lose an older one.
  Recovery then found an offset gap and reported corruption. Segments are now
  unlinked oldest first with a directory sync after each, so an interrupted
  sweep leaves a longer log instead.

- **A `ClusterSubscription` read cancelled mid-resume no longer ends the
  subscription.** After a lost connection, `next_event` cleared the loss
  before resubscribing, so a caller that dropped the call partway (a read
  under `tokio::time::timeout`, and every Python `next_event(timeout=...)`)
  came back to the dead subscription and got `None`, as if the broker had
  closed the stream. The loss is now kept until the resubscribe lands, and the
  resubscribe (after a loss or a shard move) runs on a task of its own, so a
  caller polling with a timeout shorter than a reconnect no longer restarts it
  on every call and never finishes.
- **Python's `closed` tells a timeout from the end of the stream.** It was
  only set by `close()`, so after `next_event(timeout=...)` returned `None`
  there was no way to tell the two apart, which its documentation said there
  was. It is now also set once the broker ends the stream.
- **A publish in flight when its shard's log is reset no longer reaches
  readers.** A publish waiting behind earlier ones when the broker became a
  follower (or its log was rebuilt) was woken by the reset and went on to put
  its offsets from the old log into the cleared replay ring and fan them out.
  It now fails with `unacknowledged` and applies nothing.
- **Acks for forgotten group claims are retryable.** After a group's in-memory
  state was evicted or the shard failed over, an ack or nack for an earlier
  claim was refused as `invalid_request`, which is fatal. It is now the new
  `stale_claim` code with retry class `retry`; an older client sees an unknown
  code with that class. An offset past the log tail is still `invalid_request`.
- **A traced publish no longer panics a stream handler.** The binary batch,
  JSON publish, JSON batch and subscribe handlers held a span guard across an
  `.await`. When the task resumed on another worker the guard exited there,
  leaving a stale span on the first worker's stack; the next span created on
  that worker could then clone the closed span and panic with "tried to clone
  a span that already closed", killing the publish stream. The binary batch
  span is `info`, so this hit the default filter under load, and a slow
  `on_close` in the OpenTelemetry layer widened the window. The handlers now
  use `.instrument(span)`, and `clippy.toml` rejects span guards held across
  an await.

- **`felix_publish_requests_total` and `felix_publish_bytes_total` are recorded
  in a default build.** Both went through the `telemetry`-gated macros, so a
  broker built without that feature exported neither while serving traffic.
  Bytes are now counted for fire-and-forget (`accepted`) publishes too, as the
  payload sum of the request, and no longer for a batch whose acknowledgement
  timed out. The observability page marks which of the listed metrics still
  need the `telemetry` feature.

- **A `Periodic` fsync tick skips a log with nothing to flush.** Every open
  log fsynced on every tick, written or not: a broker with ~200 idle shard,
  cache and counter logs issued ~800 fsyncs/s.

- **Clients publishing one stream spread across a broker's listeners.** The
  stream-to-publish-stream hash used a seed fixed for the whole process, so
  every `Client` in a process sent a given stream down the same pool slot, and
  so to the same listener. On a four-listener broker fed by four load
  generators, two ports carried all the publish data (3.1 GB and 9.4 GB) and
  the other two only handshakes. The seed is now per client; one client still
  keeps each stream on one writer.

- **Concurrent "ensure signing keys" calls agree on one key set.** Postgres
  and in-memory control-plane stores checked for a tenant's keys and wrote new
  ones in separate steps, so racing callers each generated keys and all but
  the last returned a set that was then overwritten; tokens signed with it
  would not verify. Postgres now decides under the tenant row lock, the way
  auth bootstrap does, and the in-memory store under one write lock. The Raft
  store was already safe: install-if-absent is applied through the log.

- **A failover no longer costs a publisher 30 seconds.** A publish in flight to
  a leader that was killed waited out the 30 s ack timeout, because a killed
  broker sends no QUIC close and the connection only died at the 30 s idle
  timeout. The default QUIC idle timeout is now 6 s and keep-alives go every
  2 s (`FELIX_MAX_IDLE_TIMEOUT_MS`, `FELIX_KEEPALIVE_MS`), so the dead
  connection fails the waiting publish and `ClusterClient` retries on the new
  leader in about 6 s. Quiet connections stay open on the keep-alives. A client
  or broker that overrides only one of the two should keep the keep-alive well
  under the idle timeout.

- **A publish to a shard that cannot be served is no longer called "stream not
  found".** The broker kept the right code but replaced the message with
  `stream not found: ...` for every refused publish, so a shard that was
  moving, fenced or still opening read as a missing stream to operators and to
  clients without error codes. Only a stream that does not resolve says that
  now; any other refusal keeps its own code and text, e.g. `publish to
  t1/ns/orders refused: this broker is still opening the shard`. Clients that
  matched on the old text for these cases will see the new one; retry
  behaviour of coded clients is unchanged, since the code was always
  `shard_unavailable`.

- **Binary publish acks carry the error's detail.** A failed binary ack had the
  code but dropped `detail`, so a binary publisher never learnt a
  `shard_unavailable` reason or a `moving` shard's suggested wait. The new flag
  bit `FLAG_BINARY_PUBLISH_ACK_DETAIL` (`0x0400`), negotiated like
  `FLAG_BINARY_PUBLISH_ACK_CODE`, adds it after the code; `felix-client`
  offers it and fills `BrokerError::detail` from it. The new
  `encode_publish_ack_bytes_detailed` encodes it; `PublishAck` gains a
  `detail` field.

- **Forged upstream tokens can no longer make the control plane hammer an
  IdP.** A token with an allowlisted issuer and an unknown `kid` made
  `/token/exchange` re-fetch that issuer's JWKS before any signature check, one
  fetch per request, with no credential needed. Each JWKS URL is now fetched at
  most once per 30 seconds, concurrent misses share the fetch in progress, and
  JWKS and discovery requests time out after 10 seconds. A key the IdP rotates
  in is accepted within 30 seconds of its first use.
- **An assignment change refreshes the node catalog.** A broker read
  `/v1/nodes` on a wake only when an assignment named a node it did not
  know. A broker that restarted comes back under the same id on new ports,
  so it was known and its old address kept: the first ship or forward to it
  after a move waited for the next catalog tick
  (`FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`). Every wake now reads the
  catalog, alongside reconcile rather than before it, and each read is
  bounded at 5 s so a control plane that never answers cannot hold the feed
  (`a_wake_refreshes_the_address_of_a_node_that_moved`, and in
  `felix-cluster`, `a_move_onto_a_restarted_broker_does_not_wait_for_the_catalog_tick`).
- **A leader ships to a restarted follower at its new address.** A
  follower's cursor kept the address it was created with for the whole
  generation, so a broker that restarted on new ports, still in the
  replica set, was dialled at its old one until the shard's generation
  changed. A move staged onto a broker that had just restarted never
  caught up and sat at `staged` until the move timeout. The cursor now
  takes each pass's address from the route, so the follower is reached
  within one catalog refresh (`FELIX_CONTROLPLANE_SYNC_INTERVAL_MS`)
  (`a_follower_that_moved_address_is_shipped_to_at_the_new_one`, and in
  `felix-cluster`, `a_restarted_broker_takes_shards_again`).

- **A broker that took a shard over replays all of it.** A follower's
  replay ring was filled when the first replicated batch opened the stream
  and was never touched again, so after a failover or a planned move a
  reader from the start got that first batch and then the live edge,
  skipping everything replicated in between. The records were on disk; a
  cursor-based subscribe had the same hole. The ring is now emptied as
  replicated records move the tail, and a reader is served from the log
  (`records_replicated_after_the_first_batch_replay_from_the_start`, and
  in `felix-cluster`, `a_handoff_that_times_out_loses_no_acknowledged_record`).
- **Client: out-of-order publish acks no longer break the stream.** The
  broker answers acked publishes as each completes, so on a publish stream
  carrying several at once a `Quorum` or forwarded publish can be answered
  after one sent behind it. The client required acks in request order and
  treated any other as a protocol error, failing every publish outstanding
  on that stream and reconnecting. Each answer now goes to the request it
  names (`acks_answered_out_of_order_reach_their_own_requests`).

- **A Postgres-backed control plane keeps a stream's replication factor.**
  Reading a stream back always reported a replication factor of 1, so a
  replicated stream was placed as leader-only, and any patch to the stream
  (retention, consistency, delivery) wrote the 1 back over the stored value.
  The store's Postgres tests, which would have caught it, never ran: `task
  test` did not enable their feature, and when enabled they skipped
  themselves, because resetting the schema failed on a foreign key and the
  tests sharing it deadlocked. They now run with the database `task test`
  starts, serially, and a setup failure fails them.
- **A `Quorum` shard is replaced when its leader dies mid-publish.** A leader
  reported a follower caught up only when it held the leader's whole log, so a
  publish landing between shipping and the report left every follower one
  record short and the report named nobody. A leader killed right then left
  the control plane no replica it may promote, and the shard stayed down for
  good while its producers were told `stream not found`. Under `Quorum` a
  follower now counts as caught up when it holds everything up to the offset a
  majority holds, which is every record that can have been acknowledged.
  `Leader` streams and moves still require an exact copy.

- **The move limits hold across control-plane instances.** Each placement
  write was conditional only on its own shard's generation, so two
  Postgres-backed instances, or a pass and an operator's request on another
  instance, could each read the last free slot and start moves on two
  different shards. One instance now runs the timed placement passes, the
  holder of a lease in the store that lasts three reconcile intervals, is
  renewed every pass and is released on shutdown; under Raft the leader
  holds it. Every placement write, operator steps included, is also fenced
  by a placement token read before the pass or request decided: a write
  lands only if no other placement write landed since, and a new holder
  advances the token, so an instance that paused past its lease writes
  nothing after a takeover. Passes woken by a report still run where the
  report arrived. Postgres gains migration `0016_placement_lease.sql`; the
  Raft log gains `put_shard_assignment_fenced` and `take_placement_lease`
  commands, which a follower running an older build refuses, so upgrade
  every member before the leader. `ControlPlaneStore::put_shard_assignment_if`
  takes the token. Metrics: `felix_placement_lease_held`,
  `felix_placement_lease_takeovers_total`,
  `felix_placement_writes_fenced_total`.

- **A redirect to a draining broker carries its address.** A `not_leader`
  answer or publish-ack owner hint naming a broker that is being drained, but
  still leads the shard, omitted the client address because only brokers
  eligible for new work were listed. Routable brokers' addresses are now used
  for redirects; `topology` still lists only eligible brokers.
- **One refused publish failed later publishes on the same connection.** A
  publish the broker refused (an unknown stream, a forbidden one, overload)
  stopped the client's publish worker that sent it, and every later publish
  routed to that worker, to any stream, got the same refusal back without a
  code. `ClusterClient` then took the uncoded error for a dead connection and
  reconnected. A refusal now answers only the publish it belongs to; a broken
  or timed-out ack stream still ends the worker.

- **A long replay lost records and could look stalled.** A subscription
  resumed from an early offset dropped history even when the application read
  every event promptly: the broker writes history as fast as it reads it, and
  the client drained it into its bounded queues under the default `drop_new`
  policy. A replay of 5,000 records typically delivered the first 256 and a
  scattered few after; when the tail was among the drops, the subscription
  went quiet until the next live publish. History below the subscription's
  `live_offset` now waits for room in the client's queues whatever the policy,
  so a replay is paced by its reader and never dropped. Live records past
  `live_offset` keep the configured policy.

- **A subscription's last events could be dropped when it ended.** The
  subscriber's connection writer dropped deliveries queued in the same batch
  as its unregister, and the feeder forgot the subscriber's connection before
  deliveries still in its lane queue were routed. Both now deliver them first,
  so a subscriber ended by a shard move receives everything the broker
  committed.

- **A `Leader` publish acknowledged on enqueue could be dropped without a
  trace.** With `ack_on_commit` off (the default) the ack goes out when the
  publish is queued; if the broker's lease lapsed before the worker wrote it,
  the worker refused the write and the refusal went nowhere. The refusal is
  right (another broker may lead the shard), so admission now reads the lease
  clock for such a publish and, with less left than the publish queue wait plus
  the ack wait (capped at half the lease), waits for the write, so a lapse
  reaches the client as `shard_unavailable`. A pause that starts after the ack
  can still strand one; it is counted in the new
  `felix_broker_acked_publishes_dropped_total{reason}` and logged at warn. The
  docs no longer claim a `Leader` ack means the record is durable under the
  default. (#671)

- A `Quorum` stream placed with one replica refused every publish on a
  cluster with `leadership_lost`: nothing ships for such a shard, so no quorum
  mark was ever published. The leader alone is its majority now.

- **An acknowledged write could be lost in a planned shard move.** Admission
  checked that the broker served the shard, but an admitted publish could wait
  in the publish queue and claim its offsets after the old leader had reported
  `drained` and the control plane had cut over; it was then committed and
  acknowledged on a broker the new owner never copied it from. The drained
  report guessed at this with the tail holding still for two passes, which a
  deep enough queue outlasts. The same gap existed for cache puts and deletes,
  counter adds, consumer-group polls, acks, nacks and dead-letter changes, and
  publishes forwarded to the old leader.

  Every such write now enters a per-shard write fence right before it claims
  its place in the log and holds it until it is durable and fanned out. The
  shard lifecycle closes the fence as soon as it sees a move, and a write that
  reaches the fence afterwards is refused the way a write to a shard the broker
  does not serve is. The drained report goes out only once the fence is closed
  with nothing inside it, which makes it exact. The readers a moved shard's old
  leader ends now receive every record it committed first. The TLA+ model splits
  admission from the claim, and `FelixShardHandoffNoClaimFence.cfg` shows the
  loss without the check.
- **A shard moved off a broker that stays up left its subscriptions and cache
  watches open and silent.** The old leader now ends them, after delivering
  what was queued, whenever it stops serving a shard.
- **A planned shard move could lose a dead letter or a counter add.** The old
  leader reported `drained` on the shard's own log and shipped its consumer
  groups' cursors and dead letters, and its cache's counters, only afterwards,
  logging a failure and moving on. A dead letter the new owner lacked was a
  record its group silently skipped, and a counter add it lacked was an
  acknowledged add gone from the sum. The drained pass now ships those logs
  first and reports `drained` only once the move's successor holds them; any
  other replica still missing them is left out of the report's caught-up
  list rather than holding the move. A move that lost its successor waits for
  every replica level on the shard's log. A move held on them stays
  `Draining`, and `felix_broker_replication_drain_withheld_total{log}` and a
  warning naming the shard and follower say why. Brokers now read the
  assignment's `successor`.

### Fixed

- A broker shutting down in the middle of a credential refresh could exit after
  the control plane had rotated its refresh token but before writing the
  replacement, so the next start presented a spent token and the chain was
  revoked. Shutdown now waits, within the drain deadline, for a refresh in
  flight to finish.


## [0.6.0-preview] - 2026-09-20

Development towards 0.6.0. Not a release: published from this line only if and
when something needs to be, and `pip` will not install it without `--pre`.

**A note on the version string.** Cargo and npm carry `0.6.0-preview`
verbatim. PEP 440 normalises it to `0.6.0rc0` (`preview` is one of its
spellings of `rc`), so the wheel's version differs from the crate's and the
npm package's on purpose. It sorts before `0.6.0` on all three.

### Fixed

- **Two control-plane instances could undo each other's shard moves.** Every
  instance over Postgres runs placement, and each wrote what it planned
  unconditionally. An instance that planned a fence, then stalled, could write
  it after another instance had already cut over, handing the shard back to
  the old leader after the new one may have acknowledged writes the old one
  never saw; two instances could likewise promote different followers after
  one failure. Placement now writes every placement, promotion and move step
  only if the shard is still at the generation it planned from, through a new
  `ControlPlaneStore::put_shard_assignment_if` (a new `PutShardAssignmentIf`
  Raft command). A write that finds the shard changed is skipped, counted in
  `felix_shard_assignment_write_conflicts_total`, and re-planned on the next
  pass. `docs/formal/FelixShardStalePlanner.cfg` is the race without the
  check, and TLC finds two brokers serving the shard.

- **The docs said the clients were not installable.** They are: `felix-client`
  is on crates.io, PyPI and npm, the same name on all three. The client pages
  still told readers to point `pip` at a release asset, to build the Node addon
  from a checkout, and to depend on `felix-client = "0.1"`, a version that was
  never published.

  Now `cargo add felix-client`, `pip install felix-client`, `npm install
  felix-client`, with the platform coverage each one actually has. The status
  table's client row said TypeScript "is not published to npm", which mattered
  more than the rest: that table is the page every other page defers to.

  The binding READMEs gained an install section too. They are what npm and PyPI
  render on the package page, and neither said how to install the package it
  was describing.

### Fixed

- **`npm stage publish` cannot claim a name either**, so the staging path added
  for that purpose does not work for a package that has never been published:

  ```
  POST /-/stage/package/felix-client-darwin-arm64
  404 Package "felix-client-darwin-arm64" not found
  ```

  Staging uploads a new *version* of an existing package. npm's documentation
  requires the package to exist before a trusted publisher can be configured
  too ([npm/cli#8544](https://github.com/npm/cli/issues/8544) tracks lifting
  that), which leaves no way for CI to create a name at all: the remaining
  option is a direct publish. That needs either a token that bypasses 2FA
  (restricted, and losing publish rights around January 2027) or a person
  answering the prompt.

  `scripts/npm_first_publish.sh` is that person's script. It publishes the
  binaries from a GitHub release, so what reaches npm is what CI built and what
  the conformance suite ran against, checks the tag against the manifest before
  it starts, and confirms all six are live afterwards. Run once per package
  name, ever; trusted publishing takes over from the second release.

  `npm_stage` stays, correctly described: it stages a version of a package that
  already exists.

- **A publish could have gone to a corporate mirror.** `npm publish` uses
  whatever registry is configured, and a mirror in `~/.npmrc` is a normal thing
  for a machine to have. This one had one, and the publish went to it and
  stopped only because it demanded credentials. Every npm command in the script
  now pins `--registry` explicitly, and `crates/felix-typescript` and each
  platform package carry an `.npmrc` naming the public registry, the way
  `docs-site` already did for resolution.

  It falls back to plain HTTPS when `gh` is absent, so it runs in Azure Cloud
  Shell and other minimal environments. Publishing is likely to happen there,
  since a corporate network cannot reach npm at all. The release is
  public, so there is nothing to authenticate to.

  The script also checks, before it downloads or packs anything, that the
  public registry is reachable and that you are logged in to *it* rather than
  to a mirror. A corporate network usually cannot reach npm at all, and finding
  that out at the upload wastes the run and leaves binaries lying around.

### Changed

- **npm publishes through trusted publishing instead of a token.** The job
  exchanges the workflow's OIDC identity for a short-lived credential, the way
  the PyPI job already did, so there is no `NPM_TOKEN` to store, rotate or
  leak. npm's own guidance is to prefer this over an automation token, and the
  alternative was a token configured to bypass 2FA: a standing credential with
  publish rights, held in CI and exempt from the control meant to protect it.

  Two details the failure mode hides: the job needs `id-token: write`, and Node
  22 ships an npm too old to know about OIDC, so the job upgrades npm first.
  Without either, a publish falls back to looking for a token and fails as
  though none were configured.

  Each of the six packages needs a trusted publisher configured on npm that
  names this repository, this workflow and the `npm` environment.

- **`napi prepublish` is no longer how the packages are published.** What it
  still did for this repository was sync the platform versions, which are
  committed and asserted by `check_npm_packages.py`, and upload the binaries to
  the GitHub release, which the release-assets job already did. What it did
  besides was swallow "this package has no binary" and exit 0. That is how a
  publish job went green having uploaded nothing.

  It is an explicit `npm publish` per package now, platform packages before the
  one that declares them as optional dependencies, and any failure stops the
  run.

- **The release can stage npm packages instead of publishing them.** A
  `npm_stage` input on the manual dispatch runs `npm stage publish`, which a
  stage-only token can do and which leaves each version for a maintainer with
  2FA to promote. It exists to claim a name for the first time: trusted
  publishing is configured on a package, and a package that has never been
  published is not there to configure.

  A trusted publisher can only be configured on a package that already exists,
  so the first release of a new name cannot use one, and direct publishing
  with a token that bypasses 2FA is deprecated and removed in January 2027.
  That leaves staging, which is also the better design: a person with 2FA
  confirms the one irreversible act, claiming a permanent name. The token is
  passed only in staging mode. Trusted publishing is the normal path, and a
  token sitting alongside it would be a second way in that nobody meant to
  leave open.

  The workflow records the one-time sequence, since nobody will remember it
  by the next release: stage, promote, configure the
  publishers now that the packages exist, delete the token.

  In staging mode the job prints what to promote and in what order, and does
  not assert the versions are live, because a staged version deliberately is
  not.

### Fixed

- **The npm job published nothing and reported success.** `napi prepublish`
  publishes the platform packages, but it does not assemble them: the binaries
  were downloaded beside the manifest, so every `npm/<triple>/` directory was
  empty, and each one was skipped with `[...felix.darwin-arm64.node] doesn't
  exist` on stdout, which is not treated as a failure. The job went green having uploaded
  nothing.

  Three steps now, because it is three things: `napi artifacts` moves each
  binary into the platform package that carries it, `napi prepublish` publishes
  those, and `npm publish` sends the JavaScript package that declares them as
  optional dependencies. The last step was missing outright, because
  `prepublish` never publishes the main package.

  The job now also asks the registry whether each of the six is really there,
  and fails if any is not. Without that check this step reported success
  twice after publishing nothing.

- **The PyPI step no longer fails on a rerun.** PyPI refuses a duplicate file
  instead of ignoring it, so once 0.5.0 was up, every later run failed on it.
  Later runs are the norm here, because the three registries finish at
  different rates and whichever run completes the slowest one will always find
  PyPI already done. `skip-existing` makes it idempotent.

### Fixed

- **The npm publish step passed an option the pinned CLI does not have.**
  `napi prepublish -t npm --access public` is a napi 3 spelling; the CLI is
  pinned to 2, to match the napi crate, and it refused the flag before
  uploading anything. The flag was never needed once the packages went
  unscoped. npm publishes an unscoped package publicly by default, and a
  scoped one takes `publishConfig.access` in its manifest instead of a flag
  on the command.

- **The crates.io job now waits out the new-crate rate limit** instead of
  failing the release. crates.io allows a short burst of *new* crate names and
  then roughly one per ten minutes: publishing this workspace for the first
  time got five up and was refused on the sixth with a `429` naming the time it
  would accept the next.

  That is a queue, and it only applies to names that have never been
  published. Later versions of an existing crate are not limited this way. The job reads the time from the response, waits, and retries, and
  gives up immediately on any failure that is not a rate limit. The existing
  skip-what-is-already-published behaviour means a resumed run picks up where
  the last one stopped instead of erroring on what already landed.

### Fixed

- **The Node addon's cross-compile leg could not build.** The 0.5.0 release
  pipeline failed on `x86_64-apple-darwin` with `can't find crate for core`,
  which took the npm assets with it.

  The job asked for `dtolnay/rust-toolchain@stable` while every other Rust job
  in the file pins `1.97.1`. The action added the target to *stable*, and then
  `rust-toolchain.toml` switched cargo to the pinned toolchain, which did not
  have it. Four of the five legs passed anyway, because their target is the
  runner's own and was already installed. Only the leg that really
  cross-compiles, x86-64 on an arm64 macOS runner, had anything to notice.

  `task ci:toolchains` now asserts every workflow's Rust setup asks for the
  pinned channel, with an explicit `toolchain-exempt:` marker for the fuzz job,
  which needs nightly for `cargo-fuzz` and says so.

## [0.5.0] - 2026-09-19

A third client, and the release where the data path stopped paying for
compatibility.

**A Node.js / TypeScript client**, a napi-rs addon over the same Rust client
the Python binding wraps, passing every required scenario in the conformance
catalogue with CI and the release pipeline both gated on it.

**The data path is binary.** A routing key now rides in the binary publish
frame, so a keyed publish no longer falls back to JSON (which measured 645.8
MB/s against 917 for the same workload), and the JSON publish surface is deprecated
for removal in 0.6.0. A forwarded publish now says so on its ack and names the
shard's owner, which makes the cost of publishing to the wrong broker visible
for the first time.

**Two defaults changed on measurement.** MTU discovery is bounded below Linux's
UDP GSO ceiling, where the old bound could stall delivery outright on a
jumbo-frame network; and a broker joining a cluster refuses to start with a
credential nothing can renew, instead of running fine until the token expires
and the lease lapses under it.

**Upgrade notes.** A deployment passing an expiring `FELIX_NODE_TOKEN` by value
must now set `FELIX_NODE_REFRESH_TOKEN_FILE` or `FELIX_NODE_TOKEN_FILE`. Two
docs URLs moved, listed below.

**Wire protocol:** two new frame flags, `FLAG_BINARY_PUBLISH_KEYED` (`0x0040`)
and `FLAG_BINARY_PUBLISH_ACK_OWNER` (`0x0080`). `VERSION` remains `1`,
`INTERNAL_VERSION` remains `1`, and no feature bit is added. Both are negotiated
on the handshake, so a client and broker that disagree about either exchange the
same bytes they did before.

### Added

- **A forwarded publish says so on its ack, and names the shard's owner**
  (#536). A publish for a shard the receiving broker does not own is forwarded
  to the owner and acknowledged once the owner has written it. That is correct,
  but it was invisible, so a client kept publishing to the same entry broker
  forever while every record was decrypted, re-encrypted and decrypted again on
  the way. A perf session put the cost at roughly half the throughput per core:
  **~250 MB/s per busy vCPU direct against ~140 forwarded**.

  `FLAG_BINARY_PUBLISH_ACK_OWNER` (`0x0080`) is a modifier on the publish ack,
  the shape `FLAG_BINARY_PUBLISH_KEYED` established. The bit's presence is the
  signal that forwarding happened; the payload carries the owner's `node_id`,
  the address it serves *clients* on, and the ownership generation.

  It is only a hint. The publish already succeeded, so a client that ignores
  it is exactly as correct as before, and just as slow. That makes it safe to
  add, since nothing depends on a client acting on it. It is only set for a
  client that advertised the bit, and an ack with no owner is byte-identical to one
  from before the bit existed.

  Clients count it as `felix_client_publish_forwarded_total`, labelled by owner,
  and `felix_client::publishes_forwarded()` exposes the same number without the
  telemetry feature, so a build compiled without measurement can still tell
  whether it is paying for forwarding.

  **Routing on the hint is not built yet.** Caching shard → owner and sending
  the next batch straight there needs a connection per owner and a client-side
  `shard_for(key)`, neither of which exists. That is a separate change, and
  this one makes its benefit measurable.


- **A routing key rides in the binary publish frame** (#549). A keyed publish
  used to force the JSON encoding, because the binary layouts were fixed and had
  nowhere to put a key. That fallback measured **645.8 MB/s against 917
  MB/s** unkeyed on the same rig, with user CPU up from 20% to 28%. Routing a
  record cost roughly 30% of throughput, so sharding carried a real cost.

  `0x0040` is a modifier on `0x0001`, the shape `FLAG_BINARY_PUBLISH_ACKED`
  already established: the body is prefixed with a `u16` key length and the key
  bytes. With both bits set the correlation prefix still comes first, so a
  broker reads `request_id` at offset 0 whether or not a key follows.

  A broker that predates the bit would read `key_len` as `tenant_len`, so the
  client sends the keyed binary frame only to a broker that advertised it and
  uses JSON otherwise. The fallback costs throughput and never correctness.

- **A Node.js / TypeScript client** (`crates/felix-typescript`), a napi-rs
  addon over `felix-client` instead of a reimplementation of the protocol.
  The reasoning matches the Python binding, and so does the surface: publish
  (keyed, or at-least-once), subscribe, sharded subscribe with per-shard
  resume, cache get/put/delete, counters, cache watches and consumer groups.
  Every call returns a `Promise`, and every handle is disposable.
  Errors arrive as typed classes (`ConnectionError`, `AuthError`,
  `NotFoundError`, `CursorError`, `InvalidArgumentError`) that mirror the
  Python binding's exceptions, so an application branches on the error class
  instead of the message text.

  It passes the client conformance suite, and CI and the release pipeline are
  both gated on that: the suite drives a real three-node fixture cluster,
  names the catalogue scenario each test demonstrates, and
  `felix-conformance verify` checks the results against the catalogue, so a
  green run that quietly skipped a required scenario still fails. Not
  published to npm: the addon is built per platform and attached to the GitHub
  release, with the npm step behind `PUBLISH_NPM` for the same reason PyPI is
  behind `PUBLISH_PYPI`.

- **`task release:check`**, and the release refuses a tag that disagrees with
  the tree. A tag and a version that disagree ship artifacts labelled with
  neither, and nothing else notices: the archive is named from the tag, each
  crate is built from its own manifest, and both succeed. The wheel, the npm
  package and the Helm chart each carry a version of their own, so bumping the
  workspace was never enough. CI asserts the fields agree with each other, which
  is the half a pull request can be wrong about; the release job additionally
  asserts they match the tag, before anything is published.

- **`task lock:refresh`**, and CI fails when a build updates a lockfile the
  commit did not include. Six crates declare their own `[workspace]` (the four
  under `demos/`, plus the Python and TypeScript bindings), so the repository
  workspace never touches their `Cargo.lock`. All four demo locks had sat at
  `0.4.0-preview` through two releases and had never heard of `io-uring`,
  because CI regenerated them on every run and nothing looked at what changed.

- **`felix-loadgen --keys <n>`** spreads the ingest scenario's batches over `n`
  routing keys. The scenario published unkeyed, and an unkeyed record resolves
  to shard 0, so every "multi-shard" measurement taken with it was really a
  single-shard one. A 12-shard and a 1-shard stream measured identically
  (920 vs 923 MB/s) because both exercised the same log. Default `0` keeps the
  old behaviour, so existing runs stay comparable.


- **A crates.io publish job** (#574), behind `PUBLISH_CRATES` like the PyPI and
  npm ones. The Rust client is what the other two bindings wrap, and it was the
  one registry with no job at all.

  The order is the mechanism: crates.io resolves each dependency against the
  registry as the crate uploads, so a crate published ahead of something it
  depends on fails *halfway*, with the earlier crates already permanent. The
  job walks a topological sort of the workspace's internal edges and skips any
  version already on the registry, so a re-run after a partial failure
  completes rather than erroring on what already landed. `task publish:check`
  asserts that list is every publishable crate and is really topological,
  because nothing about adding a crate to the workspace forces anyone to
  revisit a workflow file.

  `felix-broker` and `felix-storage` are published too, AGPL-3.0 and all:
  `felix-client`'s `in-process` feature declares them optional, and crates.io
  resolves an optional dependency like any other: `cargo publish --dry-run`
  refuses `felix-client` without them. The licence split is unchanged; a
  default `felix-client` build still pulls no AGPL-3.0 code.


- **The npm publish can run** (#575). napi ships one package per
  platform (the main package declares them as optional dependencies and npm
  installs the matching one), and none of that existed: no `npm/` directory, no
  `optionalDependencies`, and a loader that only looked for a file beside
  itself. `napi prepublish` had nothing to publish.

  Five platform packages now cover exactly the five targets the release builds,
  and the loader resolves the installed one, detecting musl rather than
  assuming glibc. A checkout is unaffected: a local build still wins, which is
  what the conformance suite runs against. `check_npm_packages.py` ties the
  build matrix, the declared triples and the packages on disk together, because
  drift between them publishes cleanly and fails at `npm install` on someone
  else's machine, against a version that is permanent. The napi CLI is pinned
  to `@2`, matching the napi crate, whose v3 renamed the config keys this
  package uses.

  Registry metadata went with it: neither binding shipped a `LICENSE` despite
  both declaring Apache-2.0, the Python package had no `py.typed` so type
  checkers ignored the stubs beside it, and every crate inherited the workspace
  readme, so the repository README, roadmap and all, rendered on eight library
  pages. Each publishable crate has its own now.

### Fixed

- **The Quickstart's first command did not work**, and neither did the one it
  became. `cargo run --release -p broker` asked which of nine binaries to run,
  because eight of them are demos and nothing set `default-run`. Corrected to
  `--bin felix-broker`, it starts and then exits: `FELIX_CONTROLPLANE_URL must
  be set for auth`. The page claimed you would see `QUIC listening on
  0.0.0.0:5000` and that "the broker is now ready to accept connections", and
  printed that command three times.

  `default-run = "felix-broker"` fixes the first half. The second half is
  intended. A broker validates client tokens against keys it fetches from the
  control plane and registers itself there for shard placement, so there is no
  unauthenticated mode. The docs had never said so.

  The Quickstart now opens with `felix-cluster up`, which starts a control
  plane, mints the credentials and brings up three brokers, then publishes
  through a non-owner and receives from the owner. That command existed the
  whole time and no getting-started page mentioned it. Every command and every
  block of output on the page was run to produce it.

  Also corrected: the landing page and Installation both told you to run the
  broker alone; the container instructions did the same with `docker run`; and
  Troubleshooting told you to set `FELIX_METRICS_BIND`, which nothing reads --
  the broker warns about that exact name, and the variable is
  `FELIX_BROKER_METRICS_BIND`.


- **Four timing-sensitive tests stopped reporting a slow machine as a broken
  invariant.** Each was a wall-clock assertion on a shared runner, each failed
  on a branch that could not have caused it, and between them they cost several
  investigations.

  `concurrent_durable_appends_share_a_flush` asserted a **wall-clock speedup**
  from group commit. A ratio cannot tell "the flushes coalesced" from "this
  machine could not put sixteen appends in flight for them to", which is how it
  read 0.70x on a two-core runner with nothing wrong. `DiskLog::flushes()` now
  exposes the flush count (one relaxed increment against an `fsync`), and the
  test asserts the thing itself: 64 appends produce 64 flushes serially and 5
  concurrently. Counting does not measure the machine.

  `an_inline_rollover_does_not_park_every_worker` allowed a fifth of one
  rollover for scheduling noise, described as "far above the scheduling noise
  even on a loaded box". CI falsified that twice, measuring 137 ms. The bug it
  catches parks every worker for the *whole* rollover, so half keeps a 2x margin
  on both sides instead of sitting next to the noise floor.

  `FELIX_TEST_TIMEOUT_SCALE` multiplies the setup deadlines in the cluster
  harness and the Raft chaos suite. Unset means 1, so a developer's run is
  unchanged and still fails fast on a genuine hang; CI sets `3` and the coverage
  job `5`. Raising the constants instead would have bought the same green while
  never noticing a hang on the machines fast enough to.

  Deliberately untouched: `losing_quorum_fails_writes_loudly_not_silently`
  waits on a leader noticing it has lost quorum, and that wait *is* the subject.
  Widening it would only make the test slower. It is
  serialised instead, which is what its module already says.

- **A cancelled idempotent publish no longer loses records silently.**
  `IdempotentProducer::publish_batch` advanced its sequence only after the
  broker answered. Dropping the future in between (a `timeout`, a losing
  `select!` branch) left the cursor pointing at a sequence the batch may
  already have been appended under. The next batch then went out under that
  spent number, and the broker's contract is to answer a remembered sequence
  *from memory without appending it*: the caller was told `Ok` and its records
  were discarded, with nothing reported anywhere.

  The producer now notices the cancellation and refuses to publish again,
  naming the reason and telling the caller to take a fresh producer id. The
  batch in doubt is the only one whose fate is unknown, and that is recoverable;
  silently dropping the ones after it was not. Covered by a cluster test that
  cancels a real publish and fails without the fix.

  Only reachable from Rust: neither the Python nor the TypeScript binding wraps
  idempotent producers.

### Changed

- **The Node package is `felix-client`, unscoped.** `@felix` on npm was already
  taken (there is an unscoped `felix` package, and the scope with it), so the
  six packages the release publishes are `felix-client` and one per platform,
  `felix-client-darwin-arm64` and friends. All six names were confirmed free.

  Unscoped rather than hunting for another scope: it is the same name the crate
  has on crates.io and the wheel has on PyPI, so one string covers every
  install instruction, and an unscoped name is first-come rather than colliding
  with a namespace someone else may hold. `publishConfig.access` went with the
  scope, since only a scoped package defaults to restricted.

  Nothing had been published, so this costs nothing but the rename.


- **The loopback MTU guarantee's buffer gate no longer moves with the MTU
  knobs.** It checks whether the host was tuned, as a proxy, because
  Linux clamps `SO_RCVBUF` to a stock ~208 KB and an untuned host cannot absorb
  the bursts the pin exists to survive. It was measured against the size about
  to be pinned, so when the discovery bound's default dropped to 4096 below the
  requirement fell from ~1 MiB of socket buffer to 256 KiB and **every stock
  Linux host silently started pinning the loopback MTU**. A routed-path hazard
  fix had no business changing who gets a loopback pin. The gate now reads the
  jumbo payload and nothing configurable.

  One behaviour goes with it: setting `FELIX_MTU_UPPER_BOUND` low on an
  *untuned* host no longer buys the guarantee. Asking for a smaller pin is not
  evidence of headroom.

- **MTU discovery is bounded below Linux's UDP GSO ceiling by default**
  (`4096`, was `16384`; macOS keeps `16384`). Linux packs a whole `sendmsg`
  batch into one IP datagram, so `MTU × segments` must stay under 65,535, and
  quinn batches up to 10, which puts the real ceiling at **6,553 bytes**. Above it
  the kernel rejects every batch with `EMSGSIZE`, which quinn does not recognise
  as a GSO failure (it falls back only on `EIO`/`EINVAL`), so the transmit is
  dropped *after* quinn has counted it as sent and delivery stalls permanently
  rather than degrading.

  Loopback was capped at 4096 when this was diagnosed; a **routed** path was
  not. That made the old default harmless on a 1500-byte network and fatal on a
  jumbo-frame one, and jumbo frames are what you buy for throughput. Every perf
  session set `FELIX_MTU_UPPER_BOUND=4096` by hand; that is now the default.

  `4096` rather than the exact `6553`: quinn's `MAX_TRANSMIT_SEGMENTS` is
  private to it, so the ceiling cannot be derived through its API, and 6,553
  breaks the moment that number rises. Two tests pin the invariant, and they
  check the non-macOS value from either host, so the check cannot quietly pass
  just because of the machine doing the editing.

- **A broker says so when the OS clamps its UDP socket buffers.** Linux accepts
  an oversized `SO_RCVBUF`/`SO_SNDBUF` and silently clamps it to
  `net.core.rmem_max`/`wmem_max`, which ship at around 208 KB against the 8 MiB
  Felix asks for. Bursts then overflow the socket and surface as QUIC
  retransmits, so the broker runs at a fraction of the host's capacity and looks
  healthy doing it. Nothing in Felix can raise a host limit, so the warning
  names the sysctls instead.

  **Deliberately unchanged: `publish_conn_pool` (4) and `publish_sharding`
  (`HashStream`).** Both were swept in an Azure session and both looked
  promising, but those runs were void. The generator was ignoring client
  environment config (#553), so the overrides never applied and the runs
  measured the defaults. Re-tested on a fixed generator, round-robin landed at
  912.6 MB/s, inside the 842–926 band every valid configuration occupied, and
  raising the broker's publish worker pool 4 → 16 measured slightly worse. The
  reasoning is now recorded next to each default so it is not re-litigated from
  the retracted numbers.


- **A Clients section in the docs, with a page per client.** Rust had its own
  page and everything else shared one, where TypeScript got two paragraphs.
  **Two URLs moved:** `/api/client-sdk/` is now `/clients/rust/`, and
  `/api/clients/` is now `/clients/overview/`. The Rust page also lost an error
  handling example that could never have compiled. It matched on
  `felix_common::Error` variants that do not exist, in a crate that is not one
  of `felix-client`'s dependencies.

- **The container images are published.** `ghcr.io/gabloe/felix-broker` and
  `ghcr.io/gabloe/felix-controlplane` went out with 0.4.1 and pull without
  credentials. Each release tags the full version, the minor series, and
  `latest` on non-prereleases.

  The docs said they were not published and told you to build your own; that
  was true when written and is not now. The Docker Compose, Kubernetes and
  installation pages now pull instead, and keep the build commands for running
  something unreleased.

  Images are signed with cosign, keyless, **over the digest and never the tag**,
  because a tag can be moved and a signature over one would follow it. The Kubernetes
  page carries the `cosign verify` invocation. The chart's image tag defaults to
  its `appVersion`, so a default install resolves to a published image with
  nothing to configure.


- **The data path is binary; JSON is compatibility only** (#550). Now that
  #549 put the routing key in the binary publish frame, the JSON encoding has
  no remaining reason to carry data-plane traffic. It measured **645.8 MB/s
  against 917** for the same keyed workload on the same rig, with user CPU up
  from 20% to 28%, and it buys nothing the binary frames do not now cover.

  `Publisher::publish_json` and `Publisher::publish_batch_json` are
  **deprecated** and will be removed in 0.6.0. Nothing routes through them any
  more: `publish_batch`'s fallback now calls the private keyed form directly, so
  it keeps working once the public surface goes.

  **The broker still accepts JSON publishes and will keep accepting them.**
  `ORIGINAL_V1_FLAGS` is frozen, so a client older than `FLAG_BINARY_PUBLISH_ACKED`
  or `FLAG_BINARY_PUBLISH_KEYED` is entitled to send them forever. What changed is
  that no current Felix client emits one except as a fallback it chooses itself,
  against a broker that did not advertise the frame it wanted.
  `felix_broker_json_publishes_total{frame="publish"|"publish_batch"}` counts what
  still arrives that way, which is the evidence a deployment would need before
  the arm could ever be dropped.

  `publish_idempotent` is unaffected. It is JSON because no binary layout carries
  a producer id and sequence yet, not for compatibility.
- **A broker refuses to start when its credential will expire with nothing able
  to renew it.** The broker's control-plane calls all read one token, and the
  heartbeat is among them, and the heartbeat *is* the lease renewal. A broker
  with an expired credential stops serving the shards it leads once the lease
  lapses. That is correct behaviour, and still an outage nobody chose.

  Joining a cluster (`FELIX_NODE_ID` set) with a token that carries an `exp` and
  neither `FELIX_NODE_REFRESH_TOKEN_FILE` nor `FELIX_NODE_TOKEN_FILE` now fails
  startup, naming both ways out. Previously it logged at `info` and ran fine
  until the token died. **Upgrade note:** a deployment passing an expiring
  `FELIX_NODE_TOKEN` by value must set one of the two files.

- **`FELIX_NODE_TOKEN_FILE` is re-read**, so a credential rotated by something
  outside the broker (a Vault agent, SPIRE, a sidecar) takes effect without a
  restart. It was read once at startup and never again, while the *refresh*
  token file was deliberately re-read every time; the asymmetry was the
  surprising half. A replacement that has already expired is declined rather
  than adopted, because swapping a working credential for a dead one turns
  someone else's rotation bug into this broker's outage.


- **Device flushes can go through `io_uring` on Linux** (#548), behind
  `FELIX_STORAGE_IO_URING=1`, default off. `IORING_OP_FSYNC` removes the
  `spawn_blocking` hand-off rather than shrinking it, and unlike running the
  sync inline it keeps the `await` as a yield point, so background rollover and
  retention still get scheduled. A perf session measured **956.7 MB/s against
  917.2** with it on. Every run was better, with no overlap between the
  distributions.

  The blocking pool stays as the fallback: a kernel too old for the opcode, or a
  container that forbids the syscall, falls back rather than failing. Durability
  must not depend on an optimisation being available.


- **Docs stopped calling shipped capabilities unbuilt.** The README, the docs
  site landing page, the overview, why-felix, the FAQ, the components and
  project-structure pages, `docs/architecture.md`, `docs/auth.md` and
  `docs/internal-protocol.md` all still listed broker-to-broker mTLS as future
  work; it shipped in M8 (#125, #126). Alongside it: `docs/architecture.md`
  said no metadata rides the control plane's Raft group, which M13 closed;
  `how-felix-works.md` named Raft and mTLS as unbuilt; the threat model called
  peer connection exhaustion unmitigated after #504 capped it, and credited
  #422 with closing forwarded-publish replay, which it does not:
  `ForwardPublish` carries no producer identity, so that case stands open.
  A wrong status marker on a *security* control misleads readers about what
  protects them.

## [0.4.1] - 2026-09-18

A throughput fix. Durable publishes to one shard were processed strictly one at
a time, so group commit (the mechanism that lets one device flush serve many
waiters) never had more than one waiter and every publish paid a full flush
alone. Nothing was lost or misordered, but the broker used only about half a
machine.

Also fixes the release job that shipped 0.4.0 with no Python wheels.

**No wire-protocol change.** `felix-wire` `VERSION` remains `1`,
`INTERNAL_VERSION` remains `1`, and no feature bit is added.

### Fixed

- **Concurrent publishes share a device flush again** (#535). Publishes for a
  shard queued to a single worker, and that worker awaited each one to
  completion, including the `fsync`, before taking the next. One publish was
  ever at the sync point, so the group-commit fan-in was **1 by construction**:

  ```
  1 flush / ~300 us = ~3,300 publishes/s per shard
  x 256 KB per batch = ~850 MB/s
  ```

  which is the per-broker ceiling measured on Azure NVMe, at ~50% CPU with half
  the cores idle, waiting on the disk instead of computing. It got *worse*
  with more publishers, who queued behind each other.

  The publish is now two phases. `claim_publish` consumes the offsets and
  reserves the commit turn; the transport calls it **serially, in arrival
  order**, so the order records land on disk is unchanged, and a client
  pipelining under `AckMode::None` keeps its send order.
  `complete_publish` awaits the flush and then appends and fans out under that
  turn, and is spawned, so several flushes overlap and group commit has
  something to coalesce. Measured fan-in on the transport-level test:
  **1.000 -> 9.5**.

  Ordering is unchanged and checked rather than assumed: `commit_order`'s unit
  tests, which are the deterministic proof that turns are granted in offset
  order regardless of arrival order, and
  `disk_order_cursor_order_and_delivery_order_agree_under_concurrency`.

- **The release job could never have built the Python wheels** (#534). It
  installed the binding with `maturin develop`, which requires an *active*
  virtualenv; `actions/setup-python` provides an interpreter and no venv. The
  failure took the wheel, sdist, attach and publish jobs with it, so 0.4.0
  shipped with no wheels and nothing on PyPI. Installation is now
  `pip install ./crates/felix-python`, which builds through maturin as the PEP
  517 backend. That is the same thing a user does.

  The job also ran *only* in `release.yml`. The Python client merged after
  v0.3.1 and every earlier release predates it, so its first execution in its
  life was the v0.4.0 tag build. It now runs in CI as well, on every change,
  using the same installation steps so the two cannot drift.

### Added

- **`FELIX_BROKER_PUB_FLUSH_CONCURRENCY`** (default `32`): how many durable
  publishes one publish worker may have awaiting their device flush at once. Offsets are
  still claimed serially, so this does not affect the order records land in; it
  decides how many flushes group commit gets to coalesce. `1` restores the
  0.4.0 behaviour of one flush at a time, which is also how the fix is tested:
  at `1` the fan-in is 1.000 and the regression test fails.

### Changed

- `FELIX_BROKER_PUB_WORKERS_PER_CONN` is documented for what it is. The pool is
  **process-wide**, built once before the accept loop, not per connection as
  the name and the old description both say. A stream-shard handle maps to one
  worker, so raising it spreads *different* shards across workers and cannot
  give one shard more than one. The name is misleading and a rename is tracked
  in #535.

### Performance notes

The per-broker figures published for 0.4.0 (**~977 MB/s at ~48% CPU**), and
the conclusion drawn from them that *"durable throughput scales by adding
brokers, not by adding cores per broker"*, describe this defect and not the
design. The measurements were accurate but the architectural inference was
wrong. Replacement numbers need a rig session against 0.4.1 and are deliberately
not guessed at here.

## [0.4.0] - 2026-09-18

The multi-node hardening release. 0.3.0 made a cache into something you can
subscribe to; 0.4.0 makes the cluster something you can authenticate, package,
and reason about formally. Every broker-to-broker link can now be mutually
authenticated, every control-plane endpoint requires a credential, the whole
deployment ships as a Helm chart, and a TLA+ model of the lease and promotion
protocol found a safety defect that reading had not.

Felix also stopped being a Rust-only project: there is a **Python client**, a
binding over the Rust one rather than a reimplementation, gated by a
conformance catalogue that the next language will have to pass too.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1` and
`INTERNAL_VERSION` remains `1`. The one new capability is a negotiated feature
bit, `FEATURE_IDEMPOTENT_PRODUCER`. A 0.3.x client and a 0.4.0 broker
interoperate byte-for-byte on everything they both know.

### Added

- A TLA+ model of one shard's lease, replication and promotion protocol, at
  `docs/formal/FelixShard.tla`, checked with TLC by `task tla:check` and in
  CI. It holds no-two-leaders, acknowledged-records-survive,
  acknowledged-records-agree and no-truncation-below-the-mark under clock
  drift, lost heartbeats, lost reports, and a broker paused between admitting
  a write and committing it. Three configurations are expected to fail and
  are held to it: one with no safety interval and one with the commit-time
  lease check removed, which show each is load-bearing, and one with
  promotion as the design writes it, which finds that a leader report older
  than the last acknowledgement can promote a replica missing an
  acknowledged `Quorum` record. Promotion by (last generation, length)
  passes, and is the rule to move to (#420).

- Idempotent producers (#422). A client asks the broker for a producer id
  (`producer_init`) and sends its batches as `publish_idempotent` with a
  per-shard sequence; the shard's leader appends the sequence it expects,
  answers a re-send of one it already holds with the original outcome and no
  second append, and refuses a gap, an unknown producer, or a sequence older
  than it remembers with a typed `publish_refused`. A batch for a shard led
  elsewhere is refused with the leader's address rather than forwarded.
  Negotiated as `FEATURE_IDEMPOTENT_PRODUCER`. `ClusterClient::idempotent_producer`
  and `Client::idempotent_producer` re-send under the same sequence after an
  ambiguous outcome, follow a not-leader refusal, and end on any other. The
  sequences live in the leader's memory: a new leader answers
  `unknown_producer`, so a batch in flight across a failover is reported rather
  than silently landed or dropped.

- A leader rebuilds a halted follower itself. A follower whose log diverged
  from the leader's, or that refused a bootstrap because it held records of
  its own, is told to discard its copy of the shard (`ReplicateRebuild`,
  kind 24) and shipped again from the leader's oldest surviving record. It
  happens under a policy: `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` caps
  how many followers a broker rebuilds at once (default `1`; `0` leaves every
  halt to an operator, as before) and `FELIX_REPLICATION_REBUILD_BYTES_PER_SEC`
  paces the transfer. A fenced halt is never rebuilt, and a follower that
  predates the message stays halted until it is upgraded (#424).
- Broker-to-broker mTLS (#125). With `FELIX_INTERNAL_TLS_CERT`,
  `FELIX_INTERNAL_TLS_KEY` and `FELIX_INTERNAL_TLS_CA` set (all three or
  none), every peer connection is mutually authenticated against the CA, and
  the certificate's DNS name is the broker's identity: a dialler verifies the
  listener's certificate against the node id it dials, and the listener checks
  the node id a peer claims in `Hello` against the certificate it presented. A
  peer with no certificate, an untrusted or expired one, or one issued to a
  different name is refused. The certificate and key are re-read every 30s,
  so a renewal is picked up by the next handshake without a restart and
  without dropping connections already up. Without the three variables the
  peer link is encrypted but unauthenticated, as before, and startup now
  warns. Node ids must be valid DNS names under mTLS. The cluster test
  harness runs every cluster test under mTLS.
- A Helm chart, at `deploy/helm/felix`, for the control plane and a broker
  cluster. The control plane is a Deployment over Postgres, rolling one
  instance at a time with none unavailable, or a StatefulSet under the Raft
  backend with a volume per member and the peers map derived from the
  release. Brokers are a StatefulSet whose pod name is the node id, with a
  volume each, the pod IP advertised to peers and the pod's DNS name to
  clients, the credential and Postgres URL taken from Secrets the operator
  creates, probes and a preStop-plus-drain grace period the chart derives,
  PodDisruptionBudgets that keep a replication-factor-three quorum, anti-
  affinity and zone spread, a NetworkPolicy that admits the internal port
  from brokers only, and optional peer mTLS issued per pod by cert-manager's
  CSI driver. Value combinations that are each fine alone and wrong together
  (an even Raft group, a budget wider than the replica count, a drain longer
  than its grace period, a backend without its store) refuse to render.
  `task chart:check` lints and renders it every way `ci/` describes and
  asserts those properties on the output; CI runs it (#131, #132).
- **A Python client** (#392, #393, #395), as a binding over the Rust client
  rather than a reimplementation, so the two cannot drift in behaviour. Both a
  synchronous and an asyncio surface, covering publish and subscribe, consumer
  groups, cache watches and multi-shard subscriptions. It is gated by a
  conformance catalogue (`scenarios.toml`) that every future client must pass:
  Python claims every required scenario and leaves two optional ones
  unclaimed: `at_least_once` does not carry a routing key, and a prefix watch over a multi-shard cache needs one watch per shard.
  Release wheels are built in CI.
- **Container images** for the broker and the control plane (#405), built and
  smoke-tested in CI, which is what the Helm chart above deploys.
- **Refresh tokens and `POST /token/refresh`** (#402). 0.3.1 raised the
  exchanged-token TTL as a stopgap because a broker read its credential once
  and held it forever; this is the real fix. A refresh rotates within a family,
  re-evaluates RBAC on every use (so a grant removed since the last exchange
  stops working without waiting for a re-exchange), and a replayed token
  revokes the whole chain. Brokers refresh their node credential instead of
  holding it for their lifetime (#404).
- **Configuration that refuses to be wrong quietly.** An unrecognised
  `FELIX_*` variable is reported as a typo instead of silently taking a default
  (#499), and settings that are each fine alone and wrong together are refused
  at startup (#509). `--print-config` prints the effective configuration with
  every credential redacted, so it can be pasted into an issue (#500).
- **Day-0 bootstrap is audited** (#506), with its scope containment pinned by
  test. It logged nothing at all before.
- **A broker names the replicas replication has stopped for** (#485), so a
  halted follower is visible instead of inferred from lag.
- **Segments record where each leadership generation began** (#466), which is
  what lets a follower resume at a generation boundary and a leader drop a
  divergent suffix.
- **An unknown internal frame kind is refused rather than fatal** (#496). A
  newer peer sending a kind an older one does not know no longer ends its
  control loop.
- `--slow-subscribers` in the load generator (#382), for measuring
  slow-consumer isolation rather than asserting it.
- The protocol decoders are fuzzed, with every target run in CI (#480).

### Performance

- **Replication stopped being a timer.** A pass ships on a durable append
  instead of waiting for the next tick (#457), ships every shard concurrently
  rather than one after another (#471), and advances the quorum mark at the
  **majority** instead of at the slowest follower (#478). A pass's replica
  reports now share one control-plane request (#494). Together these are what
  make a `Quorum` acknowledgement a measurement of replication rather than of a
  2s sweep.
- **The cache write path group-commits** like the stream path (#390).
- **A commit turn is released to one publisher rather than all of them**
  (#511), so a wake-up does not thunder.
- **Placement uses bounded-load rendezvous hashing** (#388), so shards spread
  evenly instead of piling onto whichever node scores highest.

### Fixed

- Replica reports (what a shard's leader says about which replicas hold its
  log) are now written to the control plane's store instead of kept in the
  memory of the instance that received them. With several control-plane
  instances over one Postgres, promotion could run on an instance that had
  never seen the report, so a `Quorum` acknowledgement released on it could
  not be made good at failover (#409). Under Raft the report is a log command,
  restamped with the leader's clock like a heartbeat. A report is stamped with
  the store's clock and judged against it, so freshness is no longer a
  subtraction between two hosts' clocks under Postgres; a deleted assignment
  now takes its report with it.
- **A `Quorum` acknowledgement could outrun what the replicas held.**
  A follower can no longer confirm past the batch it was sent (#427), the
  leader holds the quorum mark when a replica report did not land (#434), and
  a broker may only report positions for shards it leads (#430).
- **Clocks are no longer compared across hosts.** The lease is anchored at the
  heartbeat's *send*, not its response (#426); liveness expiry is judged by one
  clock rather than one per instance (#439); the leader stamps a heartbeat
  rather than whichever instance received it (#475); and a replica report is
  judged on the clock that stamped it (#484).
- **Storage durability gaps.** A failed fsync now poisons the segment writer
  rather than letting the next append proceed as if the disk had kept its word
  (#428); the compaction directory swap is crash-safe (#429); and a sparse
  index entry may not point inside the segment header (#483).
- **A follower resumes at the generation boundary rather than at zero** (#477),
  and a leader drops a divergent suffix left by a previous generation (#467).
- A subscriber id is never reused (#397).
- A forwarded publish is bounded by the ack budget (#399).
- An unknown stream reports zero shards rather than one (#400).
- A bootstrap whose ranges *meet* is accepted, rather than requiring an exact
  match (#432).
- Shard assignments cascade on delete in the in-memory store too (#398).
- `felix-common` builds on its own default features (#468).

### Security

- A forwarded publish or cache operation now carries the client's own bearer
  token, and the owning broker verifies it before writing: against the
  tenant's keys, for `stream.publish` on that stream or `cache.read` /
  `cache.write` on that cache. The owner used to re-check ownership and
  generation only, so anything that could reach the internal port could
  append to any tenant's stream with no credential (#503). The credential
  rides two new internal kinds, `AuthorizedForwardPublish` (22) and
  `AuthorizedForwardCacheOp` (23); the legacy kinds still decode and are
  refused `Unauthorized`. During a rolling upgrade, an upgraded broker falls
  back to the legacy kind toward an owner that predates it, while an old
  broker's forwards to an upgraded owner fail until it is upgraded. See the
  upgrade notes.
- The control-plane resource API now requires a Felix bearer token on every
  endpoint. Tenants were created, listed and deleted, and namespaces, streams
  and caches managed, with no credential at all, while `/v1/nodes` next to
  them returned `401`. Namespaces, streams and caches take `ns.manage`,
  `stream.manage` or `cache.manage` over the object from a token minted for
  that tenant, with listings filtered to the caller's scope; the tenant catalog
  takes `tenant.manage:cluster:*`; the `snapshot` and `changes` feeds take
  `node.view:cluster:*`, the broker credential that already read the
  shard-assignment watch. The credential is checked before existence, so a
  tenant with no keys answers `401` rather than a `404` that says whether it
  exists.
- **Inbound connections on the internal listener are bounded** (#505), by
  `FELIX_INTERNAL_MAX_INBOUND_CONNECTIONS` and
  `FELIX_INTERNAL_MAX_INBOUND_PER_SOURCE`. This outlives mTLS: an
  authenticated peer looping on a bug exhausts a listener exactly as a hostile
  one does.
- **Every refused credential is counted and logged** (#519), so a
  misconfigured broker or an attacker is visible rather than silent.

### Changed

- A broker presents `FELIX_NODE_TOKEN` / `FELIX_NODE_TOKEN_FILE` on the
  metadata sync as well, and accepts one without `FELIX_NODE_ID`: a standalone
  broker pointed at a control plane needs it to learn any stream. Without one
  it still starts, warns once, and has its sync refused.
- The RBAC object grammar accepts `stream:{tenant}/*/*` and `cache:{tenant}/*/*`,
  the tenant-wide forms token exchange already expanded `tenant.manage` to;
  the control plane's own parser had refused them. A wildcard namespace under
  a named leaf is still refused.
- The workspace was reorganised to Rust conventions (#391): `foo.rs` + `foo/`
  throughout, with `unreachable_pub` and `mod_module_files` enforced through
  `[workspace.lints]`.
- The replica-report shapes are shared rather than redeclared, and the
  lifecycle is typed (#447).

### Known limitations

- **Promotion can pick a replica missing an acknowledged `Quorum` record**
  (#527). Promotion reads the leader's last report, so a leader that
  acknowledges and then dies before its next report leaves a fresh report
  naming a replica that never received it. Report expiry does not close it:
  the report is recent, only older than the acknowledgement. The guarantee still
  holds against every fault the suite injects. The TLA+ model added in this
  release is what found the interleaving the suite does not reach. Promotion by greatest (last generation,
  length) closes it and is the rule to move to.
- **Shard rebalancing is not implemented** (#130). A shard whose leader is
  alive is never moved, so adding a broker adds capacity for *new* placements
  only. Scaling a broker StatefulSet up will not migrate existing shards onto
  the new pods.
- **A retried Raft proposal can answer `409` for a write that succeeded**
  (#529), so a provisioning script can be told a tenant it just created already
  exists.
- **There is no rate limiting in the control plane** (#524), and an
  unauthenticated caller can force a JWKS fetch per request by presenting a
  token with an unknown `kid`.
- Published throughput and latency figures remain **RF=1 `Leader`**. The
  harness can now seed `Quorum` streams (#526), but the numbers are not taken
  yet (#425, #375).

## [0.3.1] - 2026-09-16

A real-IdP patch. The 0.3.0 token exchange quietly assumed two things that were
true of the test identity provider and false of Microsoft Entra ID: that a
JWKS key advertises its own algorithm, and that a broker credential minted for
fifteen minutes is long enough. Neither holds against a real Entra tenant, and
0.3.0 could not authenticate against one at all. This release fixes that. No
wire-protocol change; `felix-wire` `VERSION` remains `1`.

### Fixed

- **The control plane rejected every Microsoft Entra ID token** (#371). Entra's
  JWKS keys omit the optional `alg` member (RFC 7517 §4.4), and the exchange
  required it, so a valid RS256 token came back `401 invalid token`. The key's
  algorithm is now checked only when the JWKS actually states one, still bounded
  by the key-type match; an alg-less key verifies against the algorithm the
  token itself declares. 0.3.0 cannot complete a token exchange against Entra;
  0.3.1 can.

### Added

- **`FELIX_EXCHANGE_TOKEN_TTL_SECONDS`** (#371): the lifetime of an exchanged
  Felix token, default `900`. A broker reads its node credential once and holds
  it for its whole lifetime without refreshing, so the fixed 15-minute TTL
  dropped every broker out of the cluster a quarter-hour in. Raising the TTL
  keeps a long-lived node authenticated; it is a stopgap, with proper token
  refresh tracked as follow-up work.

## [0.3.0] - 2026-09-15

The composed-semantics release. A cache stopped being something you can only
poll: you can subscribe to a key, join with current state already in hand, and
fold deltas into durable sums. These compose because each one is a reading of
the same log. A consumer group now survives failover *whole*: the dead-letter
list travels with the shard along with the cursor.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1`. Every new
capability is a negotiated feature bit (`FEATURE_CACHE_WATCH`,
`FEATURE_CACHE_WATCH_RETAINED`, `FEATURE_COUNTERS`). A 0.2.0 client and a
0.3.0 broker interoperate byte-for-byte on everything they both know, and a
client never sends a request the broker did not advertise. The internal
broker-to-broker protocol grew six message kinds (16–21), which is how that
protocol evolves; an older peer answers an unknown kind with a typed refusal
rather than a misparse.

### Added

- **Keyed cache watch** (#348). `cache_watch` subscribes to one key or key
  prefix; every applied write is delivered in the shard's write order with its
  log offset, deletes included as tombstoned changes. Resume by offset replays
  from the cache's log and joins live delivery with no gap and no duplicate,
  which is tested under concurrent writes at join time. An offset compaction has
  collapsed is answered with a marked snapshot of current values
  (`resnapshot`), never a silent gap. A watch that falls behind is **ended
  loudly** with `cache_watch_lagged` naming the first missed offset (filtered
  offsets are sparse, so a drop could never be read from an offset jump), and
  re-watching from that offset is gapless.
- **Retained delivery on a watch** (#349). Ask for `retained` and receive each
  matching key's current value first, at the offset of the write that
  produced it, then live changes: join a presence roster and hold it
  immediately, no polling. `retained_count` makes "your state is now complete"
  an explicit moment, and joining an empty key a definite zero instead of a
  silence. Survives failover: a promoted replica serves the retained value
  from its rebuilt index, at the original offset.
- **Counters** (#350). `counter_add` appends a signed delta and answers with
  the sum *including it*, so one round trip increments and tells you where you
  stand. Scoped and routed exactly like cache keys, stored beside the cache
  (a counter and a cache value sharing a key are unrelated). The sum survives
  restart, compaction (which collapses applied deltas into a checkpoint
  without renumbering the log), and leader failover, where the promoted
  replica folds the true sum from its shipped log and keeps counting.
  **At-least-once**: a retried add after a lost
  acknowledgement double-counts; the decision and its failure mode are
  recorded in `docs/projections.md`.
- **The dead-letter list replicates with its shard** (#362). Group state now
  fails over whole: a promoted leader resumes each group where it had reached
  *and* lists what it had given up on, and an operator's redrive works there.
  Before this, promotion silently forgot exactly the records an operator had
  been told to look at. Entries recorded under the earlier on-disk layout are
  still listed and discardable.
- **Client API**: `watch_cache` / `watch_cache_retained` (typed `Lagged` and
  `resnapshot` surfaces), `counter_add` / `counter_get`, each feature-gated on
  the broker's advertisement before anything is sent.

### Fixed

- A publish forwarded between brokers now lands on the shard it was routed
  for (#356).
- Control-plane readiness is proven against a database that can fail (#358),
  and the rolling-restart and Raft-chaos test harnesses retry a dead
  connection against a re-probed rotation instead of a stale snapshot
  (#359, #364), which is the load-balancer behaviour they model.

### Known limitations

- A prefix watch reads one shard; watching a whole multi-shard cache means one
  watch per shard, with no client helper yet.
- Counters carry no dedupe identity, so retried adds can double-count, and a
  cache watch does not see counter changes.
- A cache still declares no consistency level: its writes carry the `Leader`
  guarantee, not `Quorum`.

The status table
(https://gabloe.github.io/felix/getting-started/what-felix-is-for/) remains
the per-capability source of truth.

## [0.2.0] - 2026-09-07

Durable streams. A stream registered with `durable: true` now persists every
record before acknowledging it, replays it after a restart, and lets a
subscriber resume from an exact offset.

**No wire-protocol break.** `felix-wire` `VERSION` remains `1`. The new
capabilities are negotiated flag bits, so a 0.1.1 client and a 0.2.0 broker
still interoperate: they agree on the original flag set. Negotiating
capabilities instead of bumping a version is what makes this work.

**Durable storage is opt-in and off by default.** Nothing persists unless the
broker is started with `FELIX_DURABLE_STORAGE_DIR` *and* the stream is
registered durable. The on-disk format is version 2; there is no version 1 to
migrate from, because durable storage did not exist in 0.1.1.

**Clustering is not in this release.** It contains broker membership plumbing
(brokers can register, heartbeat, and appear in a control-plane listing), but
nothing routes across brokers. A client still talks to one broker and gets
exactly what it got in 0.1.1. See *Known limitations* below.

### Added

- **Durable single-node log.** A log-structured segment store behind the broker:
  records are never rewritten, a torn tail is repaired on startup while interior
  corruption refuses to start, and indexes are derived rather than trusted.
  Group commit means one device flush serves many appends. Configured with
  `FELIX_DURABLE_STORAGE_DIR`, `FELIX_DURABLE_SEGMENT_BYTES`, and the fsync
  policy (`none`, `periodic`, `on_commit`). (#173, #174)
- **Resumable subscriptions over the wire.** `Subscribe` carries an optional
  `start`: `latest` (the default, and what every older client sends),
  `earliest`, or an exact offset. Every delivered event carries its offset.
  History read from disk joins live delivery with no gap and no duplicate.
  Asking for an offset retention has discarded is a typed error, not a silent
  restart at the tail. (#197)
- **Retention on durable logs.** `FELIX_DURABLE_RETENTION_BYTES` and
  `FELIX_DURABLE_RETENTION_SECONDS` bound a stream's growth by deleting the
  oldest sealed segments. Unset means unbounded, which remains the default. (#203)
- **Capability negotiation for stream authentication.** A client offers
  `Auth.client_flags` and the broker answers `AuthOk.server_flags`. A peer that
  predates negotiation exchanges a plain `Ok`, and the only safe reading of that
  silence is the original flag set. (#170)
- **Binary publish acknowledgements**, so an acked publish no longer pays for a
  JSON round trip. (#167)
- **Broker cluster membership** (control plane only, see *Known limitations*):
  a Node resource model, membership persisted in both stores with an ordered
  changefeed, heartbeat and liveness expiry, broker-side registration and
  graceful deregistration, an operator listing at `GET /v1/nodes`, and
  Prometheus metrics for the fleet. (#218, #219, #220, #221, #222, #223)
- **Documentation site** migrated from MkDocs to Astro Starlight, with the
  durable segment format, storage performance, and delivery semantics written
  up. (#165, #166, #169, #174)
- Demos for lossy and lossless state settlement and for slow-consumer
  isolation. (#163)

### Fixed

- **Lossless publishing was not lossless end to end.** (#171)
- **The backlog-to-live handoff dropped records.** Reading history before
  registering a subscriber loses any publish landing in between; the handoff is
  now ordered so it cannot. (#191)
- **A failed rollover is now terminal.** A rollover fails on a failed fsync or a
  failed file creation, and both mean the log can no longer honour what it has
  already acknowledged. It rejects further appends rather than continuing
  quietly. (#196)
- **An inline rollover no longer parks every Tokio worker.** The segment set is
  behind a synchronous lock; held across a rollover's device flushes it stalled
  everything else on the runtime. It also stopped appends being rejected outright
  under rollover contention: six runs in twenty became zero in twenty. (#217)
- **Record header checksums (format v2)**, plus cancellation, startup, and
  hydration defects found in review. (#178)
- **MTU black-hole collapse on Linux**, where a path MTU below the probed size
  silently destroyed throughput. Acked publishes are now pipelined. (#200, #202)
- Durability, ordering, and recovery defects from successive reviews of the
  durable log. (#176, #179, #180)
- Cursor identity is pinned to disk offsets, and the broker proves its catalog
  seeded before reporting ready. (#180)

### Changed

- **An fsync was cut from the rollover path.** A freshly created and therefore
  empty index file was being flushed. Median p999 on the default inline path
  improved from 5.12ms to 3.98ms (−22%), and max from 61.8ms to 47.0ms (−24%).
  Background rollover was added alongside it and ships **disabled**, because it
  measured worse: `F_FULLFSYNC` does not overlap with concurrent writes, so
  moving a rollover's flushes off the append lock stops confining them rather
  than hiding them. (#192)
- Performance is now measured on a Linux 16 vCPU VM in addition to macOS, since
  the two platforms' fsync semantics differ enough to invert a result. (#201)

### Dependencies

- `opentelemetry` 0.31.0 → 0.32.0, `opentelemetry-otlp` 0.31.1 → 0.32.0,
  `opentelemetry_sdk` 0.31.0 → 0.32.1, `tracing-opentelemetry` 0.32.1 → 0.33.0.
  These four are version-locked to each other and have to move together.
- `sqlx` 0.8.6 → 0.9.0. Its `SqlSafeStr` bound means `sqlx::query` now takes
  only `&'static str`, so dynamic statements need `AssertSqlSafe` and a reason.
- `base64` 0.22 → 0.23, `bytes` 1.11 → 1.12, `dashmap` 6.1 → 6.2,
  `uuid` 1.21 → 1.24, `socket2` 0.6.2 → 0.6.5, `rcgen` 0.14.7 → 0.14.9,
  `metrics` 0.24.3 → 0.24.6, `serde_json` 1.0.149 → 1.0.151.
- `h2` → 0.4.16 for RUSTSEC-2026-0258.

### Known limitations

- **No clustering.** The membership endpoints added here are a registry. No
  traffic crosses a broker, brokers do not talk to each other, and there is no
  sharding, replication, or failover. Placement lands in M3, cross-broker
  forwarding in M4.
- **The membership endpoints are not authenticated.** Registration, heartbeat,
  drain, and deregister accept any caller that can reach them; only the operator
  read endpoints require a token. Safe on a trusted network, not on an open one.
  Tracked in #126.
- **At-most-once delivery.** Slow subscribers drop under the default policy.
  Durable streams make the *record* recoverable (a subscriber can resume from
  an offset), but nothing redelivers automatically.
- Single-region, single control-plane instance.

### Upgrading from 0.1.1

No code changes are required, and no configuration becomes mandatory. Every
addition above is opt-in.

- 0.1.1 clients work against a 0.2.0 broker unchanged. Capabilities are
  negotiated, so an older client simply does not offer the new flags.
- Durable storage stays off until `FELIX_DURABLE_STORAGE_DIR` is set. Setting it
  is not sufficient on its own: a stream must also be registered `durable: true`.
- If you do enable durable storage, decide the fsync policy deliberately.
  `periodic` (the default) bounds loss to one interval; `on_commit` loses
  nothing acknowledged and costs milliseconds per publish.
- `segment_size_bytes` is a latency knob as well as a recovery knob. Below a few
  MiB, tail latency is dominated by rollover flushes. Prefer the 256 MiB default
  unless restart time is really the binding constraint, and measure the tail
  if you change it.

## [0.1.1] - 2026-08-09

Maintenance release. No wire-protocol changes. `felix-wire` `VERSION` remains `1`,
and 0.1.0 clients interoperate with 0.1.1 brokers.

The headline items are two correctness fixes in the subscriber path and a set of
new per-connection resource limits that are enabled by default.

### Added

- Per-connection publish and subscription limits, configurable via
  `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES` (default 16 MiB) and
  `FELIX_MAX_SUBSCRIPTIONS_PER_CONN` (default 4096). Previously a single
  connection could occupy the entire process-wide publish budget. (#145)
- `FELIX_PUB_INGRESS_WAIT` and `FELIX_SUB_QUEUE_POLICY` to select ingress
  backpressure and subscriber-queue overflow behavior. (#145)
- SIGTERM-aware graceful shutdown for the broker, so in-flight work drains
  instead of being cut off at process exit. (#139, #153)
- Resource sampling in the soak harness: CPU, memory, and queue-depth series
  captured across a run. (#156)
- Benchmark regression gate on pull requests, plus historical benchmark
  dashboards published with the docs. (#147, #149, #152)

### Fixed

- **Subscriber queue depth accounting race.** Depth is now released by RAII on
  the queued item, so a dropped receiver, a cancelled `recv`, or a channel
  teardown can no longer leak depth and wedge a subscriber's backpressure
  signal. The process-global depth counter was replaced by per-stream deltas. (#157)
- **Subscription ID collisions and fanout delivery loss.** Subscription IDs are
  now broker-assigned, and per-connection writers are created atomically, fixing
  events being routed to the wrong subscriber or dropped entirely under
  concurrent subscribe. (#148)
- Removed two `unsafe impl Send` blocks that asserted nothing, since both types were
  already `Send + Sync` by their fields. Replaced with `const _` assertions that
  fail the build at the definition if a future field breaks the property. (#140, #153)
- Bounded attacker-declared payload counts in the wire decoder before
  allocating, closing an allocation-abort denial-of-service path on binary batch
  frames. (#140, #153)
- Soak harness resource settling and reporting. (#158)
- Benchmark dashboards now load Chart.js locally rather than from a CDN, and
  benchmark data paths were corrected. (#150, #151)
- Core shards disabled in performance workflows, which were causing spurious
  regression reports. (#155)

### Changed

- Split the four largest modules into focused submodules: `felix-wire`
  (1641 lines), `felix-broker` (1986), and the QUIC `publish` (4231) and
  `subscribe` (3346) handlers. No public paths changed; every item is
  re-exported from its original location. (#159)
- Crate versions are now inherited from `[workspace.package]` via
  `version.workspace = true`, so a release bump is a single-line edit. (#160)
- Minimum supported Rust version raised to 1.97. (#146)
- Licensing clarified per path: `felix-wire`, `felix-client`, `felix-common`,
  `felix-transport`, and `felix-conformance` are Apache-2.0; the broker,
  control plane, and remaining crates are Elastic-2.0. See
  [LICENSING.md](LICENSING.md). (#145)

### Dependencies

- `tokio` 1.49 → 1.50, `rand` 0.9.2 → 0.10.0, `tempfile` 3.26 → 3.27,
  `tracing-subscriber` 0.3.22 → 0.3.23, `opentelemetry-otlp` 0.31.0 → 0.31.1.
- CI: `actions/deploy-pages` 4 → 5, `actions/upload-pages-artifact` 4 → 5.

### Upgrading from 0.1.0

No code changes are required. Two things to check before deploying:

1. The new per-connection limits are **on by default**. A deployment that
   sustains more than 16 MiB of in-flight publish bytes or more than 4096
   subscriptions on a single connection will now be throttled where it
   previously was not. Raise
   `FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES` / `FELIX_MAX_SUBSCRIPTIONS_PER_CONN`
   if that describes your workload.
2. Building from source now requires Rust 1.97.

## [0.1.0] - 2026-08-07

Initial public milestone: QUIC transport, JSON and binary wire framing,
in-process pub/sub broker with bounded subscriber queues and slow-consumer
isolation, ephemeral cache, tenant/namespace/stream registries, RBAC and Felix
token authorization, a control plane with a Postgres-backed store, a Rust client
SDK, and a protocol conformance runner.

[Unreleased]: https://github.com/gabloe/felix/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/gabloe/felix/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/gabloe/felix/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/gabloe/felix/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/gabloe/felix/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/gabloe/felix/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/gabloe/felix/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/gabloe/felix/releases/tag/v0.1.0
