- A stream deleted and created again under the same name no longer reopens
  the deleted stream's logs once the fleet finalizes the new
  `stream_incarnations` feature (#1208). Each created stream then gets an
  `incarnation`, higher for every creation of its name (the control plane's
  clock, raised above the name's last deleted one), carried in the catalog and
  the shard assignments, and brokers keep each incarnation's records, group
  positions and dead letters in directories named with it. Every stream
  delete after the finalize, cascades included, records a tombstone for the
  name, kept for good, served in `GET /v1/streams/snapshot` as `tombstones`
  and on the `Deleted` change as `deleted_through`. A broker deletes a
  stream's data only on one of those, or on an operator's removal: every
  incarnation up to the one named goes from every store, through `.deleted/`
  and a marker under `.streams/` that lets a delete cut short finish at the
  next start. A broker that was down through a delete and a recreate deletes
  the old data from the snapshot's tombstones. It never moves a name to a
  lower incarnation or one the control plane deleted, and refuses a record
  without the incarnation for a name it has seen incarnated, counting both in
  `felix_broker_stream_incarnation_refused_total`. A delete from before the
  finalize keeps the data as before, so a stream created again at
  incarnation 0 reopens it consistently with the quorum marks kept for it.
  The first assignment of an incarnated stream's shard takes its change's
  sequence number as its generation. A control-plane restore can still make
  that equal to a deleted incarnation's, so leaders keep follower positions
  and quorum marks per incarnation too, replica reports carry an optional
  `stream_incarnation` that the control plane checks against the
  assignment's, and a stream delete takes its shards' replica reports in the
  in-memory and Raft stores as Postgres already did.
  A follower refuses replication for an
  incarnation it does not hold (the new internal `Incarnated` frame, kind
  40). The catalog sync moves past each change it applies, a shard assigned
  before its stream registers waits without holding up the others, and a
  stream whose namespace or tenant was removed first is still removed when
  its cascading delete arrives. Existing streams are incarnation 0 and keep
  their directory names.
  `felix-broker inspect segments` takes `--incarnation`; backup points carry
  the incarnation, and `restore-point` sets each stream's back to the
  point's. A broker joining a fleet with many deleted names records their
  tombstones with one flush, and `POST /v1/resources` answers with the
  incarnation the store gave each stream. Once incarnations are finalized, a
  create proposes the clock even while a Raft member has not yet heard from a
  peer since it restarted. Such a peer counts at level 10 then; a peer that
  answers with a lower level still holds level-10 commands back.
- Upgrade notes for `stream_incarnations` (#1208): it needs every Raft
  control-plane member at metadata level 10 and every broker on this release
  before it can be finalized. A Postgres control plane runs migration `0028`
  once for the database. An older Postgres instance still running after the
  finalize cannot create streams, and the database stamps its shard
  assignments with the stream's incarnation, but brokers refuse its catalog
  answers for incarnated streams, so upgrade every instance before
  finalizing.
