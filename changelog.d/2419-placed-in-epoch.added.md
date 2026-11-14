- Shard assignments carry `placed` (#2419): every broker the control plane
  has placed for the shard's group in its epoch, removed ones included. A
  broker is added by the write that first names it and never removed, so the
  per-shard Raft re-form command can read every disk that may hold
  acknowledged records. Started by a shard's first assignment; under the Raft
  metadata store it needs every member at metadata level 31.
  Restoring an export into a Raft metadata store now also needs level 31:
  the leader refuses an import until every control-plane member is upgraded.
