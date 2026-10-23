- A new cluster finalizes every fleet feature its brokers report at its first
  broker registration, not only the incarnations: `generation_start`,
  `majority_ack`, `lease_free_reads`, `fenced_caches`, `jump_hash_routing`,
  `atomic_commit`, `group_cursor_dependencies`, `stream_incarnations` and
  `cache_incarnations`, in that order. A fresh RF=3 cluster used to acknowledge
  `Quorum` writes on the control plane's replica report until an operator
  finalized `majority_ack`; it now acknowledges on its followers from the
  first stream. `publisher_principal` stays an operator step, since it grows
  every record by its publisher's id. Each finalize goes through the
  operator's checks, so a feature some broker does not report stays off with
  the features that need it, and under the Raft backend nothing is finalized
  until every member runs this release; a finalize held back that way is
  retried in the background for up to ten minutes. Start every broker with the same
  `FELIX_QUORUM_READS` and `FELIX_INTERNAL_FENCE`: a broker that lacks a
  feature the first one finalized is refused, and the refusal now names
  `FELIX_INTERNAL_FENCE` as well as `FELIX_QUORUM_READS`. An upgraded cluster
  still waits for the operator.
