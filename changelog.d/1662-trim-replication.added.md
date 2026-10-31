- Replication ships head trims, the fourth piece of #1662. A trim record
  travels with mark byte `5` and its floor, and a follower of a `Quorum`
  stream applies the floor only once the leader's commit offset passes the
  record. A promoted leader raises its log's commit offset before it
  publishes the quorum mark, so a trim it inherited applies before its first
  read. A new fleet feature, `head_trim`, gates trims in a cluster, and a
  new peer capability, `HEAD_TRIM`, keeps a batch holding a trim from any
  follower that did not offer it; shipping counts that as the
  `trim_unsupported` outcome of `felix_broker_replication_shipped_total`, and
  logs it at most once a minute. Trims now fail closed:
  `Broker::allow_head_trims_when` replaces `set_replication_enabled`, and a
  broker refuses every trim with `HeadTrimNotEnabled` (formerly
  `TrimNotReplicatedYet`) until startup says it runs alone or its fleet has
  finalized `head_trim`. A new cluster finalizes `head_trim` at its first
  registration. `mark_to_wire` no longer fails, and `UnshippableMark` is gone.
