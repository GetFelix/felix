- A broker acts on a seal order (#2242): an assignment whose new `switch`
  field orders a seal closes the shard without the ordinary open, the leader
  runs the seal round once the shard is quiet, reports to the control plane
  (`POST /v1/nodes/{node_id}/seal-report`) and gives up through
  `seal-give-up` instead of running a round twice. A sealed copy's cursor
  and dead-letter logs take no local write and are not compacted. A shard
  whose report was accepted stays closed for the hybrid; starting its Raft
  member comes next. Assignments without the field are unchanged. A round
  that reaches no majority within `FELIX_SEAL_DEADLINE_MS` (30 seconds by
  default) is given up, so a shard is not left closed behind copies that do
  not answer.
