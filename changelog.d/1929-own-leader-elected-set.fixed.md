- The control plane no longer keeps its own leader on the recorded replica set
  when that leader was elected on a different one (#1929). It took only the
  election's generation, so its record could name a set the shard never
  opened on, and a later failover could promote from it without records a
  majority of the elected set held. It now names the leader one generation
  up, leaving the elected set for the recorded one, as for any other leader
  elected on another set. A report at the assignment's own generation from
  the named leader on another set is handled the same way.
  Under Raft the adoption change waits for metadata level 27 on every member.
