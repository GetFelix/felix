- On a Raft shard, an ack that a consumer group answered OK is never handed out
  again, even after a change of leader. An ack above a gap (a record acknowledged
  while an earlier one is still out) used to live only in the leader's memory, so
  the next leader restored its claim and redelivered the record once it lapsed.
  It is now a group entry of its own (op 8), kept in the group table and its
  snapshot (table version 3), and the ack is answered once that entry has applied
  (#3360).
- A Raft stream shard snapshot whose consumer-group table is older than
  version 3, as a v0.8.0-preview.1 data directory holds, is refused rather
  than loaded without its acks. Recreate the shard (#3360).
