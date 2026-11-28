- On a Raft shard, an ack that a consumer group answered OK is never handed out
  again, even after a change of leader. An ack above a gap (a record acknowledged
  while an earlier one is still out) used to live only in the leader's memory, so
  the next leader restored its claim and redelivered the record once it lapsed.
  It is now a group entry of its own (op 8), kept in the group table and its
  snapshot (table version 3), and the ack is answered once that entry has applied
  (#3360).
