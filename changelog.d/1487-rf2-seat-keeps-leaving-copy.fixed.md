- A failover right after an RF 2 replacement seats its newcomer no longer leaves
  the shard closed (#1487). While the old leader had not yet reported at the
  seat's generation, a record could be on it and the leaving copy alone; the
  failover now keeps the leaving copy in the set while its broker serves, so the
  new leader's fence reaches a majority that holds the record. A set left above
  its factor that way is trimmed back once the rest hold what it held, so a
  drain in a three-broker cluster still finishes.
