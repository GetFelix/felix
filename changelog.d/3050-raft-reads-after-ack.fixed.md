- On a Raft stream, a range read, an offset-for-time lookup and a consumer
  group describe or seek made right after a publish was acknowledged could
  answer as if the publish had not happened, because the acknowledgement comes
  before the member applies the entry. They now wait briefly for the records
  the member holds to pass the read bound (#3050).
- A Raft write refused before anything was appended, such as one sent while no
  majority has answered the leader, is answered as an unavailable shard that
  can be retried, not as an unknown outcome (#3050).
