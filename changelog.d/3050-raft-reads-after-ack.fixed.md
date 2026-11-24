- On a Raft stream, a range read, an offset-for-time lookup and a consumer
  group describe or seek made right after a publish was acknowledged could
  answer as if the publish had not happened. The read barrier now covers
  writes answered on the leader's lease too, and the offset-for-time lookup
  and a group seek pass it; a read that cannot pass it fails as retryable
  instead of answering "nothing yet" (#3050).
- A Raft write openraft refused before appending anything, because the
  leader's lease lapsed or the batch was admitted at another term, is answered
  as an unavailable shard that can be retried, not as an unknown outcome
  (#3050).
