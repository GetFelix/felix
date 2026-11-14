- An unacknowledged publish to a shard on Raft (`FELIX_SHARD_ENGINE=raft`) is proposed to its
  group and delivered, like an acknowledged one (#2448). It was refused at the queue, by the
  hybrid's write fence, which never opens for a Raft shard, so its subscribers got nothing and
  the publisher was told nothing. An unacknowledged publish that is queued and whose
  write is not confirmed, on either engine, is now counted in
  `felix_publish_requests_total{result="unacked_failed"}`. A shard on Raft
  always answers a publish at its commit: `FELIX_ACK_ON_COMMIT=0` does not apply to it.
