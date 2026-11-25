- The `felix-cluster client-fixture` cluster now reports ready only once every
  Raft shard is led by the broker placement names (#3130). Before, a client
  suite could publish while a new group was still handing its first
  leadership over, and a plain keyed publish to that shard was refused as not
  ready.
