- An RF 2 shard no longer stays closed after its leader crashes right after a
  set change (#1486). The failover named every copy the leader's last report
  did, and that report could predate the change and name a copy already
  dropped. The crashed leader of a set of two then refused the new leader's
  fence until the operator ran `felixctl placement abandon`. A failover now
  names only copies in the old set or the new one.
