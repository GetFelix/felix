- A control-plane member could fail to restart after a snapshot it built
  finished after a newer snapshot it received from the leader (#1745). The
  older snapshot replaced the newer one on disk while the log was already
  purged up to the newer one, so replay asked for entries that were gone. A
  snapshot is now written only if it is newer than the stored one, checked in
  the same transaction as the write.
