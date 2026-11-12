- A metadata member that falls behind the control plane's purged log can catch up
  again (#2345). Each snapshot chunk had 200 ms to cross the network, so a snapshot
  of more than a few hundred KB never finished and the member stayed behind for
  good. The group kept working on the other two members, and losing either of them
  then stopped every metadata write. Chunks now get 10 s and are 512 KiB, and log
  batches are capped at 64 entries so each one fits inside the heartbeat interval.
