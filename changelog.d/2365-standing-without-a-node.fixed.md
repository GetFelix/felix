- A metadata Raft member no longer answers for another member (#2365). Two
  addresses in `FELIX_RAFT_PEERS` that reach the same member, such as an IP
  and a DNS name for one pod, used to let that member's empty answer count
  twice on a first start, so a group could form without asking the member
  that held its data. `standing` now carries the answering member's id and
  an empty answer counts only for that member. Every vote request, append
  and snapshot names the member it is for in `x-felix-raft-target`, and a
  member answers 503 to one meant for another id
  (`felix_meta_raft_peer_rejected_total{reason="target"}`).
