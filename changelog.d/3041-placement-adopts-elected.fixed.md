- On a Raft shard whose named leader goes down, placement now names the member
  the group elected, read from that leader's floor reports, instead of the
  member leading fewest (#3041). The group's leader does not hand over while a
  voter is down, so before this the assignment could point at a follower until
  the broker came back, and discovery, `felixctl` and dashboards showed the
  wrong broker. The control plane records the elected broker from metadata
  level 52.
