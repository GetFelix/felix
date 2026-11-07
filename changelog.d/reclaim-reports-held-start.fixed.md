- A wiped copy re-admitted as a learner no longer stays one for good when its
  leader crashes before reporting it caught up. The given-up leader's probe now
  lists, in `holds_start`, every follower whose copy holds the generation it
  gave up on every log of the shard, so placement admits the learner and can
  promote it. Before, such a shard could wait as `NoCaughtUpReplica` until an
  operator abandoned it.
