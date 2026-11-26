- A Raft follower with no subscriber on a shard no longer reads back from
  its log every record it commits. It moves the stream to the commit and
  leaves the replay ring empty; a subscriber that joins later reads what
  lies below its join from the log, as after a restart (#3223).
