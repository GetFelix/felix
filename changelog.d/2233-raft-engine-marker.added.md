- A Raft shard's directory holds an `engine` marker file, written durably
  before the shard's log is first opened
  (`felix_storage::disk_log::engine_marker::claim_for_raft`, #2233). The
  hybrid engine refuses to open a directory that holds it, and a Raft member
  refuses a directory that holds segments without it, so two brokers that
  disagree on a shard's engine cannot both write one log.
