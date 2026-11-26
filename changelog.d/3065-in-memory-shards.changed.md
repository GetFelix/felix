- A broker that runs Raft serves in-memory stream shards on a path of their own,
  with no hybrid lifecycle, promote fence or replication pass. They behave as
  before: one owner placed by the control plane, acknowledged on enqueue,
  refused once the owner's lease lapses, and empty on a new owner (#3065).
