- A simulator seed with Raft groups runs the same twice again (#2567).
  The Raft ticker's map of peer brokers and the broker's batched log marks used std's randomly keyed maps, so the order the ticker started its per-broker heartbeats and retries changed from run to run. They now use the seeded maps the rest of the broker uses, and a Raft copy id is drawn from the seed under a simulator too.
