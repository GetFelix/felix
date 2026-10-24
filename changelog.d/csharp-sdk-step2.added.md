- C# SDK (`sdks/dotnet`): consumer groups. `FelixClient.JoinGroup` gives a `FelixGroup` for one shard
  (poll with a named consumer, visibility and reclaim; ack, nack, extend, dead-letter, discard, redrive;
  create, seek, describe, delete), `OpenShardedGroupAsync` a `FelixShardedGroup` over every shard, and
  `GetShardCountAsync` and the `*StreamGroupAsync` calls run the group admin across a stream.
  `FelixClientOptions.Publishers` and `.Timestamps` ask brokers for each record's publisher and append
  time, set on `FelixEvent` and `GroupRecord`. Tests tag conformance scenarios with `[Scenario]` and
  write the `FELIX_CONFORMANCE_RESULTS` document; CI runs the verdict for C# without blocking on it.
