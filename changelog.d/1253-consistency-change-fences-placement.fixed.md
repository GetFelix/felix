- Changing a stream's consistency or durability now fences placement passes
  that read the stream before the change (#1253). A pass that read a `Quorum`
  stream could otherwise replace its leader on its followers' word after the
  stream became `Leader` and the leader was serving on its lease, leaving two
  brokers serving. Postgres gets migration 0036; under Raft it applies once
  every member is at metadata level 19.
