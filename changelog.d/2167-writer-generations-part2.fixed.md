- Writer generations (#2167): a record a sender holds in a segment older than
  v9 now travels as an unknown writer (generation 0) rather than the
  generation its history labels it with, so replicas whose histories disagree
  no longer halt a healthy follower below its floor. A v9 leader append is
  refused until the broker has recorded where its leadership starts. A
  follower that refuses a batch without writers is sent them from then on, so
  one that hears the `record_generations` finalize before its leader does not
  stall.
