- Every TLA+ configuration in `docs/formal` is now model-checked in CI or listed with the reason it is not, and
  `task docs:evidence` fails on one that is neither (#2142). `FelixRaftPlacementPlacedTarget` gets a bounded
  twin that finishes, with a witness that it still reaches a placed transfer racing a target change (#2676).
