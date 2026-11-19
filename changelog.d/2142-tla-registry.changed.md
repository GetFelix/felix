- Every TLA+ configuration in `docs/formal` is now model-checked in CI or listed with the reason it is not, and
  `task docs:evidence` fails on one that is neither (#2142). `FelixRaftPlacementPlacedTarget` gets a smaller twin
  without the client write, and a witness configuration for the placed transfer racing a target change (#2676).
