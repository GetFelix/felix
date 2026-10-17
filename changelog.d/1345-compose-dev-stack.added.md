- `compose.yaml` at the repository root runs a control plane, a broker and
  development credentials from the released images, so `docker compose up` (or
  `podman compose up`) is enough to publish and subscribe with `felixctl`
  (#1345). The credentials are dev-only and every port is bound to loopback. CI
  does a round trip through the stack on every change.
