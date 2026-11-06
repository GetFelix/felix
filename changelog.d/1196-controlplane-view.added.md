- A cluster member keeps what the control plane last told it about its own
  shards in `controlplane.view` in its data directory: the finalized fleet
  features, the shards' assignments, the addresses of the nodes they name, and
  their catalog entries (#1196). It is rewritten at most once a second while the
  control plane changes something, and nothing reads it yet. A later release
  starts from it when the control plane is down at a restart.
