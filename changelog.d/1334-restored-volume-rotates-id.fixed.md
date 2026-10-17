- A broker whose data directory was restored from a volume snapshot or copied in
  no longer keeps its storage id when the node has registered since the copy
  was taken, under any fsync mode (#1334). The broker records each
  registration's node incarnation in `<data dir>/registration`, and a cluster
  member asks the control plane for its current incarnation before any store
  opens: when the control plane is ahead, it writes a new storage id, so peers
  and placement stop counting the copy on positions it no longer holds. A
  cluster member with durable storage now waits for the control plane to
  answer before it opens storage. A copy taken during the node's current
  registration is not detected, so the backup runbook still deletes
  `storage-id` for one.
