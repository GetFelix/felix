-- The set a cut-over is moving a shard to while the change is pending, as
-- JSON ({"replicas": [...], "generation": n}). The assignment's own leader and
-- replicas stay the control plane's set until the leader reports its
-- generation's start record acknowledged on a majority of both (#1009).
-- NULL is what every row held before this said, and is read as none.
--
-- The generation marker is as fence_storage_generation is for
-- fence_storage_ids (0034): a member that predates the column updates the row
-- and leaves the set behind, so it is read only while the marker matches the
-- row's generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS pending_set JSONB,
    ADD COLUMN IF NOT EXISTS pending_set_generation BIGINT;

-- The pending set's generation, once a report's leader holds its generation's
-- start record acknowledged on a majority of the old set and of the new.
-- joint_start_acked_at is the marker, as given_up_at is for given_up (0033).
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS joint_start_acked BIGINT,
    ADD COLUMN IF NOT EXISTS joint_start_acked_at BIGINT;
