-- The storage identity (32 lowercase hex) the replica report a failover was
-- decided on named for each replica, as a JSON object of node id to identity.
-- The promoted leader's fence counts a replica's answer only from that copy.
-- NULL is what every row held before this said, and is read as none.
--
-- The generation marker is as by_round_at is for by_round (0029). A member
-- that predates the column updates the row and leaves the map behind, so it
-- is read only while the marker matches the row's generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS fence_storage_ids JSONB,
    ADD COLUMN IF NOT EXISTS fence_storage_generation BIGINT;
