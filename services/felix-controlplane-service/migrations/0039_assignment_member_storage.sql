-- The storage identity each member of a shard's set registered with, as JSON
-- ({"<node id>": "<32 hex>"}), written on every placement write (#1009).
-- NULL is what every row held before this said, and is read as none.
--
-- The generation marker is as fence_storage_generation is for
-- fence_storage_ids (0034): a member that predates the column updates the row
-- and leaves the map behind, so it is read only while the marker matches the
-- row's generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS member_storage_ids JSONB,
    ADD COLUMN IF NOT EXISTS member_storage_generation BIGINT;
