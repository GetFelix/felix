-- The members the control plane holds as learners after their storage was
-- replaced, as a JSON array of node ids, written on a placement write while
-- the fleet has joint fences (#1737). NULL is what every row held before
-- this said, and is read as none.
--
-- The generation marker is as fence_storage_generation is for
-- fence_storage_ids (0034): a member that predates the column updates the row
-- and leaves the set behind, so it is read only while the marker matches the
-- row's generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS learners JSONB,
    ADD COLUMN IF NOT EXISTS learners_generation BIGINT;

-- The followers a report's leader saw holding its generation's start record,
-- as a JSON array of node ids. holds_start_at is the marker, as given_up_at
-- is for given_up (0033).
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS holds_start JSONB,
    ADD COLUMN IF NOT EXISTS holds_start_at BIGINT;
