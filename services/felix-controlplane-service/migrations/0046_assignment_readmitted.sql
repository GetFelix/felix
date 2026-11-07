-- The replaced copies a leader's report showed caught up on a set naming them
-- by their new storage identity (ShardAssignment::readmitted). A fence may
-- trust an answer from such a copy. NULL is what every row held before this,
-- and is read as none.
--
-- readmitted_at is the marker, as previous_generation_at is (0045): a member
-- that predates the columns updates the row and leaves the value behind, so it
-- is read only while the marker matches the row's generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS readmitted JSONB,
    ADD COLUMN IF NOT EXISTS readmitted_at BIGINT;
