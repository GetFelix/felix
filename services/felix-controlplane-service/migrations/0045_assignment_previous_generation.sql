-- The generation of the assignment a placement write replaced when it landed
-- more than one generation above it, a rename past a ballot (#2024). A broker
-- takes that assignment's set as the one right before this generation. NULL is
-- what every row held before this, and is read as none.
--
-- previous_generation_at is the marker, as learners_generation is for learners
-- (0040): a member that predates the columns updates the row and leaves the
-- value behind, so it is read only while the marker matches the row's
-- generation.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS previous_generation BIGINT,
    ADD COLUMN IF NOT EXISTS previous_generation_at BIGINT;
