-- Which creation of its name a stream is. 0 is a stream created before
-- incarnations, whose shard directories on the brokers keep the names they
-- always had.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;

-- The incarnation of the stream a shard assignment is for, written with the
-- assignment, so a change replayed after the stream was created anew still
-- names the stream it was written for.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;
