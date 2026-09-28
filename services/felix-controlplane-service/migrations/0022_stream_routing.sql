-- How a stream maps routing keys to shards: 'modulo' or 'jump_hash'.
--
-- Nullable with no default: NULL is modulo, the mapping every existing stream
-- was written with. Filling in anything else would move their keys.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS routing TEXT;
