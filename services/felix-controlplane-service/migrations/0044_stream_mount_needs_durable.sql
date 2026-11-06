-- A mounted stream must be durable (#1690). The store refuses a mount on a
-- non-durable stream, or making a mounted stream non-durable, in the same
-- transaction that writes it; this states the rule in the schema as well.
--
-- Added NOT VALID, so it holds for every write from here on without
-- failing the migration over a row written before it. It is validated only
-- when no row breaks it, which every row the store wrote satisfies.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'streams_mount_needs_durable'
          AND conrelid = 'streams'::regclass
    ) THEN
        ALTER TABLE streams
            ADD CONSTRAINT streams_mount_needs_durable
            CHECK (mount IS NULL OR durable) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM streams WHERE mount IS NOT NULL AND NOT durable) THEN
        ALTER TABLE streams VALIDATE CONSTRAINT streams_mount_needs_durable;
    END IF;
END
$$;
