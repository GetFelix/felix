-- Every stream and cache name deleted on this database (#1447). Without
-- incarnations a stream created again under a deleted name opens the deleted
-- one's logs on every broker, each holding what it had of them, and its
-- replicas can diverge. So until the name's incarnations are finalized, a
-- create of a deleted name is refused. Kept for good, like tombstones, and
-- written for every delete, including a cascade from a namespace or tenant
-- and one by an instance that predates this migration.
CREATE TABLE IF NOT EXISTS deleted_stream_names (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, stream)
);

CREATE TABLE IF NOT EXISTS deleted_cache_names (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    cache TEXT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, cache)
);

CREATE OR REPLACE FUNCTION felix_record_deleted_stream_name() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO deleted_stream_names (tenant_id, namespace, stream)
        VALUES (OLD.tenant_id, OLD.namespace, OLD.stream)
        ON CONFLICT DO NOTHING;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS streams_deleted_name ON streams;
CREATE TRIGGER streams_deleted_name
    AFTER DELETE ON streams
    FOR EACH ROW EXECUTE FUNCTION felix_record_deleted_stream_name();

CREATE OR REPLACE FUNCTION felix_record_deleted_cache_name() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO deleted_cache_names (tenant_id, namespace, cache)
        VALUES (OLD.tenant_id, OLD.namespace, OLD.cache)
        ON CONFLICT DO NOTHING;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS caches_deleted_name ON caches;
CREATE TRIGGER caches_deleted_name
    AFTER DELETE ON caches
    FOR EACH ROW EXECUTE FUNCTION felix_record_deleted_cache_name();

-- The refusal, in the database so every instance writing it is held to it.
-- Read under the finalize's shared lock (0028, 0032), so a create that read
-- "not finalized" commits before the finalize does. The words match
-- `store::reused_name`.
CREATE OR REPLACE FUNCTION felix_refuse_reused_stream_name() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM deleted_stream_names
            WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND stream = NEW.stream)
        AND NOT felix_stream_incarnations_finalized() THEN
        RAISE EXCEPTION 'a stream named % was deleted on this cluster; finalize stream_incarnations before reusing the name (POST /v1/fleet/features/stream_incarnations/finalize)', NEW.stream
            USING ERRCODE = 'FX447';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS streams_reused_name ON streams;
CREATE TRIGGER streams_reused_name
    BEFORE INSERT ON streams
    FOR EACH ROW EXECUTE FUNCTION felix_refuse_reused_stream_name();

CREATE OR REPLACE FUNCTION felix_refuse_reused_cache_name() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM deleted_cache_names
            WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND cache = NEW.cache)
        AND NOT felix_cache_incarnations_finalized() THEN
        RAISE EXCEPTION 'a cache named % was deleted on this cluster; finalize cache_incarnations before reusing the name (POST /v1/fleet/features/cache_incarnations/finalize)', NEW.cache
            USING ERRCODE = 'FX447';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS caches_reused_name ON caches;
CREATE TRIGGER caches_reused_name
    BEFORE INSERT ON caches
    FOR EACH ROW EXECUTE FUNCTION felix_refuse_reused_cache_name();
