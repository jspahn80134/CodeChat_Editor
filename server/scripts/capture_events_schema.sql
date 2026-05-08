-- CodeChat capture event schema for dissertation analysis.
--
-- This script is safe to run on an existing legacy `events` table. It adds the
-- richer typed metadata columns, converts `timestamp` and `data` to analysis-
-- friendly PostgreSQL types, and backfills typed metadata from existing JSON
-- payloads where possible.

BEGIN;

CREATE TABLE IF NOT EXISTS public.events (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            TEXT,
    sequence_number     BIGINT,
    schema_version      INTEGER,
    user_id             TEXT NOT NULL,
    assignment_id       TEXT,
    group_id            TEXT,
    condition           TEXT,
    course_id           TEXT,
    task_id             TEXT,
    session_id          TEXT,
    event_source        TEXT,
    language_id         TEXT,
    file_hash           TEXT,
    file_path           TEXT,
    path_privacy        TEXT,
    capture_mode        TEXT,
    event_type          TEXT NOT NULL,
    "timestamp"         TIMESTAMPTZ NOT NULL DEFAULT now(),
    client_timestamp_ms BIGINT,
    client_tz_offset_min INTEGER,
    server_timestamp_ms BIGINT,
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.events ADD COLUMN IF NOT EXISTS event_id TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS sequence_number BIGINT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS schema_version INTEGER;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS condition TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS course_id TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS task_id TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS session_id TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS event_source TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS language_id TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS file_hash TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS path_privacy TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS capture_mode TEXT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS client_timestamp_ms BIGINT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS client_tz_offset_min INTEGER;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS server_timestamp_ms BIGINT;
ALTER TABLE public.events ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$
DECLARE
    current_type TEXT;
BEGIN
    SELECT data_type INTO current_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'events'
      AND column_name = 'timestamp';

    IF current_type IS DISTINCT FROM 'timestamp with time zone' THEN
        ALTER TABLE public.events
            ALTER COLUMN "timestamp" TYPE TIMESTAMPTZ
            USING COALESCE(NULLIF("timestamp"::text, '')::timestamptz, now());
    END IF;
END $$;

DO $$
DECLARE
    current_type TEXT;
BEGIN
    SELECT data_type INTO current_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'events'
      AND column_name = 'data';

    IF current_type IS DISTINCT FROM 'jsonb' THEN
        ALTER TABLE public.events
            ALTER COLUMN data TYPE JSONB
            USING CASE
                WHEN data IS NULL OR btrim(data::text) = '' THEN '{}'::jsonb
                ELSE data::jsonb
            END;
    END IF;
END $$;

UPDATE public.events
SET data = '{}'::jsonb
WHERE data IS NULL;

ALTER TABLE public.events ALTER COLUMN data SET DEFAULT '{}'::jsonb;
ALTER TABLE public.events ALTER COLUMN data SET NOT NULL;
ALTER TABLE public.events ALTER COLUMN "timestamp" SET DEFAULT now();
ALTER TABLE public.events ALTER COLUMN "timestamp" SET NOT NULL;

UPDATE public.events
SET
    event_id = COALESCE(event_id, NULLIF(data->>'event_id', '')),
    sequence_number = COALESCE(
        sequence_number,
        CASE
            WHEN data->>'sequence_number' ~ '^-?[0-9]+$'
            THEN (data->>'sequence_number')::bigint
        END
    ),
    schema_version = COALESCE(
        schema_version,
        CASE
            WHEN data->>'schema_version' ~ '^-?[0-9]+$'
            THEN (data->>'schema_version')::integer
        END
    ),
    condition = COALESCE(condition, NULLIF(data->>'condition', '')),
    course_id = COALESCE(course_id, NULLIF(data->>'course_id', '')),
    task_id = COALESCE(task_id, NULLIF(data->>'task_id', '')),
    session_id = COALESCE(session_id, NULLIF(data->>'session_id', '')),
    event_source = COALESCE(event_source, NULLIF(data->>'event_source', '')),
    language_id = COALESCE(
        language_id,
        NULLIF(data->>'language_id', ''),
        NULLIF(data->>'languageId', '')
    ),
    file_hash = COALESCE(file_hash, NULLIF(data->>'file_hash', '')),
    path_privacy = COALESCE(path_privacy, NULLIF(data->>'path_privacy', '')),
    capture_mode = COALESCE(capture_mode, NULLIF(data->>'capture_mode', '')),
    client_timestamp_ms = COALESCE(
        client_timestamp_ms,
        CASE
            WHEN data->>'client_timestamp_ms' ~ '^-?[0-9]+$'
            THEN (data->>'client_timestamp_ms')::bigint
        END
    ),
    client_tz_offset_min = COALESCE(
        client_tz_offset_min,
        CASE
            WHEN data->>'client_tz_offset_min' ~ '^-?[0-9]+$'
            THEN (data->>'client_tz_offset_min')::integer
        END
    ),
    server_timestamp_ms = COALESCE(
        server_timestamp_ms,
        CASE
            WHEN data->>'server_timestamp_ms' ~ '^-?[0-9]+$'
            THEN (data->>'server_timestamp_ms')::bigint
            ELSE floor(extract(epoch from "timestamp") * 1000)::bigint
        END
    );

CREATE INDEX IF NOT EXISTS events_timestamp_idx
    ON public.events ("timestamp");

CREATE INDEX IF NOT EXISTS events_type_timestamp_idx
    ON public.events (event_type, "timestamp");

CREATE INDEX IF NOT EXISTS events_participant_session_idx
    ON public.events (user_id, assignment_id, session_id, task_id);

CREATE INDEX IF NOT EXISTS events_file_hash_idx
    ON public.events (file_hash)
    WHERE file_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS events_event_id_idx
    ON public.events (event_id)
    WHERE event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS events_data_gin_idx
    ON public.events USING GIN (data);

COMMENT ON TABLE public.events IS
    'CodeChat dissertation capture events with typed analysis metadata and event-specific JSONB payloads.';
COMMENT ON COLUMN public.events.user_id IS 'Participant identifier supplied by capture settings.';
COMMENT ON COLUMN public.events.assignment_id IS 'Assignment or lab identifier supplied by capture settings.';
COMMENT ON COLUMN public.events.group_id IS 'Study group, section, or cohort identifier.';
COMMENT ON COLUMN public.events.condition IS 'Study condition, such as treatment, comparison, or capture-only.';
COMMENT ON COLUMN public.events.session_id IS 'Capture session UUID emitted by the VS Code extension.';
COMMENT ON COLUMN public.events.file_hash IS 'SHA-256 hash of the file path when path hashing is enabled.';
COMMENT ON COLUMN public.events.file_path IS 'Raw captured file path; NULL when path hashing is enabled.';
COMMENT ON COLUMN public.events.data IS 'Event-specific JSON payload. Duplicates typed metadata for portable fallback exports.';

COMMIT;
