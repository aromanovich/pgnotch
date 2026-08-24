-- The versioned half of the schema: what exists once rather than once per log.
-- The per-log entry tables are created by CreateLogs and cannot be versioned,
-- since there is no bound on how many of them there are. Every name here is
-- unqualified, so it lands in whatever schema the connection's search_path
-- names, which is how one deployment's logs are kept apart from another's.

-- +goose Up
CREATE TABLE pgnotch_logs (
    log_id     text     PRIMARY KEY,
    -- Reclamation re-reads the row by ordinal, having reached the log through
    -- the table names built from one; UNIQUE keeps that a lookup, not a scan.
    ordinal    bigint   NOT NULL GENERATED ALWAYS AS IDENTITY UNIQUE,
    epoch      bigint   NOT NULL,
    last_seqno bigint   NOT NULL,
    trim_upto  bigint   NOT NULL,
    cur_slot   smallint NOT NULL,
    cur_lo     bigint   NOT NULL,
    prev_hi    bigint   NOT NULL
)
-- This row is UPDATEd on every append, so it wants room on its page for a HOT
-- version and its predecessor; entry rows are written once and left at the
-- default.
WITH (fillfactor = 70);

-- +goose Down
-- The entry tables go with it, and must go first: they can only be found
-- through the registry.
-- +goose StatementBegin
DO $$
DECLARE
    entry_table text;
BEGIN
    FOR entry_table IN
        SELECT format('pgnotch_entries_%s_%s', ordinal, slot)
          FROM pgnotch_logs, generate_series(0, 1) AS slot
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', entry_table);
    END LOOP;
END
$$;
-- +goose StatementEnd
DROP TABLE pgnotch_logs;
