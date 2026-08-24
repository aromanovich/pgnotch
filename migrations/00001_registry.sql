-- The versioned half of the schema: everything that exists once rather than
-- once per log. The per-log entry tables are created by CreateLogs and cannot
-- be versioned — there is no bound on how many of them there are.
--
-- Every name here is unqualified, so it lands in whatever schema the
-- connection's search_path names. That is the whole of how one deployment's
-- logs are kept apart from another's.

-- +goose Up
CREATE TABLE pgnotch_logs (
    log_id     text     PRIMARY KEY,
    -- UNIQUE rather than decoration: reclamation re-reads the row by ordinal,
    -- having reached the log through the table names it built from one, and a
    -- registry with many logs in it would otherwise be scanned.
    ordinal    bigint   NOT NULL GENERATED ALWAYS AS IDENTITY UNIQUE,
    epoch      bigint   NOT NULL,
    last_seqno bigint   NOT NULL,
    trim_upto  bigint   NOT NULL,
    cur_slot   smallint NOT NULL,
    cur_lo     bigint   NOT NULL,
    prev_hi    bigint   NOT NULL
)
-- fillfactor is 70 here and left alone on the entry tables, and the difference
-- is the point: this row is UPDATEd on every append, so it wants room on its
-- page for a HOT version and its predecessor, while an entry row is written
-- once and never again.
WITH (fillfactor = 70);

-- +goose Down
-- The entry tables go with it. They are unbounded in number, so they can only
-- be found through the registry — and a down that dropped the registry first
-- would strand them where nothing could ever name them again.
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
