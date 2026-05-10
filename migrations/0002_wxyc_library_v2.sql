-- wxyc_library v2 hook (consolidated cross-cache identity schema)
--
-- Lands E1 §4.1.3 of the cross-cache-identity plan:
-- https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#413-homebrew-wikidata-port-5435
--
-- Creates the consolidated `wxyc_library` table per §3.1. The wikidata cache
-- is small (~262 MB, ≤64K rows) and `wxyc_library` is loaded once per
-- rebuild, so per §4.1.3 every index — including the GIN trigram indexes —
-- is built INLINE (no CONCURRENTLY), which keeps this a regular sqlx
-- in-transaction migration.
--
-- Idempotency: every statement uses IF NOT EXISTS so re-applying against a
-- populated cache is a no-op (mandated by CLAUDE.md "Idempotency is
-- mandatory" — every monthly rebuild runs `sqlx migrate run` against the
-- destination DB before the rebuild kicks off).
--
-- artist_id / label_id / format_id / release_year are nullable per §3.1:
-- this cache reads from a SQLite library.db (a wxyc-catalog export) which
-- does not carry Backend's integer IDs. They exist for forward compatibility
-- with a future Backend-direct loader.
--
-- snapshot_source CHECK matches the canonical {backend|tubafrenzy|llm} set
-- defined in §3.1; the loader-side argument validation in
-- `src/wxyc_loader.rs` mirrors this.

CREATE TABLE IF NOT EXISTS wxyc_library (
    library_id      INTEGER PRIMARY KEY,
    artist_id       INTEGER,
    artist_name     TEXT    NOT NULL,
    album_title     TEXT    NOT NULL,
    label_id        INTEGER,
    label_name      TEXT,
    format_id       INTEGER,
    format_name     TEXT,
    wxyc_genre      TEXT,
    call_letters    TEXT,
    call_numbers    INTEGER,
    release_year    SMALLINT,
    norm_artist     TEXT    NOT NULL,
    norm_title      TEXT    NOT NULL,
    norm_label      TEXT,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    snapshot_source TEXT    NOT NULL
        CHECK (snapshot_source IN ('backend', 'tubafrenzy', 'llm'))
);

-- B-tree indexes per §3.1. Inline-safe because this cache is small
-- (§4.1.3 explicitly waives the CONCURRENTLY requirement).
CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_idx
    ON wxyc_library (norm_artist);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_idx
    ON wxyc_library (norm_title);
CREATE INDEX IF NOT EXISTS wxyc_library_artist_id_idx
    ON wxyc_library (artist_id);
CREATE INDEX IF NOT EXISTS wxyc_library_format_id_idx
    ON wxyc_library (format_id);
CREATE INDEX IF NOT EXISTS wxyc_library_release_year_idx
    ON wxyc_library (release_year);

-- GIN trigram indexes for fuzzy lookup. pg_trgm is created by 0001_initial.
-- Inline (no CONCURRENTLY) per §4.1.3 — keeps the migration in a single
-- transaction.
CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_trgm_idx
    ON wxyc_library USING GIN (norm_artist gin_trgm_ops);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_trgm_idx
    ON wxyc_library USING GIN (norm_title gin_trgm_ops);
