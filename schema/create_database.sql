-- wikidata-cache PostgreSQL schema
-- Creates tables for music-relevant Wikidata entities extracted by wikidata-cache.
-- Compatible with the 8 CSV files produced by wikidata-cache's writer module.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS entity (
    qid TEXT PRIMARY KEY,
    label TEXT,
    description TEXT,
    entity_type TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(entity_type);
CREATE INDEX IF NOT EXISTS idx_entity_label_trgm ON entity USING gin(label gin_trgm_ops);

CREATE TABLE IF NOT EXISTS discogs_mapping (
    qid TEXT NOT NULL REFERENCES entity(qid),
    property TEXT NOT NULL,
    discogs_id TEXT NOT NULL,
    PRIMARY KEY (qid, property, discogs_id)
);

CREATE INDEX IF NOT EXISTS idx_discogs_mapping_property_id ON discogs_mapping(property, discogs_id);

CREATE TABLE IF NOT EXISTS influence (
    source_qid TEXT NOT NULL REFERENCES entity(qid),
    target_qid TEXT NOT NULL,
    PRIMARY KEY (source_qid, target_qid)
);

CREATE INDEX IF NOT EXISTS idx_influence_target ON influence(target_qid);

CREATE TABLE IF NOT EXISTS genre (
    entity_qid TEXT NOT NULL REFERENCES entity(qid),
    genre_qid TEXT NOT NULL,
    PRIMARY KEY (entity_qid, genre_qid)
);

CREATE TABLE IF NOT EXISTS record_label (
    artist_qid TEXT NOT NULL REFERENCES entity(qid),
    label_qid TEXT NOT NULL,
    PRIMARY KEY (artist_qid, label_qid)
);

CREATE TABLE IF NOT EXISTS label_hierarchy (
    child_qid TEXT NOT NULL,
    parent_qid TEXT NOT NULL,
    PRIMARY KEY (child_qid, parent_qid)
);

CREATE TABLE IF NOT EXISTS entity_alias (
    qid TEXT NOT NULL REFERENCES entity(qid),
    alias TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entity_alias_qid ON entity_alias(qid);
CREATE INDEX IF NOT EXISTS idx_entity_alias_text_trgm ON entity_alias USING gin(alias gin_trgm_ops);

CREATE TABLE IF NOT EXISTS occupation (
    entity_qid TEXT NOT NULL REFERENCES entity(qid),
    occupation_qid TEXT NOT NULL,
    PRIMARY KEY (entity_qid, occupation_qid)
);

-- wxyc_library v2 hook (consolidated cross-cache identity schema). Mirrored
-- from migrations/0002_wxyc_library_v2.sql per the dual-source pattern in
-- CLAUDE.md "Migrations". Per E1 §4.1.3 of the cross-cache-identity plan,
-- this cache is small enough that every index is built inline.
-- See: https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#413-homebrew-wikidata-port-5435
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
CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_trgm_idx
    ON wxyc_library USING GIN (norm_artist gin_trgm_ops);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_trgm_idx
    ON wxyc_library USING GIN (norm_title gin_trgm_ops);
