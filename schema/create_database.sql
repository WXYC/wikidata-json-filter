-- wikidata-cache PostgreSQL schema
-- Creates tables for music-relevant Wikidata entities extracted by wikidata-json-filter.
-- Compatible with the 8 CSV files produced by wikidata-json-filter's writer module.

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
