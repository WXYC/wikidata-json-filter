# Claude Code Instructions for wikidata-json-filter

## Project Overview

Purpose-built Rust tool for filtering Wikidata JSON data dumps to music-relevant entities, producing CSV files compatible with the [wikidata-cache](https://github.com/WXYC/wikidata-cache) ETL pipeline. Also provides an `import` subcommand to load those CSVs into PostgreSQL, creating the wikidata-cache database. Analogous to [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) for Discogs data.

## Architecture

### Modules

- `model.rs` -- Data structures for Wikidata JSON entities. Only fields needed for filtering and extraction are modeled; everything else is skipped during deserialization via `serde`. Key types: `Entity`, `Statement`, `Snak`, `DataValue`.
- `filter.rs` -- Music-relevance filter. Primary indicators: P1953 (Discogs artist ID), P1902 (Spotify artist ID), P106 (musician occupation), P31 (musical group / record label). Secondary properties (P737, P136, P264, P749, P2850, P3283) are extracted but don't independently qualify entities.
- `extractor.rs` -- Extracts flat CSV rows from matched entities. Classifies entity type (human/group/label/other) from P31/P106 claims. Produces rows for 8 output tables. Extracts external IDs (P1953 Discogs, P434 MusicBrainz, P1902 Spotify, P2850 Apple Music, P3283 Bandcamp) into `discogs_mapping.csv`.
- `writer.rs` -- `CsvOutput` wraps `wxyc_etl::csv_writer::MultiCsvWriter` for 8 CSV files with headers matching the wikidata-cache PostgreSQL schema. Implements `wxyc_etl::pipeline::PipelineOutput<ExtractedRows>`. The `csv_file_specs()` function defines the 8-file spec.
- `import.rs` -- CSV import module. Reads the 8 CSV files and streams them into PostgreSQL via COPY TEXT. Handles RFC 4180 quoted fields, Unicode, and empty CSVs.
- `import_schema.rs` -- PostgreSQL schema management. Embeds and applies `schema/create_database.sql`. Provides UNLOGGED/LOGGED toggle and VACUUM FULL for bulk import performance. Table constants define FK-safe import order.
- `main.rs` -- CLI (clap derive) with subcommand architecture. Default mode runs the three-stage filter pipeline via `wxyc_etl::pipeline`; `import` subcommand loads CSVs into PostgreSQL.

### Parallel Processing Pipeline

Uses `wxyc_etl::pipeline` framework (same three-stage pattern as discogs-xml-converter):

1. **Scanner thread** (`start_scanner`) -- reads the input (gzipped or plain) via `flate2::GzDecoder` + `BufReader`, reads line by line (the Wikidata dump is `[\n{entity},\n{entity},\n...\n]`), strips array brackets and trailing commas, sends raw byte vectors via `BatchSender`. Batch size and channel capacity use `BatchConfig::default()` (256 items, 64 batches).
2. **Rayon worker pool** (`run_pipeline`) -- receives batches, deserializes JSON via `serde_json::from_slice`, applies music-relevance filter, extracts target fields from matched entities. Preserves input order.
3. **Writer** (`PipelineOutput`) -- `CsvOutput` writes extracted rows to 8 CSV files in document order.

No SIMD byte scanning needed (unlike discogs-xml-converter) because entity boundaries are newlines.

### CSV Output Contract

The 8 output CSV files must be compatible with `wikidata-cache/scripts/import_csv.py`. Headers and column order are defined in `writer.rs`. Changes to the CSV schema require coordinating with wikidata-cache.

| File | Columns |
|------|---------|
| `entity.csv` | qid, label, description, entity_type |
| `discogs_mapping.csv` | qid, property, discogs_id |
| `influence.csv` | source_qid, target_qid |
| `genre.csv` | entity_qid, genre_qid |
| `record_label.csv` | artist_qid, label_qid |
| `label_hierarchy.csv` | child_qid, parent_qid |
| `entity_alias.csv` | qid, alias |
| `occupation.csv` | entity_qid, occupation_qid |

### Import Subcommand

The `import` subcommand loads the 8 CSV files into PostgreSQL:

1. Creates the schema (idempotent with `IF NOT EXISTS`)
2. Sets tables to UNLOGGED for faster bulk import
3. Truncates existing data
4. Streams each CSV via COPY TEXT in FK order (entity first, then child tables)
5. Restores tables to LOGGED
6. Runs VACUUM FULL

The `--fresh` flag drops and recreates the schema before importing.

### PostgreSQL Schema

Defined in `schema/create_database.sql` and embedded via `include_str!` in `import_schema.rs`. The schema uses pg_trgm for trigram indexes on `entity.label` and `entity_alias.alias` for fuzzy text search. FK constraints enforce referential integrity from child tables to `entity.qid`, except `influence.target_qid` and `label_hierarchy` which allow dangling references (the target entity may have been filtered out).

### Filter Criteria

An entity is music-relevant if it has ANY primary indicator (each sufficient on its own):
- **P1953** (Discogs artist ID) -- entity has a Discogs page
- **P1902** (Discogs label ID) -- record label with a Discogs page
- **P106** (occupation) with a musician-related QID. The full set is defined in `filter.rs::MUSICIAN_QIDS`.
- **P31** (instance of) with a musical group or record label QID. The full set is defined in `filter.rs::MUSICAL_GROUP_QIDS`.

Secondary properties (P737 influence, P136 genre, P264 record label, P749 parent org, P2850 Apple Music artist ID, P3283 Bandcamp profile ID) are extracted only from entities that pass the primary filter. They don't independently qualify an entity.

## Development

### TDD (Required)

All code changes follow test-driven development. No production code without a failing test first.

### Testing

```bash
cargo test              # all tests (unit + CLI + oracle + PG skipped without DB)
cargo test --lib        # unit tests only
cargo test --test cli_tests    # CLI integration tests only
cargo test --test import_test  # PostgreSQL import tests (requires docker compose up -d)

# PostgreSQL integration tests (requires TEST_DATABASE_URL)
TEST_DATABASE_URL=postgresql://musicbrainz:musicbrainz@localhost:5434/postgres \
  cargo test --test pg_import_test
```

- **Unit tests** (26): JSON parsing, filter logic, extractor, CSV output, pipeline output trait.
- **CLI tests** (4): End-to-end binary invocation with small_dump.json fixture.
- **Oracle tests** (9): CSV output diffed against expected baselines in `tests/fixtures/expected/`.
- **PG import tests** (13): Full filter -> CSV -> PG import -> query chain. Trigram search on entity names and aliases. Discogs/MusicBrainz ID lookup via indexes. Gated on `TEST_DATABASE_URL`.
- **Import integration tests**: Require PostgreSQL on port 5435 (started via `docker compose up -d`). Cover schema creation, CSV import for all 8 tables, FK integrity, Unicode handling, and end-to-end pipeline validation.

### Build

```bash
cargo build --release   # produces target/release/wikidata-json-filter
cargo install --path .  # installs to ~/.cargo/bin/
```

### Code Style

- `cargo fmt` for formatting
- `cargo clippy` for linting
- Targets macOS ARM64 and Linux x86_64

## Key Design Decisions

- Entity boundaries are newlines, so no byte scanning is needed (unlike discogs-xml-converter's SIMD `memchr` approach for XML element boundaries)
- `serde_json::from_slice` is used for JSON parsing; `simd-json` is a potential optimization if parsing becomes the bottleneck
- `par_iter().map().collect()` preserves input order so CSV output is deterministic regardless of thread scheduling
- Bounded channel (capacity 64 batches of 256 entities) provides backpressure to prevent unbounded memory growth
- Only English labels, descriptions, and aliases are extracted (`labels.en`, `descriptions.en`, `aliases.en`)
- Entity type classification priority: record label > musical group > human > other (checked in that order from P31/P106)
- The `DataValue` enum uses `#[serde(other)]` to silently skip unknown value types (time, quantity, coordinates, etc.) without failing deserialization
