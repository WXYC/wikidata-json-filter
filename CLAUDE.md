# Claude Code Instructions for wikidata-cache

## Project Overview

Purpose-built Rust tool that builds the WXYC `wikidata-cache` PostgreSQL database from Wikidata JSON dumps. Two subcommands matching the standardized WXYC cache-builder CLI shape (`wxyc_etl::cli`): `build` streams a (gzipped) Wikidata JSON dump and writes 8 CSV files of music-relevant entities to `--data-dir`; `import` loads those CSVs into PostgreSQL. Analogous to [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) for Discogs data and [musicbrainz-cache](https://github.com/WXYC/musicbrainz-cache) for MusicBrainz.

## Architecture

### Modules

- `model.rs` -- Data structures for Wikidata JSON entities. Only fields needed for filtering and extraction are modeled; everything else is skipped during deserialization via `serde`. Key types: `Entity`, `Statement`, `Snak`, `DataValue`.
- `filter.rs` -- Music-relevance filter. Primary indicators: P1953 (Discogs artist ID), P1902 (Spotify artist ID), P106 (musician occupation), P31 (musical group / record label). Secondary properties (P737, P136, P264, P749, P2850, P3283) are extracted but don't independently qualify entities.
- `extractor.rs` -- Extracts flat CSV rows from matched entities. Classifies entity type (human/group/label/other) from P31/P106 claims. Produces rows for 8 output tables. Extracts external IDs (P1953 Discogs, P434 MusicBrainz, P1902 Spotify, P2850 Apple Music, P3283 Bandcamp) into `discogs_mapping.csv`.
- `writer.rs` -- `CsvOutput` wraps `wxyc_etl::csv_writer::MultiCsvWriter` for 8 CSV files with headers matching the wikidata-cache PostgreSQL schema. Implements `wxyc_etl::pipeline::PipelineOutput<ExtractedRows>`. The `csv_file_specs()` function defines the 8-file spec.
- `import.rs` -- CSV import module. Reads the 8 CSV files and streams them into PostgreSQL via COPY TEXT. Handles RFC 4180 quoted fields, Unicode, and empty CSVs.
- `import_schema.rs` -- PostgreSQL schema management. Embeds and applies `schema/create_database.sql`. Provides UNLOGGED/LOGGED toggle and VACUUM FULL for bulk import performance. Table constants define FK-safe import order.
<<<<<<< HEAD
- `main.rs` -- CLI (clap derive) with subcommand architecture. Default mode runs the three-stage filter pipeline via `wxyc_etl::pipeline`; `import` subcommand loads CSVs into PostgreSQL. Initializes `wxyc_etl::logger` (Sentry + JSON logs) at startup and wraps each subcommand in a tracing span tagged `repo`/`tool`/`step`.

### Observability

The binary uses `wxyc_etl::logger::init` to set up structured JSON logging on stdout and (when `SENTRY_DSN` is set) panic/error forwarding to Sentry. Every log line and Sentry event carries the four standard ETL tags:

| Tag | Value |
|-----|-------|
| `repo` | `wikidata-cache` |
| `tool` | `wikidata-cache build` or `wikidata-cache import` |
| `step` | `build` or `import` (the active subcommand) |
| `run_id` | UUIDv4 generated per process |

`SENTRY_DSN` is optional; without it, JSON logging still works and Sentry stays inactive. Provisioning the DSN in deploy environments (CI, Railway, etc.) is tracked separately.
=======
- `main.rs` -- CLI (clap derive) using shared argument groups from `wxyc_etl::cli` (`DatabaseArgs`, `ResumableBuildArgs`, `ImportArgs`). The `build` subcommand runs the three-stage filter pipeline via `wxyc_etl::pipeline`; the `import` subcommand loads CSVs into PostgreSQL. `--database-url` falls back to `DATABASE_URL_WIKIDATA` via `wxyc_etl::cli::resolve_database_url`. `--output-dir` (build) and `--csv-dir` (import) are accepted as deprecated aliases for `--data-dir` with a stderr warning.
>>>>>>> 6cffa05 (Migrate CLI to standardized cache-builder shape)

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

### Subcommands

The CLI matches the standard WXYC cache-builder shape:

- **`wikidata-cache build INPUT [--data-dir DIR] [--limit N] [--progress-interval N] [--gzip] [--resume] [--state-file FILE]`** — streams the JSON dump and writes the 8 CSV files. `--resume`/`--state-file` come from `wxyc_etl::cli::ResumableBuildArgs`; the streaming filter is idempotent so they are accepted but currently no-ops.
- **`wikidata-cache import [--data-dir DIR] [--database-url URL] [--fresh]`** — loads the 8 CSV files into PostgreSQL. The connection URL falls back to the `DATABASE_URL_WIKIDATA` env var via `wxyc_etl::cli::resolve_database_url`.

`--output-dir` (build) and `--csv-dir` (import) are accepted for one release as deprecated aliases for `--data-dir` and emit a stderr warning.

The `import` subcommand:

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

## Scheduling

The full rebuild (`build` then `import --fresh`) is scheduled via `.github/workflows/rebuild-cache.yml`, which fires at 06:00 UTC on the 10th of each month and also exposes a `workflow_dispatch` trigger for ad-hoc runs (with an optional `dump_url` input). The workflow expects a `DATABASE_URL_WIKIDATA` repository secret pointing at the destination cache. The 10th-of-the-month cron is staggered against the sister cache rebuilds (`discogs-etl`, `musicbrainz-cache`) so no two multi-hour rebuilds co-run.

**Runner-capacity caveat:** the Wikidata JSON dump is roughly 130GB gzipped and a full rebuild can take many hours. GitHub-hosted `ubuntu-latest` runners have a 6-hour job timeout and only ~14GB of free disk, so the scheduled run will likely fail on disk or timeout. The workflow is intentionally a scheduling skeleton — the actual rebuild needs to migrate to a self-hosted runner, a Railway job, or a dedicated EC2 box. Until then, treat the `workflow_dispatch` trigger as the supported path (e.g., for small-dump smoke tests) and run real rebuilds out-of-band.

## Migrations

Schema changes ship as numbered SQL files under `migrations/`, applied with [sqlx-cli](https://crates.io/crates/sqlx-cli). The baseline `migrations/0001_initial.sql` mirrors `schema/create_database.sql`.

Install sqlx-cli once:

```bash
cargo install sqlx-cli --no-default-features --features postgres
```

Add a new migration:

```bash
sqlx migrate add <descriptive_name>
# edits a new migrations/000N_<descriptive_name>.sql; write forward-only SQL
```

Apply against a database (e.g., a fresh local Postgres):

```bash
sqlx migrate run --database-url postgresql://localhost:5435/<db> --source migrations
```

**Runtime path is unchanged.** `src/import_schema.rs::apply_schema()` still reads `schema/create_database.sql` on every fresh import; `sqlx migrate run` is not yet wired into the CLI or the deploy pipeline. Switching the runtime over and stamping production with the baseline version is tracked in [WXYC/wxyc-etl#56](https://github.com/WXYC/wxyc-etl/issues/56). Until that lands, keep `schema/create_database.sql` and `migrations/0001_initial.sql` in sync — any schema change should land in both files in the same PR.

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
- **CLI tests** (9): End-to-end binary invocation with small_dump.json fixture, including env-var fallback for `DATABASE_URL_WIKIDATA` and deprecation warnings for renamed flags.
- **Oracle tests** (9): CSV output diffed against expected baselines in `tests/fixtures/expected/`.
- **PG import tests** (13): Full filter -> CSV -> PG import -> query chain. Trigram search on entity names and aliases. Discogs/MusicBrainz ID lookup via indexes. Gated on `TEST_DATABASE_URL`.
- **Import integration tests**: Require PostgreSQL on port 5435 (started via `docker compose up -d`). Cover schema creation, CSV import for all 8 tables, FK integrity, Unicode handling, and end-to-end pipeline validation.

### Build

```bash
cargo build --release   # produces target/release/wikidata-cache
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
