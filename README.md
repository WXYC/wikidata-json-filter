# wikidata-cache

Streaming Rust filter for [Wikidata JSON data dumps](https://www.wikidata.org/wiki/Wikidata:Database_download). Extracts music-relevant entities (artists, bands, record labels) and writes flat CSV files, then loads them into PostgreSQL to create the wikidata-cache database.

Analogous to [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) for Discogs data.

## Usage

```bash
# Filter the full Wikidata dump (~130GB gzipped, ~3 hours)
wikidata-cache latest-all.json.gz --output-dir /path/to/csv/

# Limit entities for testing
wikidata-cache latest-all.json.gz --output-dir /tmp/test/ --limit 1000

# Adjust progress logging interval
wikidata-cache latest-all.json.gz --output-dir /path/to/csv/ --progress-interval 500000
```

Gzipped input is auto-detected by `.gz` extension.

### Options

| Flag | Description |
|------|-------------|
| `--output-dir DIR` | Output directory for CSV files (default: `output`) |
| `--limit N` | Stop after N entities, 0 = no limit (default: 0) |
| `--progress-interval N` | Log progress every N entities (default: 1000000) |

## What gets filtered

An entity is included if it has any of these properties:

| Property | Meaning | Example |
|----------|---------|---------|
| P1953 | Discogs artist ID | Autechre has Discogs ID 12 |
| P1902 | Discogs label ID | Warp Records has Discogs label ID 23528 |
| P106 | Occupation = musician, singer, DJ, composer, etc. | |
| P31 | Instance of musical group, band, or record label | |

From matched entities, the filter also extracts: P737 (influenced by), P136 (genre), P264 (record label), P749 (parent organization), P434 (MusicBrainz artist ID), P2850 (Apple Music artist ID), P3283 (Bandcamp profile ID), and English aliases.

## CSV Output

Produces 8 CSV files:

| File | Columns | Description |
|------|---------|-------------|
| `entity.csv` | qid, label, description, entity_type | Core entity metadata |
| `discogs_mapping.csv` | qid, property, discogs_id | External IDs (P1953 Discogs, P1902 Spotify, P434 MusicBrainz, P2850 Apple Music, P3283 Bandcamp) |
| `influence.csv` | source_qid, target_qid | P737 influence relationships |
| `genre.csv` | entity_qid, genre_qid | P136 genre claims |
| `record_label.csv` | artist_qid, label_qid | P264 record label claims |
| `label_hierarchy.csv` | child_qid, parent_qid | P749 parent organization |
| `entity_alias.csv` | qid, alias | English language aliases |
| `occupation.csv` | entity_qid, occupation_qid | P106 occupation claims |

## Import into PostgreSQL

The `import` subcommand loads the CSV output directly into PostgreSQL:

```bash
# Import CSVs into PostgreSQL (creates schema, imports data, runs VACUUM)
wikidata-cache import --csv-dir /path/to/csv/ --database-url 'host=localhost dbname=wikidata user=wikidata password=wikidata'

# Or use DATABASE_URL environment variable
export DATABASE_URL='host=localhost dbname=wikidata user=wikidata password=wikidata'
wikidata-cache import --csv-dir /path/to/csv/

# Drop and recreate schema before importing
wikidata-cache import --csv-dir /path/to/csv/ --database-url '...' --fresh
```

The import:
1. Creates the schema (idempotent with `IF NOT EXISTS`)
2. Sets tables to UNLOGGED for faster bulk loading
3. Truncates existing data
4. Streams each CSV via PostgreSQL COPY in FK order
5. Restores tables to LOGGED
6. Runs VACUUM FULL

### PostgreSQL Schema

The schema is defined in `schema/create_database.sql` and includes pg_trgm indexes for fuzzy text search on entity labels and aliases. See that file for the full DDL.

## Performance

Processing is parallelized across all CPU cores:

1. A **reader thread** decompresses the gzipped input on the fly and reads line by line
2. A **rayon worker pool** parses JSON and filters entities in parallel
3. The **main thread** writes matched entities to CSV sequentially, preserving order

Expected throughput: ~10K entities/second. The full Wikidata dump (~110M entities) takes roughly 3 hours. Output is ~500MB-1GB of CSV for ~2-5M music entities.

## Building

Requires the [Rust toolchain](https://rustup.rs/).

```bash
cargo build --release
cargo install --path .
```

## Testing

```bash
cargo test --lib        # unit tests only (no external deps)
cargo test --test cli_tests    # CLI integration tests

# Import integration tests require PostgreSQL:
docker compose up -d    # starts PostgreSQL on port 5435
cargo test --test import_test

# Run everything:
docker compose up -d && cargo test
```

Unit and CLI tests use hand-written JSON fixtures; no external data dumps needed. Import tests require a PostgreSQL instance (provided by docker-compose.yml on port 5435).

## Full Pipeline

```bash
# 1. Download the Wikidata dump
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz

# 2. Filter to music entities
wikidata-cache latest-all.json.gz --output-dir /path/to/csv/

# 3. Load into PostgreSQL
wikidata-cache import --csv-dir /path/to/csv/ --database-url 'host=localhost dbname=wikidata user=wikidata password=wikidata' --fresh
```

## Data source

Wikidata JSON dumps are published weekly at https://dumps.wikimedia.org/wikidatawiki/entities/. The latest dump is available at `latest-all.json.gz` (~130GB).
