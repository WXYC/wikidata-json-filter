# wikidata-json-filter

Streaming Rust filter for [Wikidata JSON data dumps](https://www.wikidata.org/wiki/Wikidata:Database_download). Extracts music-relevant entities (artists, bands, record labels) and writes flat CSV files for loading into PostgreSQL via the [wikidata-cache](https://github.com/WXYC/wikidata-cache) ETL pipeline.

Analogous to [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) for Discogs data.

## Usage

```bash
# Filter the full Wikidata dump (~130GB gzipped, ~3 hours)
wikidata-json-filter latest-all.json.gz --output-dir /path/to/csv/

# Limit entities for testing
wikidata-json-filter latest-all.json.gz --output-dir /tmp/test/ --limit 1000

# Adjust progress logging interval
wikidata-json-filter latest-all.json.gz --output-dir /path/to/csv/ --progress-interval 500000
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

From matched entities, the filter also extracts: P737 (influenced by), P136 (genre), P264 (record label), P749 (parent organization), and English aliases.

## CSV Output

Produces 8 CSV files:

| File | Columns | Description |
|------|---------|-------------|
| `entity.csv` | qid, label, description, entity_type | Core entity metadata |
| `discogs_mapping.csv` | qid, property, discogs_id | Links to Discogs IDs |
| `influence.csv` | source_qid, target_qid | P737 influence relationships |
| `genre.csv` | entity_qid, genre_qid | P136 genre claims |
| `record_label.csv` | artist_qid, label_qid | P264 record label claims |
| `label_hierarchy.csv` | child_qid, parent_qid | P749 parent organization |
| `entity_alias.csv` | qid, alias | English language aliases |
| `occupation.csv` | entity_qid, occupation_qid | P106 occupation claims |

These are consumed by `wikidata-cache/scripts/import_csv.py`.

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
cargo test
```

All tests use hand-written JSON fixtures; no external data dumps needed.

## Integration with wikidata-cache

Feed the CSV output into the wikidata-cache ETL pipeline:

```bash
# 1. Download the Wikidata dump
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz

# 2. Filter to music entities
wikidata-json-filter latest-all.json.gz --output-dir /path/to/csv/

# 3. Load into PostgreSQL
cd /path/to/wikidata-cache
python scripts/run_pipeline.py --csv-dir /path/to/csv/ --database-url postgresql://wikidata:wikidata@localhost:5434/wikidata
```

## Data source

Wikidata JSON dumps are published weekly at https://dumps.wikimedia.org/wikidatawiki/entities/. The latest dump is available at `latest-all.json.gz` (~130GB).
