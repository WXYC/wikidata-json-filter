# Claude Code Instructions for wikidata-json-filter

## Project Overview

Purpose-built Rust tool for filtering Wikidata JSON data dumps to music-relevant entities, producing CSV files compatible with the [wikidata-cache](https://github.com/WXYC/wikidata-cache) ETL pipeline. Analogous to [discogs-xml-converter](https://github.com/WXYC/discogs-xml-converter) for Discogs data.

## Architecture

### Modules

- `model.rs` -- Data structures for Wikidata JSON entities. Only fields needed for filtering and extraction are modeled; everything else is skipped during deserialization via `serde`. Key types: `Entity`, `Statement`, `Snak`, `DataValue`.
- `filter.rs` -- Music-relevance filter. Primary indicators: P1953 (Discogs artist ID), P1902 (Spotify artist ID), P106 (musician occupation), P31 (musical group / record label). Secondary properties (P737, P136, P264, P749, P2850, P3283) are extracted but don't independently qualify entities.
- `extractor.rs` -- Extracts flat CSV rows from matched entities. Classifies entity type (human/group/label/other) from P31/P106 claims. Produces rows for 8 output tables. Extracts external IDs (P1953 Discogs, P434 MusicBrainz, P1902 Spotify, P2850 Apple Music, P3283 Bandcamp) into `discogs_mapping.csv`.
- `writer.rs` -- `CsvOutput` writes 8 CSV files with headers matching the wikidata-cache PostgreSQL schema.
- `main.rs` -- CLI (clap derive) and three-stage pipeline.

### Parallel Processing Pipeline

Same three-stage pattern as discogs-xml-converter:

1. **Reader thread** -- reads the input (gzipped or plain) via `flate2::GzDecoder` + `BufReader`, reads line by line (the Wikidata dump is `[\n{entity},\n{entity},\n...\n]`), strips array brackets and trailing commas, batches raw byte vectors (256 per batch), sends via bounded crossbeam channel (capacity 64).
2. **Rayon worker pool** -- receives batches, deserializes JSON via `serde_json::from_slice`, applies music-relevance filter, extracts target fields from matched entities. `par_iter()` preserves input order.
3. **Writer (main thread)** -- writes extracted rows to 8 CSV files in document order.

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
cargo test          # all tests (unit + CLI integration)
cargo test --lib    # unit tests only
```

Unit tests use hand-crafted JSON fixtures in each module. CLI integration tests use `tests/fixtures/small_dump.json` (5 entities: 3 music-relevant, 2 non-music).

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
