//! Integration tests for the v2 `wxyc_library` hook (E1 §4.1.3 of
//! `plans/library-hook-canonicalization.md`).
//!
//! Validates the migration `0002_wxyc_library_v2.sql` (mirrored into
//! `schema/create_database.sql`) and the matching loader in
//! `src/wxyc_loader.rs`. Per the wiki §4.1.3, this cache is small and
//! schema-validation only — there is no in-repo legacy predecessor, so the
//! loader is verified against the input fixture's row count rather than a
//! parity comparator. Modeled after `discogs-etl` PR #185's 5-test suite.
//!
//! Like `tests/import_test.rs`, these tests are not gated behind `#[ignore]`
//! — they expect a PostgreSQL instance running on localhost:5435 with
//!   user=wikidata, password=wikidata, dbname=wikidata_test
//! Start with: docker compose up -d
//! Run with: cargo test --test wxyc_library_v2_test

use postgres::{Client, NoTls};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use wikidata_cache::import_schema;
use wikidata_cache::wxyc_loader::{
    self, ALLOWED_SNAPSHOT_SOURCES, NORMALIZER_NAME, populate_wxyc_library_v2,
};

const TEST_DB_URL: &str =
    "host=localhost port=5435 user=wikidata password=wikidata dbname=wikidata_test";

/// Serialize all database tests on this binary to avoid race conditions on
/// the shared test database. Mirrors the pattern in `tests/import_test.rs`.
static DB_LOCK: Mutex<()> = Mutex::new(());

fn lock_db() -> MutexGuard<'static, ()> {
    DB_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn test_client() -> Client {
    Client::connect(TEST_DB_URL, NoTls)
        .expect("Failed to connect to test database. Is `docker compose up -d` running?")
}

/// Drop and re-create the schema (which now includes `wxyc_library`).
fn fresh_schema(client: &mut Client) {
    import_schema::drop_schema(client).unwrap();
    import_schema::create_schema(client).unwrap();
}

/// Canonical fixture matching `discogs-etl/tests/integration/test_wxyc_library_v2.py`.
/// Uses WXYC-representative artists per the org-level CLAUDE.md "Example
/// Music Data" guidance. Row 6 (Nilüfer Yanya) carries the diacritic
/// canary — the normalizer pin test asserts ü is folded.
const FIXTURE_ROWS: &[(i64, &str, &str, &str, &str, &str)] = &[
    (1, "Juana Molina", "DOGA", "LP", "Sonamos", "Rock"),
    (
        2,
        "Jessica Pratt",
        "On Your Own Love Again",
        "LP",
        "Drag City",
        "Rock",
    ),
    (
        3,
        "Chuquimamani-Condori",
        "Edits",
        "CD",
        "self-released",
        "Electronic",
    ),
    (
        4,
        "Duke Ellington & John Coltrane",
        "Duke Ellington & John Coltrane",
        "LP",
        "Impulse Records",
        "Jazz",
    ),
    (5, "Stereolab", "Aluminum Tunes", "CD", "Duophonic", "Rock"),
    // Diacritic-bearing canonical name from `wxycCanonicalArtistNames`.
    (6, "Nilüfer Yanya", "Painless", "LP", "ATO Records", "Rock"),
];

/// Build a tiny `library.db` SQLite file with the canonical fixture rows.
/// Returns the temp dir and the path so the dir lives long enough.
fn build_library_db(dir: &Path) -> PathBuf {
    let db_path = dir.join("library.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE library (
             id INTEGER PRIMARY KEY,
             artist TEXT NOT NULL,
             title TEXT NOT NULL,
             format TEXT,
             label TEXT,
             genre TEXT
         );",
    )
    .unwrap();
    let mut stmt = conn
        .prepare(
            "INSERT INTO library (id, artist, title, format, label, genre) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .unwrap();
    for (id, artist, title, format, label, genre) in FIXTURE_ROWS {
        stmt.execute(rusqlite::params![id, artist, title, format, label, genre])
            .unwrap();
    }
    db_path
}

// ---------------------------------------------------------------------------
// 1. Schema lands all 8 indexes.
// ---------------------------------------------------------------------------

#[test]
fn test_migration_creates_wxyc_library_with_indexes() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    // §3.1 names 8 indexes — pkey + 5 b-tree + 2 GIN trgm. The pkey index
    // is auto-named `wxyc_library_pkey` by Postgres.
    let expected: &[&str] = &[
        "wxyc_library_pkey",
        "wxyc_library_norm_artist_idx",
        "wxyc_library_norm_title_idx",
        "wxyc_library_artist_id_idx",
        "wxyc_library_format_id_idx",
        "wxyc_library_release_year_idx",
        "wxyc_library_norm_artist_trgm_idx",
        "wxyc_library_norm_title_trgm_idx",
    ];

    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = 'public' AND tablename = 'wxyc_library'",
            &[],
        )
        .unwrap();
    let present: std::collections::HashSet<String> =
        rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for idx in expected {
        assert!(
            present.contains(*idx),
            "index '{idx}' missing after migration; present: {present:?}"
        );
    }
    assert_eq!(
        present.len(),
        expected.len(),
        "expected exactly {} indexes on wxyc_library; got {}: {:?}",
        expected.len(),
        present.len(),
        present
    );
}

// ---------------------------------------------------------------------------
// 2. Loader writes every fixture row.
// ---------------------------------------------------------------------------

#[test]
fn test_v2_loader_writes_every_fixture_row() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let tmp = tempfile::tempdir().unwrap();
    let library_db = build_library_db(tmp.path());

    let attempted = populate_wxyc_library_v2(&mut client, &library_db, "backend").unwrap();
    assert_eq!(attempted as usize, FIXTURE_ROWS.len());

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM wxyc_library", &[])
        .unwrap()
        .get(0);
    assert_eq!(count as usize, FIXTURE_ROWS.len());

    // Every fixture library_id must be present, with populated norm_artist /
    // norm_title and snapshot_source = 'backend'.
    for (id, _, _, _, _, _) in FIXTURE_ROWS {
        let row = client
            .query_one(
                "SELECT artist_name, album_title, norm_artist, norm_title, snapshot_source \
                 FROM wxyc_library WHERE library_id = $1",
                &[&(*id as i32)],
            )
            .unwrap();
        let artist_name: &str = row.get(0);
        let album_title: &str = row.get(1);
        let norm_artist: &str = row.get(2);
        let norm_title: &str = row.get(3);
        let snapshot_source: &str = row.get(4);
        assert!(!artist_name.is_empty(), "artist_name empty for id={id}");
        assert!(!album_title.is_empty(), "album_title empty for id={id}");
        assert!(!norm_artist.is_empty(), "norm_artist empty for id={id}");
        assert!(!norm_title.is_empty(), "norm_title empty for id={id}");
        assert_eq!(snapshot_source, "backend", "wrong source for id={id}");
    }
}

// ---------------------------------------------------------------------------
// 3. Loader is idempotent on re-run.
// ---------------------------------------------------------------------------

#[test]
fn test_v2_loader_is_idempotent() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let tmp = tempfile::tempdir().unwrap();
    let library_db = build_library_db(tmp.path());

    // ON CONFLICT DO NOTHING means both calls report rows-attempted, not
    // rows-inserted; idempotency is observable in COUNT(*).
    let first = populate_wxyc_library_v2(&mut client, &library_db, "backend").unwrap();
    let second = populate_wxyc_library_v2(&mut client, &library_db, "backend").unwrap();
    assert_eq!(first, second);
    assert_eq!(first as usize, FIXTURE_ROWS.len());

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM wxyc_library", &[])
        .unwrap()
        .get(0);
    assert_eq!(count as usize, FIXTURE_ROWS.len());
}

// ---------------------------------------------------------------------------
// 4. Loader rejects invalid snapshot_source (mirrors §3.1 CHECK constraint).
// ---------------------------------------------------------------------------

#[test]
fn test_v2_loader_rejects_invalid_snapshot_source() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let tmp = tempfile::tempdir().unwrap();
    let library_db = build_library_db(tmp.path());

    let err = populate_wxyc_library_v2(&mut client, &library_db, "bogus").unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("snapshot_source"),
        "error message should mention snapshot_source; got: {msg}"
    );

    // Sanity: the constant pinning what's allowed.
    assert_eq!(ALLOWED_SNAPSHOT_SOURCES, &["backend", "tubafrenzy", "llm"]);
}

// ---------------------------------------------------------------------------
// 5. Normalizer is locked to `to_identity_match_form` (no algorithm drift).
// ---------------------------------------------------------------------------

#[test]
fn test_normalizer_is_to_identity_match_form() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let tmp = tempfile::tempdir().unwrap();
    let library_db = build_library_db(tmp.path());

    populate_wxyc_library_v2(&mut client, &library_db, "backend").unwrap();

    // The audit string names the locked-on baseline.
    assert_eq!(NORMALIZER_NAME, "wxyc_etl::text::to_identity_match_form");

    // Hard-coded value pin: catches algorithm drift in `wxyc-etl`. Library
    // row 1 is "Juana Molina" / "DOGA" / "Sonamos" — no diacritics, no
    // leading articles, just lowercasing.
    let row = client
        .query_one(
            "SELECT norm_artist, norm_title, norm_label \
             FROM wxyc_library WHERE library_id = 1",
            &[],
        )
        .unwrap();
    let norm_artist: &str = row.get(0);
    let norm_title: &str = row.get(1);
    let norm_label: &str = row.get(2);
    assert_eq!(norm_artist, "juana molina");
    assert_eq!(norm_title, "doga");
    assert_eq!(norm_label, "sonamos");

    // Equality with the canonical functions — robust to other normalization
    // changes that don't affect these particular inputs.
    assert_eq!(
        norm_artist,
        wxyc_etl::text::to_identity_match_form("Juana Molina")
    );
    assert_eq!(
        norm_title,
        wxyc_etl::text::to_identity_match_form_title("DOGA")
    );
    assert_eq!(
        norm_label,
        wxyc_etl::text::to_identity_match_form("Sonamos")
    );

    // Diacritic-fold pin: row 6 has ü which must fold to u in storage.
    let row = client
        .query_one(
            "SELECT norm_artist FROM wxyc_library WHERE library_id = 6",
            &[],
        )
        .unwrap();
    let norm_a_diacritic: &str = row.get(0);
    assert_eq!(
        norm_a_diacritic, "nilufer yanya",
        "Nilüfer Yanya did not fold to ASCII as expected: {norm_a_diacritic:?}"
    );
    assert!(
        !norm_a_diacritic.contains('ü'),
        "diacritic survived normalization: {norm_a_diacritic:?}"
    );

    // Re-bind so the unused-import lint doesn't fire.
    let _ = wxyc_loader::ALLOWED_SNAPSHOT_SOURCES;
}
