//! Integration tests for CSV import into PostgreSQL.
//!
//! Requires a PostgreSQL instance running on localhost:5435 with:
//!   user=wikidata, password=wikidata, dbname=wikidata_test
//!
//! Start with: docker compose up -d
//! Run with: cargo test --test import_test

use assert_cmd::Command;
use postgres::{Client, NoTls};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use wikidata_cache::import;
use wikidata_cache::import_schema;

const TEST_DB_URL: &str =
    "host=localhost port=5435 user=wikidata password=wikidata dbname=wikidata_test";

/// Serialize all database tests to avoid race conditions on the shared test database.
static DB_LOCK: Mutex<()> = Mutex::new(());

fn lock_db() -> MutexGuard<'static, ()> {
    DB_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn test_client() -> Client {
    Client::connect(TEST_DB_URL, NoTls)
        .expect("Failed to connect to test database. Is `docker compose up -d` running?")
}

fn fresh_schema(client: &mut Client) {
    import_schema::drop_schema(client).unwrap();
    import_schema::create_schema(client).unwrap();
}

fn fixture_dir() -> &'static Path {
    Path::new("tests/fixtures/import")
}

// --- Schema validation tests ---

#[test]
fn test_schema_creates_all_tables() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let rows = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
            &[],
        )
        .unwrap();

    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

    for expected in import_schema::ALL_TABLES {
        assert!(
            tables.contains(&expected.to_string()),
            "Table '{expected}' should exist, found: {tables:?}"
        );
    }
}

#[test]
fn test_schema_creates_indexes() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname",
            &[],
        )
        .unwrap();

    let indexes: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

    let expected_indexes = [
        "idx_entity_type",
        "idx_entity_label_trgm",
        "idx_discogs_mapping_property_id",
        "idx_influence_target",
        "idx_entity_alias_qid",
        "idx_entity_alias_text_trgm",
    ];

    for idx in expected_indexes {
        assert!(
            indexes.contains(&idx.to_string()),
            "Index '{idx}' should exist, found: {indexes:?}"
        );
    }
}

#[test]
fn test_schema_fk_constraints() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    // Inserting into discogs_mapping without a matching entity should fail
    let result = client.execute(
        "INSERT INTO discogs_mapping (qid, property, discogs_id) VALUES ('Q999999', 'P1953', '12345')",
        &[],
    );
    assert!(result.is_err(), "FK violation should produce an error");

    // Insert the entity first, then the mapping should succeed
    client
        .execute(
            "INSERT INTO entity (qid, label, description, entity_type) VALUES ('Q999999', 'Test', 'Test entity', 'human')",
            &[],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO discogs_mapping (qid, property, discogs_id) VALUES ('Q999999', 'P1953', '12345')",
            &[],
        )
        .unwrap();
}

#[test]
fn test_schema_idempotent() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    // Applying the schema a second time should not error (IF NOT EXISTS)
    import_schema::create_schema(&mut client).unwrap();
}

// --- CSV import tests ---

#[test]
fn test_import_entity_csv() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 5, "Expected 5 entities");

    let row = client
        .query_one(
            "SELECT label, description, entity_type FROM entity WHERE qid = 'Q187923'",
            &[],
        )
        .unwrap();
    let label: &str = row.get(0);
    let description: &str = row.get(1);
    let entity_type: &str = row.get(2);
    assert_eq!(label, "Autechre");
    assert_eq!(description, "British electronic music duo");
    assert_eq!(entity_type, "group");
}

#[test]
fn test_import_discogs_mapping_csv() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM discogs_mapping", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 4, "Expected 4 discogs_mapping rows");

    let row = client
        .query_one(
            "SELECT discogs_id FROM discogs_mapping WHERE qid = 'Q187923' AND property = 'P1953'",
            &[],
        )
        .unwrap();
    let discogs_id: &str = row.get(0);
    assert_eq!(discogs_id, "12");
}

#[test]
fn test_import_influence_csv() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM influence", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 2, "Expected 2 influence rows");

    // Q643023 -> Q192465 is a dangling target_qid (Q192465 not in entity table)
    let row = client
        .query_one(
            "SELECT target_qid FROM influence WHERE source_qid = 'Q643023'",
            &[],
        )
        .unwrap();
    let target: &str = row.get(0);
    assert_eq!(target, "Q192465");
}

#[test]
fn test_import_all_csvs() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let total = import::import_all(&mut client, fixture_dir()).unwrap();

    let expected_counts: &[(&str, i64)] = &[
        ("entity", 5),
        ("discogs_mapping", 4),
        ("influence", 2),
        ("genre", 3),
        ("record_label", 3),
        ("label_hierarchy", 1),
        ("entity_alias", 3),
        ("occupation", 2),
    ];

    let mut expected_total: u64 = 0;
    for &(table, expected) in expected_counts {
        let count: i64 = client
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .unwrap()
            .get(0);
        assert_eq!(count, expected, "Table {table} row count mismatch");
        expected_total += expected as u64;
    }

    assert_eq!(total, expected_total, "Total imported row count mismatch");
}

#[test]
fn test_import_empty_csv() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let dir = tempfile::TempDir::new().unwrap();
    let headers: &[(&str, &str)] = &[
        ("entity.csv", "qid,label,description,entity_type\n"),
        ("discogs_mapping.csv", "qid,property,discogs_id\n"),
        ("influence.csv", "source_qid,target_qid\n"),
        ("genre.csv", "entity_qid,genre_qid\n"),
        ("record_label.csv", "artist_qid,label_qid\n"),
        ("label_hierarchy.csv", "child_qid,parent_qid\n"),
        ("entity_alias.csv", "qid,alias\n"),
        ("occupation.csv", "entity_qid,occupation_qid\n"),
    ];

    for &(name, content) in headers {
        std::fs::write(dir.path().join(name), content).unwrap();
    }

    let total = import::import_all(&mut client, dir.path()).unwrap();
    assert_eq!(total, 0, "Empty CSVs should import 0 rows");
}

#[test]
fn test_import_csv_with_commas_in_fields() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let dir = tempfile::TempDir::new().unwrap();

    // Entity with commas in description (RFC 4180 quoted field)
    std::fs::write(
        dir.path().join("entity.csv"),
        "qid,label,description,entity_type\nQ100,\"Test, Artist\",\"Rock, pop, and jazz musician\",human\n",
    )
    .unwrap();

    let empties: &[(&str, &str)] = &[
        ("discogs_mapping.csv", "qid,property,discogs_id\n"),
        ("influence.csv", "source_qid,target_qid\n"),
        ("genre.csv", "entity_qid,genre_qid\n"),
        ("record_label.csv", "artist_qid,label_qid\n"),
        ("label_hierarchy.csv", "child_qid,parent_qid\n"),
        ("entity_alias.csv", "qid,alias\n"),
        ("occupation.csv", "entity_qid,occupation_qid\n"),
    ];
    for &(name, content) in empties {
        std::fs::write(dir.path().join(name), content).unwrap();
    }

    import::import_all(&mut client, dir.path()).unwrap();

    let row = client
        .query_one(
            "SELECT label, description FROM entity WHERE qid = 'Q100'",
            &[],
        )
        .unwrap();
    let label: &str = row.get(0);
    let description: &str = row.get(1);
    assert_eq!(label, "Test, Artist");
    assert_eq!(description, "Rock, pop, and jazz musician");
}

#[test]
fn test_import_csv_with_unicode() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    let dir = tempfile::TempDir::new().unwrap();

    std::fs::write(
        dir.path().join("entity.csv"),
        "qid,label,description,entity_type\nQ101,Bj\u{00f6}rk,Icelandic musician,human\nQ102,\u{5742}\u{672c}\u{9f8d}\u{4e00},Japanese musician,human\n",
    )
    .unwrap();

    let empties: &[(&str, &str)] = &[
        ("discogs_mapping.csv", "qid,property,discogs_id\n"),
        ("influence.csv", "source_qid,target_qid\n"),
        ("genre.csv", "entity_qid,genre_qid\n"),
        ("record_label.csv", "artist_qid,label_qid\n"),
        ("label_hierarchy.csv", "child_qid,parent_qid\n"),
        ("entity_alias.csv", "qid,alias\n"),
        ("occupation.csv", "entity_qid,occupation_qid\n"),
    ];
    for &(name, content) in empties {
        std::fs::write(dir.path().join(name), content).unwrap();
    }

    import::import_all(&mut client, dir.path()).unwrap();

    let row = client
        .query_one("SELECT label FROM entity WHERE qid = 'Q101'", &[])
        .unwrap();
    let label: &str = row.get(0);
    assert_eq!(label, "Bj\u{00f6}rk");

    let row = client
        .query_one("SELECT label FROM entity WHERE qid = 'Q102'", &[])
        .unwrap();
    let label: &str = row.get(0);
    assert_eq!(label, "\u{5742}\u{672c}\u{9f8d}\u{4e00}");
}

// --- FK integrity and query pattern tests ---

#[test]
fn test_fk_entity_to_discogs_mapping() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM discogs_mapping dm JOIN entity e ON dm.qid = e.qid",
            &[],
        )
        .unwrap()
        .get(0);
    let total: i64 = client
        .query_one("SELECT COUNT(*) FROM discogs_mapping", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, total,
        "All discogs_mapping rows should have a matching entity"
    );
}

#[test]
fn test_fk_entity_to_entity_alias() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM entity_alias ea JOIN entity e ON ea.qid = e.qid",
            &[],
        )
        .unwrap()
        .get(0);
    let total: i64 = client
        .query_one("SELECT COUNT(*) FROM entity_alias", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, total,
        "All entity_alias rows should have a matching entity"
    );
}

#[test]
fn test_fk_entity_to_occupation() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM occupation o JOIN entity e ON o.entity_qid = e.qid",
            &[],
        )
        .unwrap()
        .get(0);
    let total: i64 = client
        .query_one("SELECT COUNT(*) FROM occupation", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, total,
        "All occupation rows should have a matching entity"
    );
}

#[test]
fn test_influence_source_has_entity() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM influence i JOIN entity e ON i.source_qid = e.qid",
            &[],
        )
        .unwrap()
        .get(0);
    let total: i64 = client
        .query_one("SELECT COUNT(*) FROM influence", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, total,
        "All influence source_qid should have a matching entity"
    );
}

#[test]
fn test_discogs_mapping_p1953_lookup() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    // Exact query pattern used by wikidata_client.py lookup_by_discogs_ids()
    let row = client
        .query_one(
            "SELECT e.qid, e.label FROM entity e JOIN discogs_mapping dm ON e.qid = dm.qid WHERE dm.property = 'P1953' AND dm.discogs_id = '12'",
            &[],
        )
        .unwrap();

    let qid: &str = row.get(0);
    let label: &str = row.get(1);
    assert_eq!(qid, "Q187923");
    assert_eq!(label, "Autechre");
}

#[test]
fn test_influence_query_pattern() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    // Access pattern used by wikidata_client.py get_influences()
    let rows = client
        .query(
            "SELECT source_qid, target_qid FROM influence WHERE source_qid = 'Q187923'",
            &[],
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    let target: &str = rows[0].get(1);
    assert_eq!(target, "Q49835");
}

#[test]
fn test_import_idempotent() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    import::import_all(&mut client, fixture_dir()).unwrap();

    let count_before: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);

    // Truncate and re-import (idempotent pattern)
    import_schema::truncate_all(&mut client).unwrap();
    import::import_all(&mut client, fixture_dir()).unwrap();

    let count_after: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count_before, count_after,
        "Re-import should produce the same row count"
    );
}

#[test]
fn test_unlogged_and_logged_toggle() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);

    import_schema::set_tables_unlogged(&mut client).unwrap();
    import::import_all(&mut client, fixture_dir()).unwrap();
    import_schema::set_tables_logged(&mut client).unwrap();

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 5);
}

#[test]
fn test_vacuum_after_import() {
    let _lock = lock_db();
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, fixture_dir()).unwrap();

    import_schema::vacuum_full(&mut client).unwrap();
}

// --- End-to-end test: JSON dump -> CSV -> PostgreSQL ---

#[test]
fn test_end_to_end_pipeline() {
    let _lock = lock_db();

    // Step 1: Run wikidata-cache on the small JSON dump to produce CSVs
    let csv_dir = tempfile::TempDir::new().unwrap();

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("tests/fixtures/small_dump.json")
        .arg("--output-dir")
        .arg(csv_dir.path())
        .assert()
        .success();

    // Step 2: Import the CSVs into PostgreSQL
    let mut client = test_client();
    fresh_schema(&mut client);
    import::import_all(&mut client, csv_dir.path()).unwrap();

    // Step 3: Validate the database state matches the 3 music-relevant entities
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, 3,
        "Expected 3 music-relevant entities from small_dump.json"
    );

    // Verify Autechre
    let row = client
        .query_one(
            "SELECT label, entity_type FROM entity WHERE qid = 'Q187923'",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "Autechre");
    assert_eq!(row.get::<_, &str>(1), "group");

    // Verify Warp Records
    let row = client
        .query_one(
            "SELECT label, entity_type FROM entity WHERE qid = 'Q1312934'",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "Warp Records");
    assert_eq!(row.get::<_, &str>(1), "label");

    // Verify Stereolab
    let row = client
        .query_one(
            "SELECT label, entity_type FROM entity WHERE qid = 'Q643023'",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "Stereolab");
    assert_eq!(row.get::<_, &str>(1), "group");

    // Verify discogs mappings
    let dm_count: i64 = client
        .query_one("SELECT COUNT(*) FROM discogs_mapping", &[])
        .unwrap()
        .get(0);
    assert!(
        dm_count >= 4,
        "Expected at least 4 discogs_mapping rows (P1953, P1902, P434)"
    );

    // Verify Autechre influence -> Kraftwerk
    let inf_rows = client
        .query(
            "SELECT target_qid FROM influence WHERE source_qid = 'Q187923'",
            &[],
        )
        .unwrap();
    assert_eq!(inf_rows.len(), 1);
    assert_eq!(inf_rows[0].get::<_, &str>(0), "Q49835");

    // Verify label hierarchy: Warp -> parent
    let lh_rows = client
        .query(
            "SELECT parent_qid FROM label_hierarchy WHERE child_qid = 'Q1312934'",
            &[],
        )
        .unwrap();
    assert_eq!(lh_rows.len(), 1);
    assert_eq!(lh_rows[0].get::<_, &str>(0), "Q21077");
}

// --- CLI import subcommand test ---

#[test]
fn test_import_subcommand() {
    let _lock = lock_db();

    // Drop/create schema first so the subcommand has a clean slate
    let mut client = test_client();
    import_schema::drop_schema(&mut client).unwrap();
    drop(client);

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("import")
        .arg("--csv-dir")
        .arg("tests/fixtures/import")
        .arg("--database-url")
        .arg(TEST_DB_URL)
        .arg("--fresh")
        .assert()
        .success();

    let mut client = test_client();
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 5, "Import subcommand should load 5 entities");
}
