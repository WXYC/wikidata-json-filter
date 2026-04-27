//! PostgreSQL import integration tests for wikidata-cache.
//!
//! Verifies the full filter -> CSV -> PG import -> query chain:
//! 1. Filter small_dump.json through the binary, producing CSVs
//! 2. Import CSVs into PostgreSQL via COPY
//! 3. Verify row counts, data integrity, and FK constraints
//! 4. Verify trigram search on entity names works with pg_trgm
//! 5. Verify Discogs ID lookup via index
//!
//! Gated on TEST_DATABASE_URL. Skips when the env var is unset.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use assert_cmd::Command;

fn test_db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

/// Create a temporary database and return its URL. Drop on close.
struct TempDb {
    admin_url: String,
    db_name: String,
    pub url: String,
}

impl TempDb {
    fn new(admin_url: &str) -> Self {
        let db_name = format!("wd_test_{:08x}", rand_u32());
        let mut client = postgres::Client::connect(admin_url, postgres::NoTls).unwrap();
        client
            .execute(&format!("CREATE DATABASE {}", db_name), &[])
            .unwrap();
        let base = admin_url.rsplit_once('/').unwrap().0;
        let url = format!("{}/{}", base, db_name);
        Self {
            admin_url: admin_url.to_string(),
            db_name,
            url,
        }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        if let Ok(mut client) = postgres::Client::connect(&self.admin_url, postgres::NoTls) {
            let _ = client.execute(
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                    self.db_name
                ),
                &[],
            );
            let _ = client.execute(&format!("DROP DATABASE IF EXISTS {}", self.db_name), &[]);
        }
    }
}

fn rand_u32() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::SystemTime;
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    (d.subsec_nanos() ^ (d.as_secs() as u32) ^ seq).wrapping_mul(2654435761)
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// Create the wikidata-cache schema in the test database.
fn set_up_schema(client: &mut postgres::Client) {
    client
        .batch_execute(
            "DROP TABLE IF EXISTS occupation CASCADE;
             DROP TABLE IF EXISTS entity_alias CASCADE;
             DROP TABLE IF EXISTS label_hierarchy CASCADE;
             DROP TABLE IF EXISTS record_label CASCADE;
             DROP TABLE IF EXISTS genre CASCADE;
             DROP TABLE IF EXISTS influence CASCADE;
             DROP TABLE IF EXISTS discogs_mapping CASCADE;
             DROP TABLE IF EXISTS entity CASCADE;

             CREATE TABLE entity (
                 qid text PRIMARY KEY,
                 label text NOT NULL DEFAULT '',
                 description text NOT NULL DEFAULT '',
                 entity_type text NOT NULL DEFAULT 'other'
             );
             CREATE TABLE discogs_mapping (
                 qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 property text NOT NULL,
                 discogs_id text NOT NULL
             );
             CREATE TABLE influence (
                 source_qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 target_qid text NOT NULL
             );
             CREATE TABLE genre (
                 entity_qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 genre_qid text NOT NULL
             );
             CREATE TABLE record_label (
                 artist_qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 label_qid text NOT NULL
             );
             CREATE TABLE label_hierarchy (
                 child_qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 parent_qid text NOT NULL
             );
             CREATE TABLE entity_alias (
                 qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 alias text NOT NULL
             );
             CREATE TABLE occupation (
                 entity_qid text NOT NULL REFERENCES entity(qid) ON DELETE CASCADE,
                 occupation_qid text NOT NULL
             );",
        )
        .unwrap();
}

/// Import a CSV file into a table via COPY.
fn import_csv(
    client: &mut postgres::Client,
    csv_path: &std::path::Path,
    table: &str,
    columns: &str,
) {
    let content = fs::read_to_string(csv_path).unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(content.as_bytes());

    let copy_stmt = format!("COPY {} ({}) FROM STDIN WITH (FORMAT csv)", table, columns);
    let mut writer = client.copy_in(&copy_stmt).unwrap();

    for result in rdr.records() {
        let record = result.unwrap();
        let mut line = String::new();
        for (i, field) in record.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            // Quote fields that contain commas, quotes, or newlines
            if field.contains(',') || field.contains('"') || field.contains('\n') {
                line.push('"');
                line.push_str(&field.replace('"', "\"\""));
                line.push('"');
            } else {
                line.push_str(field);
            }
        }
        line.push('\n');
        use std::io::Write;
        writer.write_all(line.as_bytes()).unwrap();
    }
    writer.finish().unwrap();
}

#[test]
fn test_csv_imports_into_pg() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let temp_db = TempDb::new(&admin_url);

    // Step 1: Generate CSVs from fixture
    let output_dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg(fixture_path("small_dump.json"))
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    // Step 2: Set up PG schema
    let mut client = postgres::Client::connect(&temp_db.url, postgres::NoTls).unwrap();
    set_up_schema(&mut client);

    // Step 3: Import CSVs in FK order (entity first, then children)
    let imports = [
        ("entity.csv", "entity", "qid,label,description,entity_type"),
        (
            "discogs_mapping.csv",
            "discogs_mapping",
            "qid,property,discogs_id",
        ),
        ("influence.csv", "influence", "source_qid,target_qid"),
        ("genre.csv", "genre", "entity_qid,genre_qid"),
        ("record_label.csv", "record_label", "artist_qid,label_qid"),
        (
            "label_hierarchy.csv",
            "label_hierarchy",
            "child_qid,parent_qid",
        ),
        ("entity_alias.csv", "entity_alias", "qid,alias"),
        ("occupation.csv", "occupation", "entity_qid,occupation_qid"),
    ];

    for (csv_file, table, columns) in &imports {
        import_csv(
            &mut client,
            &output_dir.path().join(csv_file),
            table,
            columns,
        );
    }

    // Step 4: Verify row counts
    let row = client
        .query_one("SELECT COUNT(*) FROM entity", &[])
        .unwrap();
    let entity_count: i64 = row.get(0);
    assert_eq!(
        entity_count, 3,
        "Expected 3 entities (Autechre, Warp Records, Stereolab)"
    );

    let row = client
        .query_one("SELECT COUNT(*) FROM discogs_mapping", &[])
        .unwrap();
    let mapping_count: i64 = row.get(0);
    assert_eq!(mapping_count, 4, "Expected 4 external ID mappings");

    let row = client
        .query_one("SELECT COUNT(*) FROM influence", &[])
        .unwrap();
    let influence_count: i64 = row.get(0);
    assert_eq!(
        influence_count, 1,
        "Expected 1 influence (Autechre -> Q49835)"
    );

    let row = client.query_one("SELECT COUNT(*) FROM genre", &[]).unwrap();
    let genre_count: i64 = row.get(0);
    assert_eq!(genre_count, 2, "Expected 2 genre associations");

    let row = client
        .query_one("SELECT COUNT(*) FROM record_label", &[])
        .unwrap();
    let label_count: i64 = row.get(0);
    assert_eq!(label_count, 1, "Expected 1 record label association");

    let row = client
        .query_one("SELECT COUNT(*) FROM label_hierarchy", &[])
        .unwrap();
    let hierarchy_count: i64 = row.get(0);
    assert_eq!(hierarchy_count, 1, "Expected 1 label hierarchy entry");

    let row = client
        .query_one("SELECT COUNT(*) FROM entity_alias", &[])
        .unwrap();
    let alias_count: i64 = row.get(0);
    assert_eq!(alias_count, 1, "Expected 1 alias (ae for Autechre)");

    let row = client
        .query_one("SELECT COUNT(*) FROM occupation", &[])
        .unwrap();
    let occ_count: i64 = row.get(0);
    assert_eq!(
        occ_count, 0,
        "Expected 0 occupations (none of the 3 entities have musician occupation in fixture)"
    );

    // Step 5: Verify specific data
    let row = client
        .query_one(
            "SELECT label, entity_type FROM entity WHERE qid = 'Q187923'",
            &[],
        )
        .unwrap();
    let label: &str = row.get(0);
    let entity_type: &str = row.get(1);
    assert_eq!(label, "Autechre");
    assert_eq!(entity_type, "group");

    let row = client
        .query_one(
            "SELECT label, entity_type FROM entity WHERE qid = 'Q1312934'",
            &[],
        )
        .unwrap();
    let label: &str = row.get(0);
    let entity_type: &str = row.get(1);
    assert_eq!(label, "Warp Records");
    assert_eq!(entity_type, "label");

    // Step 6: Verify FK integrity -- all child QIDs reference parent entities
    let orphans: Vec<postgres::Row> = client
        .query(
            "SELECT source_qid FROM influence WHERE source_qid NOT IN (SELECT qid FROM entity)",
            &[],
        )
        .unwrap();
    assert!(orphans.is_empty(), "Influence table has orphan source QIDs");

    let orphans: Vec<postgres::Row> = client
        .query(
            "SELECT entity_qid FROM genre WHERE entity_qid NOT IN (SELECT qid FROM entity)",
            &[],
        )
        .unwrap();
    assert!(orphans.is_empty(), "Genre table has orphan entity QIDs");

    let orphans: Vec<postgres::Row> = client
        .query(
            "SELECT qid FROM discogs_mapping WHERE qid NOT IN (SELECT qid FROM entity)",
            &[],
        )
        .unwrap();
    assert!(orphans.is_empty(), "Discogs mapping table has orphan QIDs");

    // Step 7: Verify Discogs IDs
    let rows: Vec<postgres::Row> = client
        .query(
            "SELECT property, discogs_id FROM discogs_mapping WHERE qid = 'Q187923' ORDER BY property",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    let props: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        props.contains("P1953"),
        "Autechre should have Discogs artist ID"
    );
    assert!(
        props.contains("P434"),
        "Autechre should have MusicBrainz ID"
    );
}

#[test]
fn test_all_eight_tables_populated_or_empty() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let temp_db = TempDb::new(&admin_url);

    let output_dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg(fixture_path("small_dump.json"))
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let mut client = postgres::Client::connect(&temp_db.url, postgres::NoTls).unwrap();
    set_up_schema(&mut client);

    let imports = [
        ("entity.csv", "entity", "qid,label,description,entity_type"),
        (
            "discogs_mapping.csv",
            "discogs_mapping",
            "qid,property,discogs_id",
        ),
        ("influence.csv", "influence", "source_qid,target_qid"),
        ("genre.csv", "genre", "entity_qid,genre_qid"),
        ("record_label.csv", "record_label", "artist_qid,label_qid"),
        (
            "label_hierarchy.csv",
            "label_hierarchy",
            "child_qid,parent_qid",
        ),
        ("entity_alias.csv", "entity_alias", "qid,alias"),
        ("occupation.csv", "occupation", "entity_qid,occupation_qid"),
    ];

    for (csv_file, table, columns) in &imports {
        import_csv(
            &mut client,
            &output_dir.path().join(csv_file),
            table,
            columns,
        );
    }

    // All 8 tables should exist and be queryable
    let tables = [
        "entity",
        "discogs_mapping",
        "influence",
        "genre",
        "record_label",
        "label_hierarchy",
        "entity_alias",
        "occupation",
    ];

    for table in &tables {
        let row = client
            .query_one(&format!("SELECT COUNT(*) FROM {}", table), &[])
            .unwrap();
        let count: i64 = row.get(0);
        // All tables should be queryable (some may have 0 rows, like occupation)
        assert!(
            count >= 0,
            "Table {} should be queryable after CSV import",
            table
        );
    }

    // Populated tables should have > 0 rows
    let populated = [
        "entity",
        "discogs_mapping",
        "influence",
        "genre",
        "record_label",
        "label_hierarchy",
        "entity_alias",
    ];
    for table in &populated {
        let row = client
            .query_one(&format!("SELECT COUNT(*) FROM {}", table), &[])
            .unwrap();
        let count: i64 = row.get(0);
        assert!(
            count > 0,
            "Table {} should have rows after importing small_dump.json",
            table
        );
    }
}

// --- Index and query pattern tests ---

/// Create the indexes that downstream consumers (wikidata-cache) use for querying.
fn create_indexes(client: &mut postgres::Client) {
    client
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS pg_trgm;
             CREATE INDEX IF NOT EXISTS idx_entity_label_trgm ON entity USING gin (label gin_trgm_ops);
             CREATE INDEX IF NOT EXISTS idx_entity_alias_trgm ON entity_alias USING gin (alias gin_trgm_ops);
             CREATE INDEX IF NOT EXISTS idx_discogs_mapping_property_id ON discogs_mapping (property, discogs_id);
             CREATE INDEX IF NOT EXISTS idx_discogs_mapping_qid ON discogs_mapping (qid);",
        )
        .unwrap();
}

/// Helper: set up a full test database with schema, data, and indexes.
fn set_up_full_db(admin_url: &str) -> (TempDb, postgres::Client) {
    let temp_db = TempDb::new(admin_url);

    let output_dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg(fixture_path("small_dump.json"))
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let mut client = postgres::Client::connect(&temp_db.url, postgres::NoTls).unwrap();
    set_up_schema(&mut client);

    let imports = [
        ("entity.csv", "entity", "qid,label,description,entity_type"),
        (
            "discogs_mapping.csv",
            "discogs_mapping",
            "qid,property,discogs_id",
        ),
        ("influence.csv", "influence", "source_qid,target_qid"),
        ("genre.csv", "genre", "entity_qid,genre_qid"),
        ("record_label.csv", "record_label", "artist_qid,label_qid"),
        (
            "label_hierarchy.csv",
            "label_hierarchy",
            "child_qid,parent_qid",
        ),
        ("entity_alias.csv", "entity_alias", "qid,alias"),
        ("occupation.csv", "occupation", "entity_qid,occupation_qid"),
    ];

    for (csv_file, table, columns) in &imports {
        import_csv(
            &mut client,
            &output_dir.path().join(csv_file),
            table,
            columns,
        );
    }

    create_indexes(&mut client);
    (temp_db, client)
}

#[test]
fn test_trigram_search_exact_match() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Exact name search using trigram similarity
    let rows = client
        .query(
            "SELECT qid, label FROM entity WHERE label % $1 ORDER BY similarity(label, $1) DESC",
            &[&"Autechre"],
        )
        .unwrap();
    assert!(
        !rows.is_empty(),
        "Trigram search for 'Autechre' should return results"
    );
    assert_eq!(
        rows[0].get::<_, &str>(1),
        "Autechre",
        "Best match should be exact"
    );
}

#[test]
fn test_trigram_search_fuzzy_match() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Fuzzy search: "Autechree" (typo) should still find "Autechre"
    let rows = client
        .query(
            "SELECT qid, label, similarity(label, $1) as sim \
             FROM entity \
             WHERE label % $1 \
             ORDER BY sim DESC",
            &[&"Autechree"],
        )
        .unwrap();
    assert!(
        !rows.is_empty(),
        "Trigram search for 'Autechree' (typo) should find 'Autechre'"
    );
    let best_label: &str = rows[0].get(1);
    assert_eq!(best_label, "Autechre");
}

#[test]
fn test_trigram_search_partial_match() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Partial search: "Stereo" should find "Stereolab" via ILIKE or trigram
    let rows = client
        .query(
            "SELECT qid, label FROM entity WHERE label ILIKE $1",
            &[&"%Stereo%"],
        )
        .unwrap();
    assert!(
        !rows.is_empty(),
        "ILIKE search for '%Stereo%' should find 'Stereolab'"
    );
    let labels: Vec<&str> = rows.iter().map(|r| r.get(1)).collect();
    assert!(labels.contains(&"Stereolab"));
}

#[test]
fn test_trigram_search_on_aliases() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Search aliases: "ae" is an alias for Autechre (Q187923)
    let rows = client
        .query(
            "SELECT e.qid, e.label FROM entity_alias a \
             JOIN entity e ON e.qid = a.qid \
             WHERE a.alias % $1 \
             ORDER BY similarity(a.alias, $1) DESC",
            &[&"ae"],
        )
        .unwrap();
    assert!(
        !rows.is_empty(),
        "Trigram search on aliases for 'ae' should find Autechre"
    );
    assert_eq!(rows[0].get::<_, &str>(0), "Q187923");
    assert_eq!(rows[0].get::<_, &str>(1), "Autechre");
}

#[test]
fn test_discogs_id_lookup_by_artist_id() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Look up entity by Discogs artist ID (P1953)
    let rows = client
        .query(
            "SELECT e.qid, e.label, e.entity_type \
             FROM discogs_mapping dm \
             JOIN entity e ON e.qid = dm.qid \
             WHERE dm.property = 'P1953' AND dm.discogs_id = $1",
            &[&"12"],
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Discogs artist ID 12 should match one entity"
    );
    assert_eq!(rows[0].get::<_, &str>(0), "Q187923");
    assert_eq!(rows[0].get::<_, &str>(1), "Autechre");
    assert_eq!(rows[0].get::<_, &str>(2), "group");
}

#[test]
fn test_discogs_id_lookup_by_label_id() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Look up entity by Discogs label ID (P1902)
    let rows = client
        .query(
            "SELECT e.qid, e.label, e.entity_type \
             FROM discogs_mapping dm \
             JOIN entity e ON e.qid = dm.qid \
             WHERE dm.property = 'P1902' AND dm.discogs_id = $1",
            &[&"23528"],
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Discogs label ID 23528 should match one entity"
    );
    assert_eq!(rows[0].get::<_, &str>(0), "Q1312934");
    assert_eq!(rows[0].get::<_, &str>(1), "Warp Records");
    assert_eq!(rows[0].get::<_, &str>(2), "label");
}

#[test]
fn test_discogs_id_lookup_nonexistent() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Look up a Discogs ID that doesn't exist
    let rows = client
        .query(
            "SELECT qid FROM discogs_mapping WHERE property = 'P1953' AND discogs_id = $1",
            &[&"99999999"],
        )
        .unwrap();
    assert!(
        rows.is_empty(),
        "Nonexistent Discogs ID should return no results"
    );
}

#[test]
fn test_musicbrainz_id_lookup() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Look up entity by MusicBrainz artist ID (P434)
    let rows = client
        .query(
            "SELECT e.qid, e.label FROM discogs_mapping dm \
             JOIN entity e ON e.qid = dm.qid \
             WHERE dm.property = 'P434' AND dm.discogs_id = $1",
            &[&"410c9baf-5469-44f6-9852-826524b80c61"],
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "MusicBrainz ID should match Autechre");
    assert_eq!(rows[0].get::<_, &str>(1), "Autechre");
}

#[test]
fn test_indexes_exist_after_creation() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname",
            &[],
        )
        .unwrap();
    let index_names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();

    assert!(
        index_names.contains(&"idx_entity_label_trgm".to_string()),
        "Trigram index on entity.label should exist"
    );
    assert!(
        index_names.contains(&"idx_entity_alias_trgm".to_string()),
        "Trigram index on entity_alias.alias should exist"
    );
    assert!(
        index_names.contains(&"idx_discogs_mapping_property_id".to_string()),
        "Composite index on discogs_mapping(property, discogs_id) should exist"
    );
    assert!(
        index_names.contains(&"idx_discogs_mapping_qid".to_string()),
        "Index on discogs_mapping(qid) should exist"
    );
}

#[test]
fn test_index_used_for_discogs_lookup() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // EXPLAIN should show Index Scan for the Discogs ID lookup
    let rows = client
        .query(
            "EXPLAIN SELECT qid FROM discogs_mapping WHERE property = 'P1953' AND discogs_id = '12'",
            &[],
        )
        .unwrap();
    let plan: String = rows
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");

    // With only 4 rows the planner may choose Seq Scan, so we just verify the
    // query succeeds and the index exists (already tested above). For large
    // datasets the planner would use the index.
    assert!(!plan.is_empty(), "EXPLAIN should return a query plan");
}

/// Run `EXPLAIN <query>` (default text format) and join the lines into one
/// string for substring assertions. Plain text format avoids needing the
/// `postgres` crate's optional `with-serde_json-1` feature.
fn explain_plan_text(client: &mut postgres::Client, query: &str) -> String {
    let rows = client.query(&format!("EXPLAIN {}", query), &[]).unwrap();
    rows.iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn test_explain_uses_index_for_discogs_lookup() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Insert ~500 synthetic discogs_mapping rows so the planner prefers the
    // composite index over a sequential scan. Each synthetic row needs a parent
    // entity row to satisfy the FK on discogs_mapping.qid.
    client
        .batch_execute(
            "INSERT INTO entity (qid, label, description, entity_type)
             SELECT 'Q9' || g::text, 'synthetic_' || g::text, '', 'group'
             FROM generate_series(1, 500) g;
             INSERT INTO discogs_mapping (qid, property, discogs_id)
             SELECT 'Q9' || g::text, 'P1953', (1000000 + g)::text
             FROM generate_series(1, 500) g;
             REINDEX INDEX idx_discogs_mapping_property_id;
             ANALYZE entity;
             ANALYZE discogs_mapping;",
        )
        .unwrap();

    let plan = explain_plan_text(
        &mut client,
        "SELECT qid FROM discogs_mapping \
         WHERE property = 'P1953' AND discogs_id = '1000123'",
    );

    let uses_index = plan.contains("Index Scan")
        || plan.contains("Bitmap Index Scan")
        || plan.contains("Index Only Scan");
    assert!(
        uses_index,
        "Expected EXPLAIN plan to include an index-scan node.\nPlan:\n{}",
        plan
    );

    assert!(
        plan.contains("idx_discogs_mapping_property_id"),
        "Expected plan to reference idx_discogs_mapping_property_id.\nPlan:\n{}",
        plan
    );
}

#[test]
fn test_explain_uses_trigram_index_for_label_search() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Insert ~20k entities with diverse synthetic labels so the GIN trigram
    // index beats a sequential scan (GIN has noticeable startup cost, so the
    // table has to be large enough that scanning it linearly is slower).
    // Use md5() to vary trigrams across rows. After bulk insert we REINDEX so
    // the GIN index is compact (incremental GIN inserts bloat the index, which
    // inflates the planner's estimated cost and can defeat this assertion).
    client
        .batch_execute(
            "INSERT INTO entity (qid, label, description, entity_type)
             SELECT 'Q8' || g::text,
                    'synth_' || md5(g::text) || '_band',
                    '',
                    'group'
             FROM generate_series(1, 20000) g;
             REINDEX INDEX idx_entity_label_trgm;
             ANALYZE entity;",
        )
        .unwrap();

    // Trigram similarity match against the seeded fixture name 'Autechre'. The
    // % operator is what idx_entity_label_trgm (gin_trgm_ops) accelerates.
    let plan = explain_plan_text(
        &mut client,
        "SELECT qid, label FROM entity WHERE label % 'autechre'",
    );

    assert!(
        plan.contains("Bitmap Index Scan"),
        "Expected a Bitmap Index Scan in the plan.\nPlan:\n{}",
        plan
    );
    assert!(
        plan.contains("idx_entity_label_trgm"),
        "Expected the plan to reference idx_entity_label_trgm.\nPlan:\n{}",
        plan
    );
}

#[test]
fn test_full_filter_csv_pg_query_chain() {
    let Some(admin_url) = test_db_url() else {
        return;
    };
    let (_temp_db, mut client) = set_up_full_db(&admin_url);

    // Full chain: filter -> CSV -> PG -> trigram search -> join with Discogs ID
    // Find "Stereolab" via trigram, then look up its Discogs artist ID
    let rows = client
        .query(
            "SELECT e.qid, e.label, dm.property, dm.discogs_id \
             FROM entity e \
             JOIN discogs_mapping dm ON dm.qid = e.qid \
             WHERE e.label % $1 AND dm.property = 'P1953' \
             ORDER BY similarity(e.label, $1) DESC",
            &[&"Stereolab"],
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Should find exactly one Stereolab + Discogs match"
    );
    assert_eq!(rows[0].get::<_, &str>(0), "Q643023");
    assert_eq!(rows[0].get::<_, &str>(1), "Stereolab");
    assert_eq!(rows[0].get::<_, &str>(2), "P1953");
    assert_eq!(rows[0].get::<_, &str>(3), "4965");
}
