//! PostgreSQL import integration test for wikidata-json-filter.
//!
//! Verifies the CSV -> PG roundtrip: filter small_dump.json, produce CSVs,
//! import each CSV into PostgreSQL via COPY, verify all 8 tables are populated
//! and foreign key integrity holds.
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
        if let Ok(mut client) =
            postgres::Client::connect(&self.admin_url, postgres::NoTls)
        {
            let _ = client.execute(
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                    self.db_name
                ),
                &[],
            );
            let _ = client.execute(
                &format!("DROP DATABASE IF EXISTS {}", self.db_name),
                &[],
            );
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
fn import_csv(client: &mut postgres::Client, csv_path: &std::path::Path, table: &str, columns: &str) {
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
    let Some(admin_url) = test_db_url() else { return };
    let temp_db = TempDb::new(&admin_url);

    // Step 1: Generate CSVs from fixture
    let output_dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("wikidata-json-filter")
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
        ("discogs_mapping.csv", "discogs_mapping", "qid,property,discogs_id"),
        ("influence.csv", "influence", "source_qid,target_qid"),
        ("genre.csv", "genre", "entity_qid,genre_qid"),
        ("record_label.csv", "record_label", "artist_qid,label_qid"),
        ("label_hierarchy.csv", "label_hierarchy", "child_qid,parent_qid"),
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
    let row = client.query_one("SELECT COUNT(*) FROM entity", &[]).unwrap();
    let entity_count: i64 = row.get(0);
    assert_eq!(entity_count, 3, "Expected 3 entities (Autechre, Warp Records, Stereolab)");

    let row = client.query_one("SELECT COUNT(*) FROM discogs_mapping", &[]).unwrap();
    let mapping_count: i64 = row.get(0);
    assert_eq!(mapping_count, 4, "Expected 4 external ID mappings");

    let row = client.query_one("SELECT COUNT(*) FROM influence", &[]).unwrap();
    let influence_count: i64 = row.get(0);
    assert_eq!(influence_count, 1, "Expected 1 influence (Autechre -> Q49835)");

    let row = client.query_one("SELECT COUNT(*) FROM genre", &[]).unwrap();
    let genre_count: i64 = row.get(0);
    assert_eq!(genre_count, 2, "Expected 2 genre associations");

    let row = client.query_one("SELECT COUNT(*) FROM record_label", &[]).unwrap();
    let label_count: i64 = row.get(0);
    assert_eq!(label_count, 1, "Expected 1 record label association");

    let row = client.query_one("SELECT COUNT(*) FROM label_hierarchy", &[]).unwrap();
    let hierarchy_count: i64 = row.get(0);
    assert_eq!(hierarchy_count, 1, "Expected 1 label hierarchy entry");

    let row = client.query_one("SELECT COUNT(*) FROM entity_alias", &[]).unwrap();
    let alias_count: i64 = row.get(0);
    assert_eq!(alias_count, 1, "Expected 1 alias (ae for Autechre)");

    let row = client.query_one("SELECT COUNT(*) FROM occupation", &[]).unwrap();
    let occ_count: i64 = row.get(0);
    assert_eq!(occ_count, 0, "Expected 0 occupations (none of the 3 entities have musician occupation in fixture)");

    // Step 5: Verify specific data
    let row = client
        .query_one("SELECT label, entity_type FROM entity WHERE qid = 'Q187923'", &[])
        .unwrap();
    let label: &str = row.get(0);
    let entity_type: &str = row.get(1);
    assert_eq!(label, "Autechre");
    assert_eq!(entity_type, "group");

    let row = client
        .query_one("SELECT label, entity_type FROM entity WHERE qid = 'Q1312934'", &[])
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
    assert!(props.contains("P1953"), "Autechre should have Discogs artist ID");
    assert!(props.contains("P434"), "Autechre should have MusicBrainz ID");
}

#[test]
fn test_all_eight_tables_populated_or_empty() {
    let Some(admin_url) = test_db_url() else { return };
    let temp_db = TempDb::new(&admin_url);

    let output_dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("wikidata-json-filter")
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
        ("discogs_mapping.csv", "discogs_mapping", "qid,property,discogs_id"),
        ("influence.csv", "influence", "source_qid,target_qid"),
        ("genre.csv", "genre", "entity_qid,genre_qid"),
        ("record_label.csv", "record_label", "artist_qid,label_qid"),
        ("label_hierarchy.csv", "label_hierarchy", "child_qid,parent_qid"),
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
    let populated = ["entity", "discogs_mapping", "influence", "genre", "record_label", "label_hierarchy", "entity_alias"];
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
