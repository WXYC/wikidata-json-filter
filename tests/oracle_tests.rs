//! Oracle tests: verify Rust output matches expected CSVs.
//!
//! These tests filter small_dump.json and diff each CSV against
//! the expected output in tests/fixtures/expected/.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use assert_cmd::Command;
use pretty_assertions::assert_eq;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn expected_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("expected")
        .join(name)
}

/// Normalize CSV content for comparison: parse and re-serialize to handle
/// quoting differences, then sort data rows for order-independent comparison.
fn normalize_csv(content: &str) -> String {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(content.as_bytes());

    let headers = rdr.headers().unwrap().clone();
    let mut rows: Vec<Vec<String>> = rdr
        .records()
        .map(|r| {
            let record = r.unwrap();
            record.iter().map(|f| f.to_string()).collect()
        })
        .collect();

    // Sort rows for deterministic comparison
    rows.sort();

    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record(&headers).unwrap();
    for row in &rows {
        wtr.write_record(row).unwrap();
    }
    wtr.flush().unwrap();
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
}

fn generate_output() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture_path("small_dump.json");

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("build")
        .arg(input)
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success();

    dir
}

#[test]
fn test_oracle_entity_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("entity.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("entity.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_discogs_mapping_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("discogs_mapping.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("discogs_mapping.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_influence_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("influence.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("influence.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_genre_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("genre.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("genre.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_record_label_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("record_label.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("record_label.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_label_hierarchy_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("label_hierarchy.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("label_hierarchy.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_entity_alias_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("entity_alias.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("entity_alias.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_oracle_occupation_csv() {
    let dir = generate_output();
    let actual = fs::read_to_string(dir.path().join("occupation.csv")).unwrap();
    let expected = fs::read_to_string(expected_path("occupation.csv")).unwrap();
    assert_eq!(normalize_csv(&actual), normalize_csv(&expected));
}

#[test]
fn test_referential_integrity() {
    let dir = generate_output();

    // Load entity QIDs
    let mut rdr = csv::Reader::from_path(dir.path().join("entity.csv")).unwrap();
    let entity_qids: HashSet<String> = rdr.records().map(|r| r.unwrap()[0].to_string()).collect();

    // Check child CSVs: all source QIDs must reference an entity
    for (filename, qid_col) in &[
        ("influence.csv", 0),
        ("genre.csv", 0),
        ("record_label.csv", 0),
        ("entity_alias.csv", 0),
        ("occupation.csv", 0),
    ] {
        let path = dir.path().join(filename);
        let mut rdr = csv::Reader::from_path(&path).unwrap();
        for result in rdr.records() {
            let record = result.unwrap();
            let qid = &record[*qid_col];
            assert!(
                entity_qids.contains(qid),
                "Orphan QID {} in {} not found in entity.csv",
                qid,
                filename
            );
        }
    }
}
