use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

#[test]
fn filters_small_dump() {
    let output_dir = TempDir::new().unwrap();

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("tests/fixtures/small_dump.json")
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    // Should have matched 3 entities: Autechre, Warp Records, Stereolab
    // Should have skipped 2: Van Gogh (painter), Belgium (country)
    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    let entity_lines: Vec<&str> = entity_csv.lines().collect();
    // Header + 3 data rows
    assert_eq!(entity_lines.len(), 4, "Expected 3 music entities + header");
    assert!(entity_csv.contains("Autechre"));
    assert!(entity_csv.contains("Warp Records"));
    assert!(entity_csv.contains("Stereolab"));
    assert!(!entity_csv.contains("Van Gogh"));
    assert!(!entity_csv.contains("Belgium"));

    // External ID mappings: Autechre (P1953:12, P434:UUID), Warp (P1902:23528), Stereolab (P1953:4965)
    let mapping_csv = fs::read_to_string(output_dir.path().join("discogs_mapping.csv")).unwrap();
    assert!(mapping_csv.contains("P1953"));
    assert!(mapping_csv.contains("P1902"));
    assert!(mapping_csv.contains("P434"));
    assert!(mapping_csv.contains("12"));
    assert!(mapping_csv.contains("23528"));
    assert!(mapping_csv.contains("4965"));
    assert!(mapping_csv.contains("410c9baf-5469-44f6-9852-826524b80c61"));

    // Influences: Autechre -> Q49835
    let influence_csv = fs::read_to_string(output_dir.path().join("influence.csv")).unwrap();
    assert!(influence_csv.contains("Q187923"));
    assert!(influence_csv.contains("Q49835"));

    // Label hierarchy: Warp (Q1312934) -> parent Q21077
    let hierarchy_csv = fs::read_to_string(output_dir.path().join("label_hierarchy.csv")).unwrap();
    assert!(hierarchy_csv.contains("Q1312934"));
    assert!(hierarchy_csv.contains("Q21077"));
}

#[test]
fn limit_flag_stops_early() {
    let output_dir = TempDir::new().unwrap();

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("tests/fixtures/small_dump.json")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--limit")
        .arg("2")
        .assert()
        .success();

    // With limit=2, only first 2 entities processed (Autechre + Van Gogh)
    // Only Autechre should match
    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    let entity_lines: Vec<&str> = entity_csv.lines().collect();
    assert_eq!(entity_lines.len(), 2, "Expected 1 music entity + header");
    assert!(entity_csv.contains("Autechre"));
    assert!(!entity_csv.contains("Stereolab"));
}

#[test]
fn stdin_piping() {
    let output_dir = TempDir::new().unwrap();
    let input = fs::read("tests/fixtures/small_dump.json").unwrap();

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("-")
        .arg("--output-dir")
        .arg(output_dir.path())
        .write_stdin(input)
        .assert()
        .success();

    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    assert_eq!(
        entity_csv.lines().count(),
        4,
        "Expected 3 music entities + header"
    );
    assert!(entity_csv.contains("Autechre"));
}

#[test]
fn missing_input_fails() {
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("nonexistent.json")
        .assert()
        .failure();
}
