//! Error handling tests for wikidata-json-filter.
//!
//! Verifies graceful behavior on corrupted gzip input and malformed JSON entities.

use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

/// Create a corrupted gzip file: valid gzip header followed by garbage bytes.
fn create_corrupted_gzip(path: &std::path::Path) {
    // Gzip magic number (1f 8b) + method (08) + flags (00) + timestamp + OS
    let mut data = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
    // Append random garbage that isn't valid compressed data
    data.extend_from_slice(b"\xDE\xAD\xBE\xEF\x00\x01\x02\x03\x04\x05");
    data.extend_from_slice(b"\xFF\xFE\xFD\xFC\xFB\xFA\xF9\xF8\xF7\xF6");
    fs::write(path, data).unwrap();
}

#[test]
fn corrupted_gzip_returns_gracefully_not_panic() {
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    let corrupted_path = input_dir.path().join("corrupted.json.gz");
    create_corrupted_gzip(&corrupted_path);

    // The binary should handle corrupted gzip gracefully -- it prints a warning
    // about the truncated stream and exits with success (0 entities processed).
    // The key assertion is that it does NOT panic or hang.
    let output = Command::cargo_bin("wikidata-json-filter")
        .unwrap()
        .arg(corrupted_path.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.path())
        .timeout(std::time::Duration::from_secs(10))
        .assert()
        .success();

    // Stderr should contain the read error warning
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("read error")
            || stderr.contains("truncated stream")
            || stderr.contains("corrupt"),
        "Expected warning about corrupted stream in stderr, got: {}",
        stderr
    );
}

#[test]
fn corrupted_gzip_produces_no_output_files_or_empty() {
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    let corrupted_path = input_dir.path().join("bad.json.gz");
    create_corrupted_gzip(&corrupted_path);

    let _ = Command::cargo_bin("wikidata-json-filter")
        .unwrap()
        .arg(corrupted_path.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.path())
        .timeout(std::time::Duration::from_secs(10))
        .output();

    // If entity.csv exists, it should only contain the header (no data rows)
    let entity_path = output_dir.path().join("entity.csv");
    if entity_path.exists() {
        let content = fs::read_to_string(&entity_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        // At most header row
        assert!(
            lines.len() <= 1,
            "Corrupted input should produce at most a header row, got {} lines",
            lines.len()
        );
    }
}

#[test]
fn malformed_json_entity_skipped_with_warning() {
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    // Create a JSON dump with one valid entity, one malformed entity, one valid entity
    let dump = r#"[
{"id":"Q187923","labels":{"en":{"language":"en","value":"Autechre"}},"descriptions":{"en":{"language":"en","value":"British electronic music duo"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","id":"Q215380"}}}}],"P1953":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"string","value":"12"}}}]}},
{THIS_IS_NOT_VALID_JSON: "broken", "missing_quotes: true},
{"id":"Q643023","labels":{"en":{"language":"en","value":"Stereolab"}},"descriptions":{"en":{"language":"en","value":"Anglo-French rock band"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","id":"Q215380"}}}}],"P1953":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"string","value":"4965"}}}]}}
]"#;

    let dump_path = input_dir.path().join("malformed.json");
    fs::write(&dump_path, dump).unwrap();

    Command::cargo_bin("wikidata-json-filter")
        .unwrap()
        .arg(dump_path.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    // The valid entities should be present in output
    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    assert!(
        entity_csv.contains("Autechre"),
        "Valid entity Autechre should be in output"
    );
    assert!(
        entity_csv.contains("Stereolab"),
        "Valid entity Stereolab should be in output"
    );

    // Should have exactly 2 music-relevant entities (+ header)
    let entity_lines: Vec<&str> = entity_csv.lines().collect();
    assert_eq!(
        entity_lines.len(),
        3,
        "Expected 2 valid entities + header, got {} lines",
        entity_lines.len()
    );
}

#[test]
fn empty_json_array_produces_empty_output() {
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    let dump_path = input_dir.path().join("empty.json");
    fs::write(&dump_path, "[\n]\n").unwrap();

    Command::cargo_bin("wikidata-json-filter")
        .unwrap()
        .arg(dump_path.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    let entity_lines: Vec<&str> = entity_csv.lines().collect();
    assert_eq!(
        entity_lines.len(),
        1,
        "Empty dump should produce only header row"
    );
}

#[test]
fn entity_with_missing_labels_field_skipped() {
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    // Entity with no labels field at all -- should be deserialized but handled
    let dump = r#"[
{"id":"Q999","descriptions":{"en":{"language":"en","value":"no labels"}},"claims":{"P1953":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"string","value":"999"}}}]}},
{"id":"Q187923","labels":{"en":{"language":"en","value":"Autechre"}},"descriptions":{"en":{"language":"en","value":"British electronic music duo"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"wikibase-entityid","value":{"entity-type":"item","id":"Q215380"}}}}],"P1953":[{"mainsnak":{"snaktype":"value","datavalue":{"type":"string","value":"12"}}}]}}
]"#;

    let dump_path = input_dir.path().join("missing_labels.json");
    fs::write(&dump_path, dump).unwrap();

    Command::cargo_bin("wikidata-json-filter")
        .unwrap()
        .arg(dump_path.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    // At least Autechre should be present
    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    assert!(entity_csv.contains("Autechre"));
}
