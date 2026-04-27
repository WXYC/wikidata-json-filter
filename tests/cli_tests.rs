use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

#[test]
fn filters_small_dump() {
    let output_dir = TempDir::new().unwrap();

    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("build")
        .arg("tests/fixtures/small_dump.json")
        .arg("--data-dir")
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
        .arg("build")
        .arg("tests/fixtures/small_dump.json")
        .arg("--data-dir")
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
        .arg("build")
        .arg("-")
        .arg("--data-dir")
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
        .arg("build")
        .arg("nonexistent.json")
        .assert()
        .failure();
}

#[test]
fn build_subcommand_required() {
    // Running with no subcommand should fail; this is a breaking change from
    // the old default-mode CLI.
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("tests/fixtures/small_dump.json")
        .assert()
        .failure();
}

#[test]
fn deprecated_output_dir_alias_still_works() {
    let output_dir = TempDir::new().unwrap();

    let assert = Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("build")
        .arg("tests/fixtures/small_dump.json")
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        stderr.contains("--output-dir is deprecated"),
        "Expected deprecation warning for --output-dir, got: {stderr}"
    );

    // Output should still land in the directory.
    let entity_csv = fs::read_to_string(output_dir.path().join("entity.csv")).unwrap();
    assert!(entity_csv.contains("Autechre"));
}

#[test]
fn entrypoint_emits_json_logs_with_repo_tag_and_no_sentry_dsn() {
    // With SENTRY_DSN unset, the binary must still start, run to completion, and
    // emit at least one JSON log line carrying the `repo: "wikidata-cache"` tag
    // on stderr (matching env_logger semantics; see wxyc-etl logger module).
    let output_dir = TempDir::new().unwrap();

    let assert = Command::cargo_bin("wikidata-cache")
        .unwrap()
        .env_remove("SENTRY_DSN")
        .env("RUST_LOG", "info")
        .arg("build")
        .arg("tests/fixtures/small_dump.json")
        .arg("--data-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();

    let json_line_with_repo = stderr.lines().find(|line| {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('{') {
            return false;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            return false;
        };
        value.to_string().contains("\"repo\":\"wikidata-cache\"")
    });

    assert!(
        json_line_with_repo.is_some(),
        "expected a JSON log line tagged repo=wikidata-cache on stderr; stderr was:\n{stderr}"
    );
}

#[test]
fn import_requires_database_url() {
    // Neither --database-url nor DATABASE_URL_WIKIDATA is set; should fail
    // with a clear error rather than panicking.
    Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("import")
        .arg("--data-dir")
        .arg(".")
        .env_remove("DATABASE_URL_WIKIDATA")
        .env_remove("DATABASE_URL")
        .assert()
        .failure()
        .stderr(predicates::str::contains("DATABASE_URL_WIKIDATA"));
}

#[test]
fn import_database_url_env_fallback_used() {
    // With a bogus URL via the env var, the binary should attempt to connect
    // (and fail at connect time, not at arg-parse time). The presence of a
    // connection error in stderr proves the env var was picked up.
    let assert = Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("import")
        .arg("--data-dir")
        .arg(".")
        .env(
            "DATABASE_URL_WIKIDATA",
            "postgresql://nobody:nobody@127.0.0.1:1/nope",
        )
        .env_remove("DATABASE_URL")
        .timeout(std::time::Duration::from_secs(15))
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    // The "missing database URL" error must NOT appear -- the env var supplied one.
    assert!(
        !stderr.contains("DATABASE_URL_WIKIDATA environment variable"),
        "env var fallback should have supplied a URL, but stderr says it was missing: {stderr}"
    );
    // Should fail at connect time, not at arg parsing.
    assert!(
        stderr.contains("PostgreSQL")
            || stderr.contains("connect")
            || stderr.contains("Connection refused"),
        "Expected a PostgreSQL connection error, got: {stderr}"
    );
}

#[test]
fn deprecated_csv_dir_alias_still_works() {
    // Pass a junk URL so we fail at connection time rather than arg parsing,
    // and inspect stderr for the deprecation warning.
    let assert = Command::cargo_bin("wikidata-cache")
        .unwrap()
        .arg("import")
        .arg("--csv-dir")
        .arg(".")
        .arg("--database-url")
        .arg("postgresql://nobody:nobody@127.0.0.1:1/nope")
        .timeout(std::time::Duration::from_secs(15))
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("--csv-dir is deprecated"),
        "Expected deprecation warning for --csv-dir, got: {stderr}"
    );
}
