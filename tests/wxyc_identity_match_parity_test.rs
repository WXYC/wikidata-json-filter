//! Parity check for the four `wxyc_identity_match_*` plpgsql functions
//! deployed by `migrations/0003_wxyc_identity_match_functions.sql`.
//!
//! Three independent assertions:
//!
//! 1. **Pin freshness** — `vendor/wxyc-etl/wxyc_unaccent.rules`,
//!    `vendor/wxyc-etl/wxyc_identity_match_functions.sql`, and
//!    `tests/fixtures/identity_normalization_cases.csv` hash to the SHA-256
//!    values recorded in `wxyc-etl-pin.txt`. If any vendored file drifts
//!    from the pin, fail with a re-vendoring hint.
//! 2. **Migration freshness** — `migrations/0003_wxyc_identity_match_functions.sql`
//!    ends with the canonical SQL byte-for-byte. The prefix is the
//!    wrapper (CREATE EXTENSION + CREATE DICTIONARY) that sqlx-cli needs
//!    because it can't `\i` external files.
//! 3. **Postgres byte-equality** (`#[ignore]`-gated, runs in CI's
//!    test-postgres job) — each of the 252 fixture rows is fed through
//!    the corresponding plpgsql function on the live PG service; the
//!    result must match the fixture's `expected` column. Implicit
//!    Rust↔PG parity: the fixture IS the Rust-validated reference.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &str) -> Vec<u8> {
    let mut p = repo_root();
    p.push(path);
    fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

fn pin_map() -> HashMap<String, String> {
    let bytes = read("wxyc-etl-pin.txt");
    let text = String::from_utf8(bytes).expect("pin file is UTF-8");
    let mut m = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            m.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    m
}

#[test]
fn pin_file_sha256s_match_vendored_files() {
    let pin = pin_map();
    let pairs = [
        (
            "vendor/wxyc-etl/wxyc_unaccent.rules",
            "unaccent_rules_sha256",
        ),
        (
            "vendor/wxyc-etl/wxyc_identity_match_functions.sql",
            "functions_sql_sha256",
        ),
        (
            "tests/fixtures/identity_normalization_cases.csv",
            "fixture_csv_sha256",
        ),
    ];
    for (path, key) in pairs {
        let actual = sha256_hex(&read(path));
        let expected = pin
            .get(key)
            .unwrap_or_else(|| panic!("missing pin entry {key:?}"));
        assert_eq!(
            &actual,
            expected,
            "{path} drifted from pin {key} — re-vendor from wxyc-etl@v{} and bump wxyc-etl-pin.txt",
            pin.get("wxyc_etl_version")
                .map(String::as_str)
                .unwrap_or("?")
        );
    }
}

/// Sentinel emitted by the wrapper prelude just before the canonical body.
/// Anchoring the split here (rather than the first line of the canonical)
/// prevents a future wrapper edit that happens to paste that first line from
/// silently moving the split point.
const CANONICAL_SENTINEL: &str = "-- @BEGIN CANONICAL BODY (do not edit; vendored from wxyc-etl)\n";

#[test]
fn migration_inlines_canonical_sql_byte_for_byte() {
    let migration = read("migrations/0003_wxyc_identity_match_functions.sql");
    let canonical = read("vendor/wxyc-etl/wxyc_identity_match_functions.sql");
    let migration = String::from_utf8(migration).expect("migration is UTF-8");
    let canonical = String::from_utf8(canonical).expect("canonical SQL is UTF-8");
    let sentinel_idx = migration.find(CANONICAL_SENTINEL).unwrap_or_else(|| {
        panic!(
            "migration is missing the `{}` sentinel that marks the start of the vendored canonical body — re-generate the migration by appending the sentinel + vendor/wxyc-etl/wxyc_identity_match_functions.sql to the wrapper prelude",
            CANONICAL_SENTINEL.trim_end()
        )
    });
    let body_start = sentinel_idx + CANONICAL_SENTINEL.len();
    assert_eq!(
        &migration[body_start..],
        canonical,
        "migration body after the @BEGIN CANONICAL BODY sentinel diverges from vendor/wxyc-etl/wxyc_identity_match_functions.sql — re-vendor and regenerate"
    );
}

// -- live-PG fixture parity -------------------------------------------------

use postgres::Client;

#[derive(Debug)]
struct Row {
    line_no: usize,
    input: String,
    expected: String,
    variant: String,
    category: String,
}

fn fixture_rows() -> Vec<Row> {
    let bytes = read("tests/fixtures/identity_normalization_cases.csv");
    let text = String::from_utf8(bytes).expect("fixture UTF-8");
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line_no = i + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if line_no == 1 && trimmed.starts_with("input,") {
            continue;
        }
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 5, "line {line_no} fields={}", fields.len());
        rows.push(Row {
            line_no,
            input: fields[0].clone(),
            expected: fields[1].clone(),
            variant: fields[2].clone(),
            category: fields[3].clone(),
        });
    }
    rows
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut iter = line.chars().peekable();
    while let Some(c) = iter.next() {
        match (c, in_quotes) {
            ('"', true) => {
                if iter.peek() == Some(&'"') {
                    field.push('"');
                    iter.next();
                } else {
                    in_quotes = false;
                }
            }
            ('"', false) => in_quotes = true,
            (',', false) => out.push(std::mem::take(&mut field)),
            (other, _) => field.push(other),
        }
    }
    out.push(field);
    out
}

fn pg_function(variant: &str) -> &'static str {
    match variant {
        "base" => "wxyc_identity_match_artist",
        "title" => "wxyc_identity_match_title",
        "punct" => "wxyc_identity_match_with_punctuation",
        "disamb" => "wxyc_identity_match_with_disambiguator_strip",
        other => panic!("unknown variant {other:?}"),
    }
}

fn apply_migration(client: &mut Client) {
    let migration = read("migrations/0003_wxyc_identity_match_functions.sql");
    let sql = String::from_utf8(migration).expect("migration UTF-8");
    client
        .batch_execute(&sql)
        .expect("apply 0003_wxyc_identity_match_functions migration");
}

#[test]
#[ignore]
fn postgres_functions_match_fixture_row_for_row() {
    let Ok(db_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL unset — skipping");
        return;
    };
    let mut client = Client::connect(&db_url, postgres::NoTls).expect("connect to test PG");
    apply_migration(&mut client);

    let rows = fixture_rows();
    assert!(rows.len() >= 250, "fixture row count {} < 250", rows.len());

    let mut failures: Vec<String> = Vec::new();
    for row in &rows {
        let fn_name = pg_function(&row.variant);
        let pg_row = client
            .query_one(&format!("SELECT {fn_name}($1)"), &[&row.input])
            .unwrap_or_else(|e| {
                panic!(
                    "PG query failed line {} ({fn_name}, input={:?}): {e:?}",
                    row.line_no, row.input
                )
            });
        let pg_out: Option<String> = pg_row.get(0);
        let pg_out = pg_out.unwrap_or_default();
        if pg_out != row.expected {
            failures.push(format!(
                "  line {} [{}/{}] input={:?}\n    expected={:?}\n          pg={:?}",
                row.line_no, row.variant, row.category, row.input, row.expected, pg_out
            ));
        }
    }
    if !failures.is_empty() {
        panic!(
            "{} of {} parity rows failed:\n{}",
            failures.len(),
            rows.len(),
            failures.join("\n")
        );
    }
}

#[test]
#[ignore]
fn migration_double_apply_is_a_no_op() {
    // Re-applying the whole migration must not throw and must leave the
    // functions in the same state. CREATE OR REPLACE FUNCTION + DROP/CREATE
    // TEXT SEARCH DICTIONARY are individually idempotent, but proving they
    // compose cleanly when the migration is replayed end-to-end pins the
    // contract this template makes to every consumer.
    let Ok(db_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL unset — skipping");
        return;
    };
    let mut client = Client::connect(&db_url, postgres::NoTls).expect("connect to test PG");
    apply_migration(&mut client);
    apply_migration(&mut client);
    let row = client
        .query_one("SELECT wxyc_identity_match_artist('Stereolab')", &[])
        .expect("query after double-apply");
    let got: Option<String> = row.get(0);
    assert_eq!(got.as_deref(), Some("stereolab"));
}

#[test]
#[ignore]
fn postgres_functions_idempotent() {
    let Ok(db_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL unset — skipping");
        return;
    };
    let mut client = Client::connect(&db_url, postgres::NoTls).expect("connect to test PG");
    apply_migration(&mut client);

    let probe = "   The Foo Fighters (1995)   ";
    for fn_name in [
        "wxyc_identity_match_artist",
        "wxyc_identity_match_title",
        "wxyc_identity_match_with_punctuation",
        "wxyc_identity_match_with_disambiguator_strip",
    ] {
        let once_row = client
            .query_one(&format!("SELECT {fn_name}($1)"), &[&probe])
            .unwrap_or_else(|e| panic!("{fn_name} call 1 failed: {e:?}"));
        let once: Option<String> = once_row.get(0);
        let once = once.expect("non-null");
        let twice_row = client
            .query_one(&format!("SELECT {fn_name}($1)"), &[&once])
            .unwrap_or_else(|e| panic!("{fn_name} call 2 failed: {e:?}"));
        let twice: Option<String> = twice_row.get(0);
        assert_eq!(
            twice.as_deref(),
            Some(once.as_str()),
            "{fn_name} not idempotent: once={once:?} twice={twice:?}"
        );
    }
}
