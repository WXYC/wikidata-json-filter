//! WX-1.2.10 detector for the CSV → PG COPY byte path.
//!
//! Drives the cross-repo `@wxyc/shared` charset-torture corpus through
//! `import::import_all` against a real PG schema, then SELECTs back and
//! asserts byte equality on the `entity.label` column. Uses the
//! wxyc-etl#79 EXPECTED_FAILURES + unexpected-pass detector pattern.
//!
//! See WXYC/docs#15 for the WX-1 plan.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use postgres::{Client, NoTls};
use serde::Deserialize;

use wikidata_cache::{import, import_schema};

#[derive(Deserialize, Debug)]
struct CorpusEntry {
    input: String,
    notes: String,
}

#[derive(Deserialize)]
struct Corpus {
    categories: HashMap<String, Vec<CorpusEntry>>,
}

fn load_corpus() -> Corpus {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/charset-torture.json");
    let bytes = std::fs::read(&path).expect("vendored corpus exists");
    serde_json::from_slice(&bytes).expect("corpus is valid JSON")
}

/// Inputs whose CSV→COPY round-trip cannot succeed today.
fn expected_failures() -> HashMap<&'static str, &'static str> {
    HashMap::new()
}

const TEST_DB_URL: &str =
    "host=localhost port=5435 user=wikidata password=wikidata dbname=wikidata_test";

static DB_LOCK: Mutex<()> = Mutex::new(());

const CSV_FILES: &[(&str, &str)] = &[
    ("entity.csv", "qid,label,description,entity_type"),
    ("discogs_mapping.csv", "qid,property,discogs_id"),
    ("influence.csv", "source_qid,target_qid"),
    ("genre.csv", "entity_qid,genre_qid"),
    ("record_label.csv", "artist_qid,label_qid"),
    ("label_hierarchy.csv", "child_qid,parent_qid"),
    ("entity_alias.csv", "qid,alias"),
    ("occupation.csv", "entity_qid,occupation_qid"),
];

fn csv_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push_str("\"\"");
        } else {
            out.push(c);
        }
    }
    out.push('"');
    out
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored
fn corpus_csv_pg_roundtrip() {
    let _guard = DB_LOCK.lock().unwrap();
    let corpus = load_corpus();
    let known_failures = expected_failures();

    let entries: Vec<(usize, &str, &str, &str)> = corpus
        .categories
        .iter()
        .flat_map(|(category, entries)| {
            entries
                .iter()
                .map(move |e| (category.as_str(), e.input.as_str(), e.notes.as_str()))
        })
        .enumerate()
        .map(|(i, (cat, input, notes))| (i + 1, cat, input, notes))
        .collect();

    let tmp = tempfile::tempdir().expect("tempdir");

    // Build entity.csv with one row per corpus entry. NUL (U+0000) is
    // carried through the CSV layer; the PG COPY layer strips it per
    // the WX-3.B boundary policy (WXYC/docs#18).
    let mut entity_csv = String::from("qid,label,description,entity_type\n");
    let mut written: Vec<(usize, &str)> = Vec::new();
    for (id, _, input, _) in &entries {
        let qid = format!("Q{}", id);
        entity_csv.push_str(&qid);
        entity_csv.push(',');
        entity_csv.push_str(&csv_quote(input));
        entity_csv.push(',');
        entity_csv.push_str("\"\""); // empty description
        entity_csv.push(',');
        entity_csv.push_str("human");
        entity_csv.push('\n');
        written.push((*id, *input));
    }
    std::fs::write(tmp.path().join("entity.csv"), entity_csv).unwrap();

    // The other 7 CSVs need to exist with headers only.
    for (file, header) in CSV_FILES.iter().skip(1) {
        std::fs::write(tmp.path().join(file), format!("{header}\n")).unwrap();
    }

    let mut client = Client::connect(TEST_DB_URL, NoTls).expect("connect");
    import_schema::drop_schema(&mut client).unwrap();
    import_schema::create_schema(&mut client).unwrap();

    let imported = import::import_all(&mut client, tmp.path()).expect("import_all");
    assert_eq!(
        imported as usize,
        written.len(),
        "import row count mismatch"
    );

    let mut unexpected_failures: Vec<String> = Vec::new();
    let mut unexpected_passes: Vec<String> = Vec::new();

    for (id, category, input, notes) in &entries {
        let known = known_failures.get(input).copied();
        let qid = format!("Q{}", id);
        let row = client
            .query_opt("SELECT label FROM entity WHERE qid = $1", &[&qid])
            .ok()
            .flatten();
        let actual: Option<String> = row.map(|r| r.get(0));

        // WX-3.B: U+0000 is stripped at the PG TEXT write boundary
        // (WXYC/docs#18), so the expected stored value is the input
        // with NUL bytes removed.
        let expected: String = input.replace('\0', "");
        let passed = actual.as_deref() == Some(expected.as_str());
        match (passed, known) {
            (true, None) => {}
            (true, Some(_tag)) => {
                unexpected_passes.push(format!(
                    "{category}: {input:?} now round-trips; remove from EXPECTED_FAILURES"
                ));
            }
            (false, Some(_tag)) => {}
            (false, None) => {
                unexpected_failures.push(format!(
                    "{category}: {input:?} -> {actual:?}\n    notes: {notes}"
                ));
            }
        }
    }

    let mut report = String::new();
    if !unexpected_failures.is_empty() {
        report.push_str(&format!(
            "\nUnexpected failures ({}):\n  {}\n",
            unexpected_failures.len(),
            unexpected_failures.join("\n  ")
        ));
    }
    if !unexpected_passes.is_empty() {
        report.push_str(&format!(
            "\nUnexpected passes ({}):\n  {}\n",
            unexpected_passes.len(),
            unexpected_passes.join("\n  ")
        ));
    }
    assert!(report.is_empty(), "{report}");
}
