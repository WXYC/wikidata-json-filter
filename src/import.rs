//! CSV import module: reads the 8 CSV files produced by wikidata-cache
//! and streams them into PostgreSQL via COPY.

use anyhow::{Context, Result};
use postgres::Client;
use std::path::Path;

/// COPY statement for each table, keyed by table name.
fn copy_stmt(table: &str, columns: &[&str]) -> String {
    let cols = columns.join(", ");
    format!("COPY {table} ({cols}) FROM STDIN WITH (FORMAT text)")
}

/// Table definitions: (table_name, csv_filename, columns).
const TABLE_DEFS: &[(&str, &str, &[&str])] = &[
    (
        "entity",
        "entity.csv",
        &["qid", "label", "description", "entity_type"],
    ),
    (
        "discogs_mapping",
        "discogs_mapping.csv",
        &["qid", "property", "discogs_id"],
    ),
    ("influence", "influence.csv", &["source_qid", "target_qid"]),
    ("genre", "genre.csv", &["entity_qid", "genre_qid"]),
    (
        "record_label",
        "record_label.csv",
        &["artist_qid", "label_qid"],
    ),
    (
        "label_hierarchy",
        "label_hierarchy.csv",
        &["child_qid", "parent_qid"],
    ),
    ("entity_alias", "entity_alias.csv", &["qid", "alias"]),
    (
        "occupation",
        "occupation.csv",
        &["entity_qid", "occupation_qid"],
    ),
];

/// Escape a string value for PostgreSQL COPY TEXT format.
///
/// Handles backslash, tab, newline, and carriage return. Drops U+0000
/// (NUL) silently — PostgreSQL TEXT cannot store it (SQL standard), and
/// per the org-wide WX-3.B policy (WXYC/docs#18) we strip it at every
/// PG TEXT write boundary. NUL in artist/title metadata is always a
/// corruption signal, never intentional.
fn escape_copy_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\0' => {} // strip at boundary (WXYC/docs#18)
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

/// Import a single CSV file into the corresponding PostgreSQL table via COPY.
fn import_csv(
    client: &mut Client,
    csv_dir: &Path,
    table: &str,
    csv_file: &str,
    columns: &[&str],
) -> Result<u64> {
    let path = csv_dir.join(csv_file);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .with_context(|| format!("Failed to open {}", path.display()))?;

    let stmt = copy_stmt(table, columns);
    let mut writer = client.copy_in(&stmt)?;

    let mut count: u64 = 0;
    for result in rdr.records() {
        let record =
            result.with_context(|| format!("Failed to read CSV record from {csv_file}"))?;

        let mut line = String::new();
        for (i, field) in record.iter().enumerate() {
            if i > 0 {
                line.push('\t');
            }
            if field.is_empty() {
                line.push_str("\\N");
            } else {
                line.push_str(&escape_copy_text(field));
            }
        }
        line.push('\n');

        use std::io::Write;
        writer.write_all(line.as_bytes())?;
        count += 1;
    }

    writer.finish()?;
    Ok(count)
}

/// Import all 8 CSV files from a directory into PostgreSQL.
///
/// Tables are imported in FK order (entity first, then child tables).
/// Returns the total number of rows imported across all tables.
pub fn import_all(client: &mut Client, csv_dir: &Path) -> Result<u64> {
    let mut total = 0u64;
    for &(table, csv_file, columns) in TABLE_DEFS {
        let count = import_csv(client, csv_dir, table, csv_file, columns)?;
        log::info!("Imported {count} rows into {table}");
        total += count;
    }
    Ok(total)
}
