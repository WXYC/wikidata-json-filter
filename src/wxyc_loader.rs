//! WXYC library hook loader for the Homebrew wikidata cache.
//!
//! Implements E1 §4.1.3 of the cross-cache-identity plan:
//! <https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#413-homebrew-wikidata-port-5435>
//!
//! Reads a SQLite `library.db` (a wxyc-catalog export of the WXYC library
//! catalog) and populates the consolidated `wxyc_library` PostgreSQL table.
//!
//! # Idempotency
//!
//! The writer uses `INSERT ... ON CONFLICT (library_id) DO NOTHING`, so
//! re-running the loader against an already-populated cache is a no-op for
//! existing library_ids. New rows are inserted; conflicts are silently
//! skipped. This matches the pattern used by `discogs-etl`'s
//! `loaders/wxyc.py::populate_wxyc_library_v2` (PR #185 / issue #178).
//!
//! # Normalization
//!
//! Per the plan §3.3 / E3 step 4, this loader is locked onto the canonical
//! identity normalizers from `wxyc-etl` 0.3.0:
//!
//! - [`wxyc_etl::text::to_identity_match_form`] — used for both
//!   `norm_artist` AND `norm_label` (labels share the artist-side pipeline;
//!   no `_label` variant exists or is needed).
//! - [`wxyc_etl::text::to_identity_match_form_title`] — title-side variant,
//!   used for `norm_title`.
//!
//! The opt-in variants (`_with_punctuation`, `_with_disambiguator_strip`)
//! are deliberately not invoked here — the cross-cache-identity hook stays
//! on the locked-on baseline so every consumer cache normalizes identically.
//! `wxyc_etl::text::to_match_form` (the WX-2 comparison form) is a different
//! normalizer and must NOT be substituted.
//!
//! # Nullability
//!
//! Per §3.1, `artist_id` / `label_id` / `format_id` / `release_year` are all
//! nullable. This cache reads from library.db (a SQLite catalog export)
//! which does not carry Backend's integer IDs; for the foreseeable future
//! every row from this loader stamps NULL on those four columns. They
//! exist for forward compatibility with a future Backend-direct loader.

use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use postgres::Client;
use wxyc_etl::text::{to_identity_match_form, to_identity_match_form_title};

/// Audit string surfaced in INFO logs and asserted by integration tests.
/// Pinning the string makes a future API rename in `wxyc-etl` observable.
pub const NORMALIZER_NAME: &str = "wxyc_etl::text::to_identity_match_form";

/// Allowed values for `wxyc_library.snapshot_source`. Mirrors the §3.1 CHECK
/// constraint at the loader-argument boundary so callers get a friendly
/// error instead of a Postgres CheckViolation buried in a transaction.
pub const ALLOWED_SNAPSHOT_SOURCES: &[&str] = &["backend", "tubafrenzy", "llm"];

/// One row read from `library.db`. Mirrors §3.1's column list. The four
/// nullable columns (`artist_id`, `label_id`, `format_id`, `release_year`)
/// are always `None` from this loader — see the module-level "Nullability"
/// note.
#[derive(Debug, Clone)]
pub struct LibraryRow {
    pub library_id: i32,
    pub artist_name: String,
    pub album_title: String,
    pub label_name: Option<String>,
    pub format_name: Option<String>,
    pub wxyc_genre: Option<String>,
    pub call_letters: Option<String>,
    pub call_numbers: Option<i32>,
}

/// Read every row from a SQLite `library.db` into [`LibraryRow`] records.
///
/// The minimal-fixture schema is `(id, artist, title)`; the production schema
/// adds `format`, `label`, `genre`, `call_letters`, `release_call_number`.
/// We adapt to whatever optional columns are present rather than failing —
/// the same shape as `discogs-etl/loaders/wxyc.py::_read_library_db`.
pub fn read_library_db(library_db: &Path) -> Result<Vec<LibraryRow>> {
    let conn = rusqlite::Connection::open(library_db)
        .with_context(|| format!("Failed to open {}", library_db.display()))?;

    let cols = existing_columns(&conn, "library")?;

    // Required columns. The PRAGMA-driven approach below is forgiving of
    // schema drift; if `id` / `artist` / `title` are missing the SELECT
    // itself will surface the problem with a clear SQLite error.
    let mut select_parts: Vec<&str> = vec!["id", "artist", "title"];
    for c in &[
        "label",
        "format",
        "genre",
        "call_letters",
        "release_call_number",
    ] {
        if cols.contains(*c) {
            select_parts.push(c);
        }
    }
    let query = format!("SELECT {} FROM library", select_parts.join(", "));

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let mut r = LibraryRow {
            library_id: row.get::<_, i64>("id")? as i32,
            artist_name: row.get("artist")?,
            album_title: row.get("title")?,
            label_name: None,
            format_name: None,
            wxyc_genre: None,
            call_letters: None,
            call_numbers: None,
        };
        if cols.contains("label") {
            r.label_name = row.get("label").ok();
        }
        if cols.contains("format") {
            r.format_name = row.get("format").ok();
        }
        if cols.contains("genre") {
            r.wxyc_genre = row.get("genre").ok();
        }
        if cols.contains("call_letters") {
            r.call_letters = row.get("call_letters").ok();
        }
        if cols.contains("release_call_number") {
            r.call_numbers = row
                .get::<_, Option<i64>>("release_call_number")
                .ok()
                .flatten()
                .map(|v| v as i32);
        }
        Ok(r)
    })?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

fn existing_columns(
    conn: &rusqlite::Connection,
    table: &str,
) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut set = std::collections::HashSet::new();
    for n in names {
        set.insert(n?);
    }
    Ok(set)
}

/// Strip NUL bytes (U+0000) from a TEXT value at the PostgreSQL write
/// boundary, matching the org-wide WX-3.B policy ([WXYC/docs#18]) and
/// `import.rs::escape_copy_text`. PostgreSQL TEXT cannot store NUL; in
/// library metadata it's always corruption, never intentional signal.
///
/// [WXYC/docs#18]: https://github.com/WXYC/docs/issues/18
fn strip_pg_null_bytes(s: &str) -> String {
    s.chars().filter(|c| *c != '\0').collect()
}

fn strip_pg_null_bytes_opt(s: Option<&str>) -> Option<String> {
    s.map(strip_pg_null_bytes)
}

/// Identity-tier normalization for the optional `norm_label` column.
///
/// `to_identity_match_form` returns an empty string for empty input; we want
/// NULL to flow through to PostgreSQL for the nullable `norm_label` column,
/// so re-introduce the None at the boundary.
fn norm_label(value: Option<&str>) -> Option<String> {
    value.map(to_identity_match_form)
}

/// Populate `wxyc_library` from a SQLite `library.db`.
///
/// Per E1 §4.1.3 + §3.1: every library row is written (Option B; no filter).
/// Idempotent on `library_id` (`ON CONFLICT DO NOTHING`).
///
/// `snapshot_source` MUST be one of `backend` | `tubafrenzy` | `llm` per
/// §3.1; the function returns an error otherwise (mirrors the database-side
/// CHECK constraint at the loader boundary so the error message is
/// human-readable).
///
/// Returns the number of rows attempted (pre-conflict). With a clean target
/// table this equals the row count of `library.db`; on a re-run the report
/// is identical but `COUNT(*)` in `wxyc_library` does not change.
pub fn populate_wxyc_library_v2(
    client: &mut Client,
    library_db: &Path,
    snapshot_source: &str,
) -> Result<u64> {
    if !ALLOWED_SNAPSHOT_SOURCES.contains(&snapshot_source) {
        anyhow::bail!(
            "snapshot_source must be one of {:?}, got {snapshot_source:?}",
            ALLOWED_SNAPSHOT_SOURCES
        );
    }

    let rows = read_library_db(library_db)?;
    if rows.is_empty() {
        log::warn!(
            "populate_wxyc_library_v2: no rows from {}",
            library_db.display()
        );
        return Ok(0);
    }

    // Single timestamp for the whole snapshot — cross-cache freshness is
    // observable via the spread of snapshot_at across caches, so all rows
    // from one load should share an instant.
    let snapshot_at = SystemTime::now();

    // Use a single prepared statement and a transaction. The cache is small
    // (≤64K rows) so a per-row INSERT inside a tx is plenty; if this ever
    // becomes a bottleneck the right move is `COPY ... FROM STDIN` like
    // `import.rs::import_csv`, but ON CONFLICT semantics complicate that.
    let stmt_sql = "
        INSERT INTO wxyc_library (
            library_id, artist_id, artist_name, album_title,
            label_id, label_name, format_id, format_name,
            wxyc_genre, call_letters, call_numbers, release_year,
            norm_artist, norm_title, norm_label,
            snapshot_at, snapshot_source
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11, $12,
            $13, $14, $15,
            $16, $17
        )
        ON CONFLICT (library_id) DO NOTHING
    ";

    let mut tx = client.transaction()?;
    let stmt = tx.prepare(stmt_sql)?;

    let mut attempted: u64 = 0;
    for r in &rows {
        // Strip NUL bytes BEFORE normalization so derived columns inherit the
        // PG-safe form. Reversing the order would let a NUL byte in the source
        // pass through into norm_artist / norm_title / norm_label and crash
        // the INSERT — every TEXT column that hits PostgreSQL must have been
        // stripped, including the derived ones.
        let artist_name = strip_pg_null_bytes(&r.artist_name);
        let album_title = strip_pg_null_bytes(&r.album_title);
        let label_name = strip_pg_null_bytes_opt(r.label_name.as_deref());
        let format_name = strip_pg_null_bytes_opt(r.format_name.as_deref());
        let wxyc_genre = strip_pg_null_bytes_opt(r.wxyc_genre.as_deref());
        let call_letters = strip_pg_null_bytes_opt(r.call_letters.as_deref());

        // norm_artist / norm_title are NOT NULL per §3.1; the normalizer
        // collapses to a non-empty string for any non-empty input. If it
        // ever returns an empty string for a real artist/title, that's a
        // bug worth crashing on — no `or ""` fallback.
        let norm_artist = to_identity_match_form(&artist_name);
        let norm_title = to_identity_match_form_title(&album_title);
        let norm_label_v = norm_label(label_name.as_deref());

        // For this cache today, every row stamps NULL on artist_id /
        // label_id / format_id / release_year — see the module-level
        // "Nullability" note.
        let artist_id: Option<i32> = None;
        let label_id: Option<i32> = None;
        let format_id: Option<i32> = None;
        let release_year: Option<i16> = None;

        tx.execute(
            &stmt,
            &[
                &r.library_id,
                &artist_id,
                &artist_name,
                &album_title,
                &label_id,
                &label_name,
                &format_id,
                &format_name,
                &wxyc_genre,
                &call_letters,
                &r.call_numbers,
                &release_year,
                &norm_artist,
                &norm_title,
                &norm_label_v,
                &snapshot_at,
                &snapshot_source,
            ],
        )?;
        attempted += 1;
    }
    tx.commit()?;

    log::info!(
        "populate_wxyc_library_v2: wrote {} rows to wxyc_library (snapshot_source={}, normalizer={})",
        attempted,
        snapshot_source,
        NORMALIZER_NAME,
    );
    Ok(attempted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_pg_null_bytes_drops_nul() {
        assert_eq!(strip_pg_null_bytes("a\0b"), "ab");
        assert_eq!(strip_pg_null_bytes("\0a\0b\0"), "ab");
    }

    #[test]
    fn strip_pg_null_bytes_clean_input_unchanged() {
        assert_eq!(strip_pg_null_bytes("Stereolab"), "Stereolab");
    }

    #[test]
    fn norm_label_passes_none_through() {
        assert_eq!(norm_label(None), None);
    }

    #[test]
    fn norm_label_normalizes_some() {
        // to_identity_match_form lowercases + collapses; full algorithmic
        // pin lives in the integration tests against PG.
        let v = norm_label(Some("Sonamos"));
        assert_eq!(v.as_deref(), Some("sonamos"));
    }

    #[test]
    fn allowed_snapshot_sources_pinned() {
        assert_eq!(ALLOWED_SNAPSHOT_SOURCES, &["backend", "tubafrenzy", "llm"]);
    }

    /// `read_library_db` adapts to whichever optional columns happen to be
    /// present. The integration tests exercise the full-prod schema; this
    /// unit test pins the minimal-schema branch (`id, artist, title` only).
    /// Older library.db snapshots — and the smallest test fixtures — don't
    /// carry label / format / genre / call_letters / release_call_number,
    /// and the loader needs to handle those without a PRAGMA-keyed panic.
    #[test]
    fn read_library_db_minimal_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("library.db");

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE library (\
                id INTEGER PRIMARY KEY, \
                artist TEXT NOT NULL, \
                title TEXT NOT NULL\
            );\
            INSERT INTO library (id, artist, title) VALUES \
                (1, 'Juana Molina', 'DOGA'), \
                (2, 'Stereolab', 'Aluminum Tunes');",
        )
        .unwrap();
        drop(conn);

        let rows = read_library_db(&db_path).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].library_id, 1);
        assert_eq!(rows[0].artist_name, "Juana Molina");
        assert_eq!(rows[0].album_title, "DOGA");
        // Optional columns must be None when the source schema doesn't carry them.
        assert!(rows[0].label_name.is_none());
        assert!(rows[0].format_name.is_none());
        assert!(rows[0].wxyc_genre.is_none());
        assert!(rows[0].call_letters.is_none());
        assert!(rows[0].call_numbers.is_none());
    }
}
