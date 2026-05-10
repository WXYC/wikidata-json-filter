//! PostgreSQL schema management for wikidata-cache.
//!
//! Reads `schema/create_database.sql` and applies it to a PostgreSQL database.
//! Provides table name constants and admin operations (UNLOGGED toggle, VACUUM).

use anyhow::Result;
use postgres::Client;

/// The DDL SQL embedded from `schema/create_database.sql`.
pub const DDL: &str = include_str!("../schema/create_database.sql");

/// All wikidata-cache tables in FK-safe import order (parent first).
///
/// The first 8 entries are the streaming-filter output tables. `wxyc_library`
/// is the cross-cache identity hook (E1 §4.1.3, see
/// `migrations/0002_wxyc_library_v2.sql`) — it has no FK relationship to
/// the other tables, so order is irrelevant; placed last because it's
/// loaded by the separate `import-wxyc-library` subcommand rather than the
/// CSV `import` subcommand.
pub const ALL_TABLES: &[&str] = &[
    "entity",
    "discogs_mapping",
    "influence",
    "genre",
    "record_label",
    "label_hierarchy",
    "entity_alias",
    "occupation",
    "wxyc_library",
];

/// Apply the wikidata-cache schema DDL to the database.
///
/// Handles the pg_trgm extension creation separately to tolerate concurrent
/// connections racing on `CREATE EXTENSION IF NOT EXISTS`.
pub fn create_schema(client: &mut Client) -> Result<()> {
    // Create extension separately -- concurrent CREATE EXTENSION IF NOT EXISTS
    // can race and produce a unique constraint violation on pg_extension_name_index.
    // We tolerate that error since it means the extension already exists.
    if let Err(e) = client.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_trgm") {
        let msg = e.to_string();
        if !msg.contains("pg_extension_name_index") && !msg.contains("already exists") {
            return Err(e.into());
        }
    }

    // Apply the rest of the DDL (skip the CREATE EXTENSION line)
    let ddl_without_extension = DDL
        .lines()
        .filter(|line| !line.starts_with("CREATE EXTENSION"))
        .collect::<Vec<_>>()
        .join("\n");
    client.batch_execute(&ddl_without_extension)?;
    Ok(())
}

/// Drop all wikidata-cache tables (in reverse FK order).
pub fn drop_schema(client: &mut Client) -> Result<()> {
    for table in ALL_TABLES.iter().rev() {
        client.batch_execute(&format!("DROP TABLE IF EXISTS {table} CASCADE"))?;
    }
    Ok(())
}

/// Truncate all tables (in reverse FK order) for idempotent re-import.
pub fn truncate_all(client: &mut Client) -> Result<()> {
    for table in ALL_TABLES.iter().rev() {
        client.batch_execute(&format!("TRUNCATE {table} CASCADE"))?;
    }
    Ok(())
}

/// Set tables to UNLOGGED mode for faster bulk import (disables WAL).
///
/// Processes child tables first (reverse FK order), then parent tables,
/// because a logged table cannot reference an unlogged table.
pub fn set_tables_unlogged(client: &mut Client) -> Result<()> {
    for table in ALL_TABLES.iter().rev() {
        client.batch_execute(&format!("ALTER TABLE {table} SET UNLOGGED"))?;
    }
    Ok(())
}

/// Restore tables to LOGGED mode (re-enables WAL durability).
///
/// Processes parent tables first (FK order) so child tables can reference them.
pub fn set_tables_logged(client: &mut Client) -> Result<()> {
    for table in ALL_TABLES {
        client.batch_execute(&format!("ALTER TABLE {table} SET LOGGED"))?;
    }
    Ok(())
}

/// Run VACUUM FULL on all tables to reclaim space after bulk import.
pub fn vacuum_full(client: &mut Client) -> Result<()> {
    for table in ALL_TABLES {
        client.batch_execute(&format!("VACUUM FULL {table}"))?;
    }
    Ok(())
}
