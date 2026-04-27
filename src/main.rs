//! Streaming filter for Wikidata JSON dumps.
//!
//! Reads a gzipped Wikidata JSON dump, filters to music-relevant entities,
//! and writes flat CSV files for loading into PostgreSQL.
//!
//! Also provides an `import` subcommand to load the resulting CSV files
//! into PostgreSQL via COPY.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use flate2::read::MultiGzDecoder;
use std::ffi::OsString;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use wxyc_etl::logger::{self, LoggerConfig};
use wxyc_etl::pipeline::{self, BatchConfig};

use wikidata_cache::extractor::extract;
use wikidata_cache::filter::is_music_relevant;
use wikidata_cache::import;
use wikidata_cache::import_schema;
use wikidata_cache::model::Entity;
use wikidata_cache::writer::CsvOutput;

#[derive(Parser)]
#[command(name = "wikidata-cache")]
#[command(about = "Filter Wikidata JSON dumps to music-relevant entities")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to the Wikidata JSON dump (plain or .gz), or "-" for stdin
    input: Option<OsString>,

    /// Output directory for CSV files
    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    /// Stop after processing N entities (0 = no limit)
    #[arg(long, default_value = "0")]
    limit: u64,

    /// Log progress every N entities
    #[arg(long, default_value = "1000000")]
    progress_interval: u64,

    /// Force gzip decompression (auto-detected for .gz files, required for stdin)
    #[arg(long)]
    gzip: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Import CSV files into PostgreSQL
    Import {
        /// Directory containing the 8 CSV files produced by the filter
        #[arg(long, default_value = "output")]
        csv_dir: PathBuf,

        /// PostgreSQL connection string
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Drop and recreate the schema before importing
        #[arg(long)]
        fresh: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let (tool, step) = match cli.command {
        Some(Commands::Import { .. }) => ("wikidata-cache import", "import"),
        None => ("wikidata-cache build", "build"),
    };
    let _logger_guard = logger::init(LoggerConfig {
        repo: "wikidata-cache",
        tool,
        sentry_dsn: None,
        run_id: None,
    });

    // TODO: provision SENTRY_DSN in the runtime env (CI / deploy config) — see issue #13.

    let span = tracing::info_span!("run", repo = "wikidata-cache", tool = tool, step = step,);
    span.in_scope(|| {
        tracing::info!("starting");
        match cli.command {
            Some(Commands::Import {
                csv_dir,
                database_url,
                fresh,
            }) => run_import(&csv_dir, &database_url, fresh),
            None => {
                let input = cli
                    .input
                    .ok_or_else(|| anyhow::anyhow!("Input file is required for filter mode. Use `import` subcommand for CSV-to-PostgreSQL import."))?;
                run_filter(
                    input,
                    &cli.output_dir,
                    cli.limit,
                    cli.progress_interval,
                    cli.gzip,
                )
            }
        }
    })
}

fn run_import(csv_dir: &Path, database_url: &str, fresh: bool) -> Result<()> {
    let start = Instant::now();

    eprintln!("Connecting to PostgreSQL...");
    let mut client = postgres::Client::connect(database_url, postgres::NoTls)
        .context("Failed to connect to PostgreSQL")?;

    if fresh {
        eprintln!("Dropping existing schema...");
        import_schema::drop_schema(&mut client)?;
    }

    eprintln!("Creating schema (if not exists)...");
    import_schema::create_schema(&mut client)?;

    eprintln!("Setting tables to UNLOGGED for bulk import...");
    import_schema::set_tables_unlogged(&mut client)?;

    eprintln!("Truncating existing data...");
    import_schema::truncate_all(&mut client)?;

    eprintln!("Importing CSVs from {}...", csv_dir.display());
    let total = import::import_all(&mut client, csv_dir)?;

    eprintln!("Restoring tables to LOGGED...");
    import_schema::set_tables_logged(&mut client)?;

    eprintln!("Running VACUUM FULL...");
    import_schema::vacuum_full(&mut client)?;

    let elapsed = start.elapsed();
    eprintln!();
    eprintln!("Done in {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Total rows imported: {total:>10}");
    eprintln!("  Source directory:    {}", csv_dir.display());

    Ok(())
}

fn run_filter(
    input: OsString,
    output_dir: &Path,
    limit: u64,
    progress_interval: u64,
    gzip: bool,
) -> Result<()> {
    let start = Instant::now();

    // Open input: file path or "-" for stdin
    let is_stdin = input == "-";
    let use_gzip = gzip
        || (!is_stdin
            && PathBuf::from(&input)
                .extension()
                .is_some_and(|ext| ext == "gz"));

    // Stdin is read via /dev/stdin as a file so the reader is Send.
    let reader: Box<dyn BufRead + Send> = if is_stdin {
        let file = std::fs::File::open("/dev/stdin").context("Failed to open stdin")?;
        if use_gzip {
            Box::new(BufReader::with_capacity(
                8 * 1024 * 1024,
                MultiGzDecoder::new(file),
            ))
        } else {
            Box::new(BufReader::with_capacity(8 * 1024 * 1024, file))
        }
    } else {
        let path = PathBuf::from(&input);
        let file = std::fs::File::open(&path)
            .with_context(|| format!("Failed to open {}", path.display()))?;
        if use_gzip {
            Box::new(BufReader::with_capacity(
                8 * 1024 * 1024,
                MultiGzDecoder::new(file),
            ))
        } else {
            Box::new(BufReader::with_capacity(8 * 1024 * 1024, file))
        }
    };

    // Set up CSV output
    let mut output = CsvOutput::new(output_dir)
        .with_context(|| format!("Failed to create output in {}", output_dir.display()))?;

    // Scanner: read lines, strip JSON array delimiters, send byte batches
    let config = BatchConfig::default();
    let (rx, handle) = pipeline::start_scanner(
        move |tx| {
            let mut count: u64 = 0;

            for line_result in reader.lines() {
                let line = match line_result {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("Warning: read error (truncated stream?): {e}");
                        break;
                    }
                };
                let trimmed = line.trim();

                // Skip JSON array boundaries
                if trimmed == "[" || trimmed == "]" {
                    continue;
                }

                // Strip trailing comma
                let json_bytes = trimmed
                    .strip_suffix(',')
                    .unwrap_or(trimmed)
                    .as_bytes()
                    .to_vec();

                // Skip empty lines
                if json_bytes.is_empty() {
                    continue;
                }

                tx.send_item(json_bytes)?;
                count += 1;

                if progress_interval > 0 && count.is_multiple_of(progress_interval) {
                    log::info!("Scanned {count} entities...");
                }

                if limit > 0 && count >= limit {
                    break;
                }
            }

            Ok(count as usize)
        },
        config,
    );

    // Process: deserialize JSON, filter, extract — then write sequentially
    let stats = pipeline::run_pipeline(
        rx,
        handle,
        |json_bytes: &Vec<u8>| {
            let entity: Entity = serde_json::from_slice(json_bytes).ok()?;
            if is_music_relevant(&entity) {
                Some(extract(&entity))
            } else {
                None
            }
        },
        &mut output,
    )?;

    let elapsed = start.elapsed();
    eprintln!();
    eprintln!("Done in {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Total entities:   {:>12}", stats.scanned);
    eprintln!("  Music-relevant:   {:>12}", stats.written);
    eprintln!(
        "  Match rate:       {:>11.1}%",
        if stats.scanned > 0 {
            stats.written as f64 / stats.scanned as f64 * 100.0
        } else {
            0.0
        }
    );
    eprintln!("  Output:           {}", output_dir.display());

    Ok(())
}
