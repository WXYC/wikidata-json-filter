//! Streaming filter for Wikidata JSON dumps.
//!
//! Reads a gzipped Wikidata JSON dump, filters to music-relevant entities,
//! and writes flat CSV files for loading into PostgreSQL.

use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use std::ffi::OsString;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use wxyc_etl::pipeline::{self, BatchConfig};

use wikidata_json_filter::extractor::extract;
use wikidata_json_filter::filter::is_music_relevant;
use wikidata_json_filter::model::Entity;
use wikidata_json_filter::writer::CsvOutput;

#[derive(Parser)]
#[command(name = "wikidata-json-filter")]
#[command(about = "Filter Wikidata JSON dumps to music-relevant entities")]
struct Cli {
    /// Path to the Wikidata JSON dump (plain or .gz), or "-" for stdin
    input: OsString,

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

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let start = Instant::now();

    // Open input: file path or "-" for stdin
    let is_stdin = cli.input == "-";
    let use_gzip = cli.gzip
        || (!is_stdin
            && PathBuf::from(&cli.input)
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
        let path = PathBuf::from(&cli.input);
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
    let mut output = CsvOutput::new(&cli.output_dir)
        .with_context(|| format!("Failed to create output in {}", cli.output_dir.display()))?;

    // Scanner: read lines, strip JSON array delimiters, send byte batches
    let limit = cli.limit;
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
    eprintln!("  Output:           {}", cli.output_dir.display());

    Ok(())
}
