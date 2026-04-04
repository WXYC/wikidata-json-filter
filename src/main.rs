//! Streaming filter for Wikidata JSON dumps.
//!
//! Reads a gzipped Wikidata JSON dump, filters to music-relevant entities,
//! and writes flat CSV files for loading into PostgreSQL.

use anyhow::{Context, Result};
use clap::Parser;
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use rayon::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use wikidata_json_filter::extractor::extract;
use wikidata_json_filter::filter::is_music_relevant;
use wikidata_json_filter::model::Entity;
use wikidata_json_filter::writer::CsvOutput;

#[derive(Parser)]
#[command(name = "wikidata-json-filter")]
#[command(about = "Filter Wikidata JSON dumps to music-relevant entities")]
struct Cli {
    /// Path to the Wikidata JSON dump (plain or .gz)
    input: PathBuf,

    /// Output directory for CSV files
    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    /// Stop after processing N entities (0 = no limit)
    #[arg(long, default_value = "0")]
    limit: u64,

    /// Log progress every N entities
    #[arg(long, default_value = "1000000")]
    progress_interval: u64,
}

/// Batch size for sending lines to the worker pool.
const BATCH_SIZE: usize = 256;

/// Channel capacity (batches) for backpressure.
const CHANNEL_CAPACITY: usize = 64;

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let start = Instant::now();

    let total_entities = AtomicU64::new(0);
    let matched_entities = AtomicU64::new(0);

    // Open input (gzipped or plain)
    let file = std::fs::File::open(&cli.input)
        .with_context(|| format!("Failed to open {}", cli.input.display()))?;

    let reader: Box<dyn BufRead + Send> = if cli
        .input
        .extension()
        .is_some_and(|ext| ext == "gz")
    {
        Box::new(BufReader::with_capacity(8 * 1024 * 1024, GzDecoder::new(file)))
    } else {
        Box::new(BufReader::with_capacity(8 * 1024 * 1024, file))
    };

    // Set up CSV output
    let mut output = CsvOutput::new(&cli.output_dir)
        .with_context(|| format!("Failed to create output in {}", cli.output_dir.display()))?;

    // Channel: scanner -> rayon workers send matched ExtractedRows back
    let (batch_tx, batch_rx) = bounded::<Vec<Vec<u8>>>(CHANNEL_CAPACITY);

    // Scanner thread: read lines, batch them, send to workers
    let limit = cli.limit;
    let progress_interval = cli.progress_interval;
    let scanner = std::thread::spawn(move || -> Result<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut count: u64 = 0;

        for line_result in reader.lines() {
            let line = line_result?;
            let trimmed = line.trim();

            // Skip JSON array boundaries
            if trimmed == "[" || trimmed == "]" {
                continue;
            }

            // Strip trailing comma
            let json_bytes = if trimmed.ends_with(',') {
                trimmed[..trimmed.len() - 1].as_bytes().to_vec()
            } else {
                trimmed.as_bytes().to_vec()
            };

            // Skip empty lines
            if json_bytes.is_empty() {
                continue;
            }

            batch.push(json_bytes);
            count += 1;

            if batch.len() >= BATCH_SIZE {
                if batch_tx.send(std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE))).is_err() {
                    break; // Receiver dropped
                }
            }

            if limit > 0 && count >= limit {
                break;
            }
        }

        // Send remaining batch
        if !batch.is_empty() {
            let _ = batch_tx.send(batch);
        }

        Ok(())
    });

    // Main thread: receive batches, parse in parallel via rayon, write results
    for batch in batch_rx {
        let results: Vec<_> = batch
            .par_iter()
            .filter_map(|json_bytes| {
                let entity: Entity = match serde_json::from_slice(json_bytes) {
                    Ok(e) => e,
                    Err(_) => return None,
                };

                if is_music_relevant(&entity) {
                    Some(extract(&entity))
                } else {
                    None
                }
            })
            .collect();

        let batch_total = batch.len() as u64;
        let batch_matched = results.len() as u64;

        for rows in &results {
            output.write(rows)?;
        }

        let prev_total = total_entities.fetch_add(batch_total, Ordering::Relaxed);
        matched_entities.fetch_add(batch_matched, Ordering::Relaxed);

        // Progress logging
        let new_total = prev_total + batch_total;
        if progress_interval > 0
            && (prev_total / progress_interval) != (new_total / progress_interval)
        {
            let matched = matched_entities.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs();
            let rate = if elapsed > 0 { new_total / elapsed } else { 0 };
            eprintln!(
                "  ... {new_total} entities processed, {matched} matched ({rate}/s)"
            );
        }
    }

    output.flush()?;

    // Wait for scanner to finish
    scanner
        .join()
        .map_err(|_| anyhow::anyhow!("Scanner thread panicked"))??;

    let total = total_entities.load(Ordering::Relaxed);
    let matched = matched_entities.load(Ordering::Relaxed);
    let elapsed = start.elapsed();

    eprintln!();
    eprintln!("Done in {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Total entities:   {total:>12}");
    eprintln!("  Music-relevant:   {matched:>12}");
    eprintln!(
        "  Match rate:       {:>11.1}%",
        if total > 0 {
            matched as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    );
    eprintln!("  Output:           {}", cli.output_dir.display());

    Ok(())
}
