//! CSV output writer for extracted entity rows.

use crate::extractor::ExtractedRows;
use anyhow::Result;
use std::path::Path;
use wxyc_etl::csv_writer::{CsvFileSpec, MultiCsvWriter};
use wxyc_etl::pipeline::PipelineOutput;

/// CSV file indices — must match the order in [`csv_file_specs`].
const ENTITY: usize = 0;
const DISCOGS_MAPPING: usize = 1;
const INFLUENCE: usize = 2;
const GENRE: usize = 3;
const RECORD_LABEL: usize = 4;
const LABEL_HIERARCHY: usize = 5;
const ENTITY_ALIAS: usize = 6;
const OCCUPATION: usize = 7;

/// Build the 8-file CSV spec for Wikidata entity output.
pub fn csv_file_specs() -> Vec<CsvFileSpec> {
    vec![
        CsvFileSpec::new("entity.csv", &["qid", "label", "description", "entity_type"]),
        CsvFileSpec::new("discogs_mapping.csv", &["qid", "property", "discogs_id"]),
        CsvFileSpec::new("influence.csv", &["source_qid", "target_qid"]),
        CsvFileSpec::new("genre.csv", &["entity_qid", "genre_qid"]),
        CsvFileSpec::new("record_label.csv", &["artist_qid", "label_qid"]),
        CsvFileSpec::new("label_hierarchy.csv", &["child_qid", "parent_qid"]),
        CsvFileSpec::new("entity_alias.csv", &["qid", "alias"]),
        CsvFileSpec::new("occupation.csv", &["entity_qid", "occupation_qid"]),
    ]
}

/// Writes extracted entity data to 8 CSV files via [`MultiCsvWriter`].
pub struct CsvOutput {
    inner: MultiCsvWriter,
}

impl CsvOutput {
    /// Create a new CsvOutput writing to the given directory.
    pub fn new(output_dir: &Path) -> Result<Self> {
        let specs = csv_file_specs();
        let inner = MultiCsvWriter::new(output_dir, &specs)?;
        Ok(Self { inner })
    }

    /// Write all rows from one extracted entity.
    pub fn write(&mut self, rows: &ExtractedRows) -> Result<()> {
        if let Some(e) = &rows.entity {
            self.inner
                .writer(ENTITY)
                .write_record([&e.qid, &e.label, &e.description, &e.entity_type])?;
        }

        for m in &rows.discogs_mappings {
            self.inner
                .writer(DISCOGS_MAPPING)
                .write_record([&m.qid, &m.property, &m.discogs_id])?;
        }

        for i in &rows.influences {
            self.inner
                .writer(INFLUENCE)
                .write_record([&i.source_qid, &i.target_qid])?;
        }

        for g in &rows.genres {
            self.inner
                .writer(GENRE)
                .write_record([&g.entity_qid, &g.genre_qid])?;
        }

        for r in &rows.record_labels {
            self.inner
                .writer(RECORD_LABEL)
                .write_record([&r.artist_qid, &r.label_qid])?;
        }

        for h in &rows.label_hierarchies {
            self.inner
                .writer(LABEL_HIERARCHY)
                .write_record([&h.child_qid, &h.parent_qid])?;
        }

        for a in &rows.aliases {
            self.inner
                .writer(ENTITY_ALIAS)
                .write_record([&a.qid, &a.alias])?;
        }

        for o in &rows.occupations {
            self.inner
                .writer(OCCUPATION)
                .write_record([&o.entity_qid, &o.occupation_qid])?;
        }

        Ok(())
    }

    /// Flush all writers.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush_all()
    }
}

impl PipelineOutput<ExtractedRows> for CsvOutput {
    fn write_item(&mut self, item: &ExtractedRows) -> Result<()> {
        self.write(item)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush_all()
    }

    fn finish(&mut self) -> Result<()> {
        self.inner.flush_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extractor::extract;
    use crate::model::Entity;
    use tempfile::TempDir;

    #[test]
    fn writes_csv_files() {
        let dir = TempDir::new().unwrap();
        let mut output = CsvOutput::new(dir.path()).unwrap();

        let entity: Entity = serde_json::from_str(r#"{
            "id": "Q187923",
            "labels": {"en": {"language": "en", "value": "Autechre"}},
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q215380"}}}}],
                "P1953": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "12"}}}]
            }
        }"#).unwrap();

        let rows = extract(&entity);
        output.write(&rows).unwrap();
        output.flush().unwrap();

        let entity_csv = std::fs::read_to_string(dir.path().join("entity.csv")).unwrap();
        assert!(entity_csv.contains("Q187923"));
        assert!(entity_csv.contains("Autechre"));
        assert!(entity_csv.contains("group"));

        let mapping_csv = std::fs::read_to_string(dir.path().join("discogs_mapping.csv")).unwrap();
        assert!(mapping_csv.contains("Q187923"));
        assert!(mapping_csv.contains("P1953"));
        assert!(mapping_csv.contains("12"));
    }

    #[test]
    fn all_eight_files_created() {
        let dir = TempDir::new().unwrap();
        let _output = CsvOutput::new(dir.path()).unwrap();

        let expected = [
            "entity.csv",
            "discogs_mapping.csv",
            "influence.csv",
            "genre.csv",
            "record_label.csv",
            "label_hierarchy.csv",
            "entity_alias.csv",
            "occupation.csv",
        ];

        for name in expected {
            assert!(dir.path().join(name).exists(), "{name} should exist");
        }
    }

    #[test]
    fn shared_writer_creates_expected_files() {
        use wxyc_etl::csv_writer::MultiCsvWriter;

        let dir = TempDir::new().unwrap();
        let specs = csv_file_specs();
        let writer = MultiCsvWriter::new(dir.path(), &specs).unwrap();
        drop(writer);

        let expected = [
            "entity.csv",
            "discogs_mapping.csv",
            "influence.csv",
            "genre.csv",
            "record_label.csv",
            "label_hierarchy.csv",
            "entity_alias.csv",
            "occupation.csv",
        ];

        for name in expected {
            assert!(dir.path().join(name).exists(), "{name} should exist");
        }

        // Verify headers match current contract
        let entity_csv = std::fs::read_to_string(dir.path().join("entity.csv")).unwrap();
        let first_line = entity_csv.lines().next().unwrap();
        assert_eq!(first_line, "qid,label,description,entity_type");

        let mapping_csv = std::fs::read_to_string(dir.path().join("discogs_mapping.csv")).unwrap();
        let first_line = mapping_csv.lines().next().unwrap();
        assert_eq!(first_line, "qid,property,discogs_id");
    }

    #[test]
    fn csv_output_implements_pipeline_output() {
        use wxyc_etl::pipeline::PipelineOutput;

        let dir = TempDir::new().unwrap();
        let mut output = CsvOutput::new(dir.path()).unwrap();

        let entity: Entity = serde_json::from_str(r#"{
            "id": "Q187923",
            "labels": {"en": {"language": "en", "value": "Autechre"}},
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q215380"}}}}],
                "P1953": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "12"}}}]
            }
        }"#).unwrap();

        let rows = extract(&entity);
        output.write_item(&rows).unwrap();
        PipelineOutput::flush(&mut output).unwrap();

        let entity_csv = std::fs::read_to_string(dir.path().join("entity.csv")).unwrap();
        assert!(entity_csv.contains("Q187923"));
        assert!(entity_csv.contains("Autechre"));
    }
}
