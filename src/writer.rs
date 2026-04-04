//! CSV output writer for extracted entity rows.

use crate::extractor::ExtractedRows;
use anyhow::Result;
use csv::Writer;
use std::fs::File;
use std::path::Path;

/// Writes extracted entity data to 8 CSV files.
pub struct CsvOutput {
    entity: Writer<File>,
    discogs_mapping: Writer<File>,
    influence: Writer<File>,
    genre: Writer<File>,
    record_label: Writer<File>,
    label_hierarchy: Writer<File>,
    entity_alias: Writer<File>,
    occupation: Writer<File>,
}

impl CsvOutput {
    /// Create a new CsvOutput writing to the given directory.
    pub fn new(output_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(output_dir)?;

        let mut entity = Writer::from_path(output_dir.join("entity.csv"))?;
        entity.write_record(["qid", "label", "description", "entity_type"])?;

        let mut discogs_mapping = Writer::from_path(output_dir.join("discogs_mapping.csv"))?;
        discogs_mapping.write_record(["qid", "property", "discogs_id"])?;

        let mut influence = Writer::from_path(output_dir.join("influence.csv"))?;
        influence.write_record(["source_qid", "target_qid"])?;

        let mut genre = Writer::from_path(output_dir.join("genre.csv"))?;
        genre.write_record(["entity_qid", "genre_qid"])?;

        let mut record_label = Writer::from_path(output_dir.join("record_label.csv"))?;
        record_label.write_record(["artist_qid", "label_qid"])?;

        let mut label_hierarchy = Writer::from_path(output_dir.join("label_hierarchy.csv"))?;
        label_hierarchy.write_record(["child_qid", "parent_qid"])?;

        let mut entity_alias = Writer::from_path(output_dir.join("entity_alias.csv"))?;
        entity_alias.write_record(["qid", "alias"])?;

        let mut occupation = Writer::from_path(output_dir.join("occupation.csv"))?;
        occupation.write_record(["entity_qid", "occupation_qid"])?;

        Ok(Self {
            entity,
            discogs_mapping,
            influence,
            genre,
            record_label,
            label_hierarchy,
            entity_alias,
            occupation,
        })
    }

    /// Write all rows from one extracted entity.
    pub fn write(&mut self, rows: &ExtractedRows) -> Result<()> {
        if let Some(e) = &rows.entity {
            self.entity
                .write_record([&e.qid, &e.label, &e.description, &e.entity_type])?;
        }

        for m in &rows.discogs_mappings {
            self.discogs_mapping
                .write_record([&m.qid, &m.property, &m.discogs_id])?;
        }

        for i in &rows.influences {
            self.influence
                .write_record([&i.source_qid, &i.target_qid])?;
        }

        for g in &rows.genres {
            self.genre
                .write_record([&g.entity_qid, &g.genre_qid])?;
        }

        for r in &rows.record_labels {
            self.record_label
                .write_record([&r.artist_qid, &r.label_qid])?;
        }

        for h in &rows.label_hierarchies {
            self.label_hierarchy
                .write_record([&h.child_qid, &h.parent_qid])?;
        }

        for a in &rows.aliases {
            self.entity_alias.write_record([&a.qid, &a.alias])?;
        }

        for o in &rows.occupations {
            self.occupation
                .write_record([&o.entity_qid, &o.occupation_qid])?;
        }

        Ok(())
    }

    /// Flush all writers.
    pub fn flush(&mut self) -> Result<()> {
        self.entity.flush()?;
        self.discogs_mapping.flush()?;
        self.influence.flush()?;
        self.genre.flush()?;
        self.record_label.flush()?;
        self.label_hierarchy.flush()?;
        self.entity_alias.flush()?;
        self.occupation.flush()?;
        Ok(())
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
}
