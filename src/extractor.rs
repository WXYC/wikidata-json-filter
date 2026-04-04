//! Extract target fields from music-relevant entities into flat CSV rows.

use crate::filter::props;
use crate::model::Entity;

/// The entity type derived from P31/P106 claims.
pub fn classify_entity_type(entity: &Entity) -> &'static str {
    let instance_of = entity.entity_ids(props::INSTANCE_OF);

    // Record label
    if instance_of.contains(&"Q18127") {
        return "label";
    }

    // Musical group / band / ensemble
    for qid in &instance_of {
        if matches!(*qid, "Q5741069" | "Q215380" | "Q56816954") {
            return "group";
        }
    }

    // Human (check P31 for Q5)
    if instance_of.contains(&"Q5") {
        return "human";
    }

    "other"
}

/// Extracted rows from a single entity, ready for CSV writing.
#[derive(Debug, Default)]
pub struct ExtractedRows {
    pub entity: Option<EntityRow>,
    pub discogs_mappings: Vec<DiscogsMappingRow>,
    pub influences: Vec<InfluenceRow>,
    pub genres: Vec<GenreRow>,
    pub record_labels: Vec<RecordLabelRow>,
    pub label_hierarchies: Vec<LabelHierarchyRow>,
    pub aliases: Vec<AliasRow>,
    pub occupations: Vec<OccupationRow>,
}

#[derive(Debug)]
pub struct EntityRow {
    pub qid: String,
    pub label: String,
    pub description: String,
    pub entity_type: String,
}

#[derive(Debug)]
pub struct DiscogsMappingRow {
    pub qid: String,
    pub property: String,
    pub discogs_id: String,
}

#[derive(Debug)]
pub struct InfluenceRow {
    pub source_qid: String,
    pub target_qid: String,
}

#[derive(Debug)]
pub struct GenreRow {
    pub entity_qid: String,
    pub genre_qid: String,
}

#[derive(Debug)]
pub struct RecordLabelRow {
    pub artist_qid: String,
    pub label_qid: String,
}

#[derive(Debug)]
pub struct LabelHierarchyRow {
    pub child_qid: String,
    pub parent_qid: String,
}

#[derive(Debug)]
pub struct AliasRow {
    pub qid: String,
    pub alias: String,
}

#[derive(Debug)]
pub struct OccupationRow {
    pub entity_qid: String,
    pub occupation_qid: String,
}

/// Extract all CSV rows from a music-relevant entity.
pub fn extract(entity: &Entity) -> ExtractedRows {
    let mut rows = ExtractedRows::default();
    let qid = &entity.id;

    // Entity row
    rows.entity = Some(EntityRow {
        qid: qid.clone(),
        label: entity.en_label().unwrap_or("").to_string(),
        description: entity.en_description().unwrap_or("").to_string(),
        entity_type: classify_entity_type(entity).to_string(),
    });

    // Discogs artist ID (P1953)
    for val in entity.string_values(props::DISCOGS_ARTIST_ID) {
        rows.discogs_mappings.push(DiscogsMappingRow {
            qid: qid.clone(),
            property: "P1953".to_string(),
            discogs_id: val.to_string(),
        });
    }

    // Discogs label ID (P1902)
    for val in entity.string_values(props::DISCOGS_LABEL_ID) {
        rows.discogs_mappings.push(DiscogsMappingRow {
            qid: qid.clone(),
            property: "P1902".to_string(),
            discogs_id: val.to_string(),
        });
    }

    // Influences (P737)
    for target_qid in entity.entity_ids(props::INFLUENCED_BY) {
        rows.influences.push(InfluenceRow {
            source_qid: qid.clone(),
            target_qid: target_qid.to_string(),
        });
    }

    // Genres (P136)
    for genre_qid in entity.entity_ids(props::GENRE) {
        rows.genres.push(GenreRow {
            entity_qid: qid.clone(),
            genre_qid: genre_qid.to_string(),
        });
    }

    // Record labels (P264)
    for label_qid in entity.entity_ids(props::RECORD_LABEL) {
        rows.record_labels.push(RecordLabelRow {
            artist_qid: qid.clone(),
            label_qid: label_qid.to_string(),
        });
    }

    // Parent org / label hierarchy (P749)
    for parent_qid in entity.entity_ids(props::PARENT_ORG) {
        rows.label_hierarchies.push(LabelHierarchyRow {
            child_qid: qid.clone(),
            parent_qid: parent_qid.to_string(),
        });
    }

    // English aliases
    for alias in entity.en_aliases() {
        rows.aliases.push(AliasRow {
            qid: qid.clone(),
            alias: alias.to_string(),
        });
    }

    // Occupations (P106)
    for occ_qid in entity.entity_ids(props::OCCUPATION) {
        rows.occupations.push(OccupationRow {
            entity_qid: qid.clone(),
            occupation_qid: occ_qid.to_string(),
        });
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Entity;

    fn parse(json: &str) -> Entity {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn extract_autechre() {
        let entity = parse(r#"{
            "id": "Q187923",
            "labels": {"en": {"language": "en", "value": "Autechre"}},
            "descriptions": {"en": {"language": "en", "value": "British electronic music duo"}},
            "aliases": {"en": [{"language": "en", "value": "ae"}]},
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q215380"}}}}],
                "P1953": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "12"}}}],
                "P737": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q49835"}}}}],
                "P136": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q11399"}}}}],
                "P264": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q1312934"}}}}]
            }
        }"#);

        let rows = extract(&entity);

        let e = rows.entity.unwrap();
        assert_eq!(e.qid, "Q187923");
        assert_eq!(e.label, "Autechre");
        assert_eq!(e.description, "British electronic music duo");
        assert_eq!(e.entity_type, "group");

        assert_eq!(rows.discogs_mappings.len(), 1);
        assert_eq!(rows.discogs_mappings[0].discogs_id, "12");
        assert_eq!(rows.discogs_mappings[0].property, "P1953");

        assert_eq!(rows.influences.len(), 1);
        assert_eq!(rows.influences[0].target_qid, "Q49835");

        assert_eq!(rows.genres.len(), 1);
        assert_eq!(rows.genres[0].genre_qid, "Q11399");

        assert_eq!(rows.record_labels.len(), 1);
        assert_eq!(rows.record_labels[0].label_qid, "Q1312934");

        assert_eq!(rows.aliases.len(), 1);
        assert_eq!(rows.aliases[0].alias, "ae");
    }

    #[test]
    fn classify_record_label() {
        let entity = parse(r#"{
            "id": "Q1312934",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q18127"}}}}]
            }
        }"#);
        assert_eq!(classify_entity_type(&entity), "label");
    }

    #[test]
    fn classify_human() {
        let entity = parse(r#"{
            "id": "Q1000",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q5"}}}}],
                "P106": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q130857"}}}}]
            }
        }"#);
        assert_eq!(classify_entity_type(&entity), "human");
    }

    #[test]
    fn extract_label_hierarchy() {
        let entity = parse(r#"{
            "id": "Q1312934",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q18127"}}}}],
                "P749": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q21077"}}}}]
            }
        }"#);
        let rows = extract(&entity);
        assert_eq!(rows.label_hierarchies.len(), 1);
        assert_eq!(rows.label_hierarchies[0].child_qid, "Q1312934");
        assert_eq!(rows.label_hierarchies[0].parent_qid, "Q21077");
    }

    #[test]
    fn extract_minimal_entity() {
        let entity = parse(r#"{"id": "Q1"}"#);
        let rows = extract(&entity);
        let e = rows.entity.unwrap();
        assert_eq!(e.qid, "Q1");
        assert_eq!(e.label, "");
        assert_eq!(e.entity_type, "other");
        assert!(rows.discogs_mappings.is_empty());
    }
}
