//! Data structures for Wikidata entities.
//!
//! Only the fields needed for music-relevance filtering and CSV extraction
//! are modeled. Everything else is ignored during deserialization.

use serde::Deserialize;
use std::collections::HashMap;

/// A Wikidata entity (item or property).
#[derive(Debug, Deserialize)]
pub struct Entity {
    pub id: String,
    #[serde(default)]
    pub labels: HashMap<String, LangValue>,
    #[serde(default)]
    pub descriptions: HashMap<String, LangValue>,
    #[serde(default)]
    pub aliases: HashMap<String, Vec<LangValue>>,
    #[serde(default)]
    pub claims: HashMap<String, Vec<Statement>>,
}

/// A language-tagged string value.
#[derive(Debug, Deserialize)]
pub struct LangValue {
    pub value: String,
}

/// A Wikidata statement (claim).
#[derive(Debug, Deserialize)]
pub struct Statement {
    pub mainsnak: Snak,
}

/// A snak — the core value carrier in Wikidata.
#[derive(Debug, Deserialize)]
pub struct Snak {
    pub snaktype: String,
    #[serde(default)]
    pub datavalue: Option<DataValue>,
}

/// A typed data value.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum DataValue {
    #[serde(rename = "wikibase-entityid")]
    EntityId { value: EntityIdValue },
    #[serde(rename = "string")]
    StringVal { value: String },
    #[serde(other)]
    Other,
}

/// The value of an entity-id data value.
#[derive(Debug, Deserialize)]
pub struct EntityIdValue {
    pub id: String,
}

impl Entity {
    /// Get the English label, falling back to other Latin-script languages.
    pub fn en_label(&self) -> Option<&str> {
        // Prefer English, then fall back through common Latin-script languages
        const FALLBACKS: &[&str] = &["en", "fr", "de", "es", "it", "pt", "nl", "pl", "cs"];
        for lang in FALLBACKS {
            if let Some(v) = self.labels.get(*lang) {
                return Some(v.value.as_str());
            }
        }
        // Last resort: any available label
        self.labels.values().next().map(|v| v.value.as_str())
    }

    /// Get the English description, or None.
    pub fn en_description(&self) -> Option<&str> {
        self.descriptions.get("en").map(|v| v.value.as_str())
    }

    /// Get English aliases.
    pub fn en_aliases(&self) -> Vec<&str> {
        self.aliases
            .get("en")
            .map(|vs| vs.iter().map(|v| v.value.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get all entity-id values for a property (e.g., P31, P106, P737).
    pub fn entity_ids(&self, property: &str) -> Vec<&str> {
        self.claims
            .get(property)
            .map(|stmts| {
                stmts
                    .iter()
                    .filter_map(|s| match &s.mainsnak.datavalue {
                        Some(DataValue::EntityId { value }) => Some(value.id.as_str()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all string values for a property (e.g., P1953 Discogs ID).
    pub fn string_values(&self, property: &str) -> Vec<&str> {
        self.claims
            .get(property)
            .map(|stmts| {
                stmts
                    .iter()
                    .filter_map(|s| match &s.mainsnak.datavalue {
                        Some(DataValue::StringVal { value }) => Some(value.as_str()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn autechre_json() -> &'static str {
        r#"{
            "id": "Q187923",
            "labels": {"en": {"language": "en", "value": "Autechre"}},
            "descriptions": {"en": {"language": "en", "value": "British electronic music duo"}},
            "aliases": {"en": [
                {"language": "en", "value": "ae"},
                {"language": "en", "value": "Ae"}
            ]},
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q215380"}}}}],
                "P1953": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "12"}}}],
                "P737": [
                    {"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q49835"}}}},
                    {"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q192540"}}}}
                ]
            }
        }"#
    }

    #[test]
    fn parse_entity() {
        let entity: Entity = serde_json::from_str(autechre_json()).unwrap();
        assert_eq!(entity.id, "Q187923");
        assert_eq!(entity.en_label(), Some("Autechre"));
        assert_eq!(
            entity.en_description(),
            Some("British electronic music duo")
        );
    }

    #[test]
    fn en_aliases() {
        let entity: Entity = serde_json::from_str(autechre_json()).unwrap();
        assert_eq!(entity.en_aliases(), vec!["ae", "Ae"]);
    }

    #[test]
    fn entity_ids() {
        let entity: Entity = serde_json::from_str(autechre_json()).unwrap();
        assert_eq!(entity.entity_ids("P31"), vec!["Q215380"]);
        assert_eq!(entity.entity_ids("P737"), vec!["Q49835", "Q192540"]);
        assert!(entity.entity_ids("P999").is_empty());
    }

    #[test]
    fn string_values() {
        let entity: Entity = serde_json::from_str(autechre_json()).unwrap();
        assert_eq!(entity.string_values("P1953"), vec!["12"]);
        assert!(entity.string_values("P999").is_empty());
    }

    #[test]
    fn missing_fields_default() {
        let entity: Entity = serde_json::from_str(r#"{"id": "Q1"}"#).unwrap();
        assert_eq!(entity.en_label(), None);
        assert_eq!(entity.en_description(), None);
        assert!(entity.en_aliases().is_empty());
        assert!(entity.entity_ids("P31").is_empty());
    }
}
