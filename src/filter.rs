//! Music-relevance filter for Wikidata entities.
//!
//! An entity is music-relevant if it has any primary indicator:
//! - P1953 (Discogs artist ID)
//! - P1902 (Discogs label ID)
//! - P106 (occupation) with a musician-related QID
//! - P31 (instance of) with a musical group or record label QID

use crate::model::Entity;
use std::collections::HashSet;
use std::sync::LazyLock;

/// Wikidata property IDs.
pub mod props {
    pub const INSTANCE_OF: &str = "P31";
    pub const OCCUPATION: &str = "P106";
    pub const DISCOGS_ARTIST_ID: &str = "P1953";
    pub const DISCOGS_LABEL_ID: &str = "P1902";
    pub const INFLUENCED_BY: &str = "P737";
    pub const GENRE: &str = "P136";
    pub const RECORD_LABEL: &str = "P264";
    pub const PARENT_ORG: &str = "P749";
    pub const MUSICBRAINZ_ARTIST_ID: &str = "P434";
    pub const APPLE_MUSIC_ARTIST_ID: &str = "P2850";
    pub const BANDCAMP_ID: &str = "P3283";
}

/// Musician-related occupation QIDs (P106 values).
static MUSICIAN_QIDS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "Q130857",   // musician
        "Q386854",   // singer
        "Q183945",   // DJ
        "Q177220",   // singer-songwriter
        "Q36834",    // composer
        "Q753110",   // songwriter
        "Q639669",   // record producer
        "Q806349",   // bandleader
        "Q855091",   // guitarist
        "Q488205",   // rapper
        "Q584301",   // drummer
        "Q386854",   // vocalist
        "Q1075651",  // bass guitarist
        "Q12800682", // keyboardist
        "Q158852",   // conductor
    ])
});

/// Musical group / ensemble instance-of QIDs (P31 values).
static MUSICAL_GROUP_QIDS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "Q5741069",  // musical group
        "Q215380",   // musical group (band)
        "Q56816954", // musical ensemble
        "Q18127",    // record label
    ])
});

/// Returns true if the entity is music-relevant.
pub fn is_music_relevant(entity: &Entity) -> bool {
    // P1953: has a Discogs artist ID
    if !entity.string_values(props::DISCOGS_ARTIST_ID).is_empty() {
        return true;
    }

    // P1902: has a Discogs label ID
    if !entity.string_values(props::DISCOGS_LABEL_ID).is_empty() {
        return true;
    }

    // P106: occupation is musician-related
    for qid in entity.entity_ids(props::OCCUPATION) {
        if MUSICIAN_QIDS.contains(qid) {
            return true;
        }
    }

    // P31: instance of musical group or record label
    for qid in entity.entity_ids(props::INSTANCE_OF) {
        if MUSICAL_GROUP_QIDS.contains(qid) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Entity;

    fn parse(json: &str) -> Entity {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn discogs_artist_id_qualifies() {
        let entity = parse(
            r#"{
            "id": "Q187923",
            "claims": {
                "P1953": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "12"}}}]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }

    #[test]
    fn discogs_label_id_qualifies() {
        let entity = parse(
            r#"{
            "id": "Q1312934",
            "claims": {
                "P1902": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "23528"}}}]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }

    #[test]
    fn musician_occupation_qualifies() {
        let entity = parse(
            r#"{
            "id": "Q1000",
            "claims": {
                "P106": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q130857"}}}}]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }

    #[test]
    fn musical_group_instance_qualifies() {
        let entity = parse(
            r#"{
            "id": "Q187923",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q215380"}}}}]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }

    #[test]
    fn record_label_instance_qualifies() {
        let entity = parse(
            r#"{
            "id": "Q1312934",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q18127"}}}}]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }

    #[test]
    fn painter_does_not_qualify() {
        let entity = parse(
            r#"{
            "id": "Q5582",
            "claims": {
                "P31": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q5"}}}}],
                "P106": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q1028181"}}}}],
                "P737": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q5592"}}}}]
            }
        }"#,
        );
        assert!(!is_music_relevant(&entity));
    }

    #[test]
    fn empty_entity_does_not_qualify() {
        let entity = parse(r#"{"id": "Q1"}"#);
        assert!(!is_music_relevant(&entity));
    }

    #[test]
    fn influence_alone_does_not_qualify() {
        let entity = parse(
            r#"{
            "id": "Q999",
            "claims": {
                "P737": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q100"}}}}]
            }
        }"#,
        );
        assert!(!is_music_relevant(&entity));
    }

    #[test]
    fn genre_alone_does_not_qualify() {
        let entity = parse(
            r#"{
            "id": "Q999",
            "claims": {
                "P136": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q11399"}}}}]
            }
        }"#,
        );
        assert!(!is_music_relevant(&entity));
    }

    #[test]
    fn multiple_occupations_one_musician() {
        let entity = parse(
            r#"{
            "id": "Q1000",
            "claims": {
                "P106": [
                    {"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q33999"}}}},
                    {"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": "Q36834"}}}}
                ]
            }
        }"#,
        );
        assert!(is_music_relevant(&entity));
    }
}
