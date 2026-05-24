use crate::engine::ParsedRecord;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Recette XML : un enregistrement = une occurrence de `record_tag`, ses
/// champs = les éléments enfants directs. (Phase 3 : valeurs en String ;
/// le typage XML pourra être ajouté plus tard.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XmlRecipe {
    pub signature: String,
    pub version: u32,
    pub backend: String,
    pub record_tag: String,
    pub fields: Vec<String>,
}

/// Détection grossière du format à partir d'un échantillon.
pub fn detect_format(sample: &str) -> &'static str {
    if sample.trim_start().starts_with('<') {
        "xml"
    } else {
        "csv"
    }
}

fn local_name(raw: &[u8]) -> String {
    let s = String::from_utf8_lossy(raw);
    match s.rsplit(':').next() {
        Some(x) => x.to_string(),
        None => s.to_string(),
    }
}

/// Infère la structure : l'élément répété (niveau 2) et ses champs enfants.
pub fn sniff_xml(sample: &str) -> XmlRecipe {
    let mut reader = Reader::from_str(sample);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<String> = Vec::new();
    let mut record_tag: Option<String> = None;
    let mut fields: Vec<String> = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                stack.push(name.clone());
                if stack.len() == 2 && record_tag.is_none() {
                    record_tag = Some(name);
                } else if stack.len() == 3 && is_first_record(&stack, &record_tag) {
                    push_unique(&mut fields, name);
                }
            }
            Ok(Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                if stack.len() == 2 && is_first_record(&stack, &record_tag) {
                    push_unique(&mut fields, name);
                }
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    let rt = record_tag.unwrap_or_else(|| "record".to_string());
    let signature = format!("xml:rec={}:fields={}", rt, fields.len());
    XmlRecipe {
        signature,
        version: 1,
        backend: "xml".into(),
        record_tag: rt,
        fields,
    }
}

fn is_first_record(stack: &[String], record_tag: &Option<String>) -> bool {
    match record_tag {
        Some(rt) => stack.get(1) == Some(rt),
        None => false,
    }
}

fn push_unique(v: &mut Vec<String>, name: String) {
    if !v.contains(&name) {
        v.push(name);
    }
}

/// Parse un fragment d'un seul enregistrement (`<row>…</row>`) → Value.
pub fn parse_record_xml(recipe: &XmlRecipe, fragment: &str) -> ParsedRecord {
    let mut reader = Reader::from_str(fragment);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<String> = Vec::new();
    let mut cur_field: Option<String> = None;
    let mut obj = Map::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                stack.push(name.clone());
                if stack.len() == 2 {
                    cur_field = Some(name);
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(f) = &cur_field {
                    if recipe.fields.contains(f) {
                        let txt = String::from_utf8_lossy(e.as_ref()).to_string();
                        obj.insert(f.clone(), Value::String(txt));
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                if stack.len() == 1 && recipe.fields.contains(&name) {
                    obj.insert(name, Value::String(String::new()));
                }
            }
            Ok(Event::End(_)) => {
                stack.pop();
                if stack.len() < 2 {
                    cur_field = None;
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    let total = recipe.fields.len();
    let matched = recipe.fields.iter().filter(|f| obj.contains_key(*f)).count();
    let confidence = if total == 0 {
        1.0
    } else {
        matched as f64 / total as f64
    };
    ParsedRecord {
        value: Value::Object(obj),
        confidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str =
        "<rows><row><name>Alice</name><age>30</age></row>\
         <row><name>Bob</name><age>25</age></row></rows>";

    #[test]
    fn detect_format_xml_vs_csv() {
        assert_eq!(detect_format("  <rows>"), "xml");
        assert_eq!(detect_format("a;b\n1;2"), "csv");
    }

    #[test]
    fn sniff_xml_finds_record_and_fields() {
        let r = sniff_xml(SAMPLE);
        assert_eq!(r.record_tag, "row");
        assert_eq!(r.fields, vec!["name".to_string(), "age".to_string()]);
        assert_eq!(r.backend, "xml");
    }

    #[test]
    fn parse_record_xml_extracts_fields() {
        let r = sniff_xml(SAMPLE);
        let out = parse_record_xml(&r, "<row><name>Carol</name><age>40</age></row>");
        assert_eq!(out.value["name"], serde_json::json!("Carol"));
        assert_eq!(out.value["age"], serde_json::json!("40"));
        assert!((out.confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn missing_field_lowers_confidence() {
        let r = sniff_xml(SAMPLE);
        let out = parse_record_xml(&r, "<row><name>Dan</name></row>");
        assert!(out.confidence < 1.0);
    }
}
