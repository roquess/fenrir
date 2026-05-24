use crate::engine::ParsedRecord;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Text / semi-structured recipe: a regex pattern with named groups. Each
/// named group becomes a field. Targets logs, statements, repetitive lines
/// whose structure is not formal (cf. the loki_text family).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextRecipe {
    pub signature: String,
    pub version: u32,
    pub backend: String,
    pub pattern: String,
    pub fields: Vec<String>,
}

/// Builds a recipe from a pattern: fields are derived from the regex's named
/// capture groups.
pub fn recipe_from_pattern(pattern: &str) -> TextRecipe {
    let fields = match Regex::new(pattern) {
        Ok(re) => re
            .capture_names()
            .flatten()
            .map(|s| s.to_string())
            .collect(),
        Err(_) => vec![],
    };
    let signature = format!("text:fields={}", fields.len());
    TextRecipe {
        signature,
        version: 1,
        backend: "text".into(),
        pattern: pattern.to_string(),
        fields,
    }
}

/// Applies the pattern to a line → Value, with confidence = ratio of fields
/// actually captured.
pub fn parse_text(recipe: &TextRecipe, line: &str) -> ParsedRecord {
    let re = match Regex::new(&recipe.pattern) {
        Ok(r) => r,
        Err(_) => {
            return ParsedRecord {
                value: Value::Object(Map::new()),
                confidence: 0.0,
            }
        }
    };
    let mut obj = Map::new();
    let mut matched = 0usize;
    if let Some(caps) = re.captures(line) {
        for name in &recipe.fields {
            if let Some(m) = caps.name(name) {
                matched += 1;
                obj.insert(name.clone(), Value::String(m.as_str().to_string()));
            }
        }
    }
    let total = recipe.fields.len();
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

    const PATTERN: &str = r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<level>\w+)\s+user=(?P<user>\d+)";

    #[test]
    fn recipe_lists_named_groups() {
        let r = recipe_from_pattern(PATTERN);
        assert_eq!(r.fields, vec!["date", "level", "user"]);
        assert_eq!(r.backend, "text");
    }

    #[test]
    fn parses_log_line() {
        let r = recipe_from_pattern(PATTERN);
        let out = parse_text(&r, "2026-01-02 ERR user=42 'timeout'");
        assert_eq!(out.value["date"], serde_json::json!("2026-01-02"));
        assert_eq!(out.value["level"], serde_json::json!("ERR"));
        assert_eq!(out.value["user"], serde_json::json!("42"));
        assert!((out.confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn non_matching_line_is_zero_confidence() {
        let r = recipe_from_pattern(PATTERN);
        let out = parse_text(&r, "garbage that does not match");
        assert!((out.confidence - 0.0).abs() < 1e-9);
    }
}
