use crate::recipe::{Field, FieldType, Recipe};
use serde_json::{Map, Value};

pub struct ParsedRecord {
    pub value: Value,
    pub confidence: f64,
}

pub fn parse_line(recipe: &Recipe, line: &str) -> ParsedRecord {
    let raw = split_csv(line, recipe.config.sep, recipe.config.quote);
    let mut obj = Map::new();
    let mut matched = 0usize;
    let total = recipe.schema.len();

    for f in &recipe.schema {
        let cell = raw.get(f.from).map(|s| s.as_str()).unwrap_or("");
        let cell = apply_split(cell, f);
        match coerce(cell, f.ty) {
            Some(v) => {
                matched += 1;
                obj.insert(f.name.clone(), v);
            }
            None => {
                obj.insert(f.name.clone(), Value::Null);
            }
        }
    }
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

fn apply_split<'a>(cell: &'a str, f: &Field) -> &'a str {
    match (&f.split, f.take) {
        (Some(sep), Some(idx)) => cell.split(sep.as_str()).nth(idx).unwrap_or(""),
        (Some(sep), None) => cell.split(sep.as_str()).next().unwrap_or(""),
        _ => cell,
    }
}

fn coerce(cell: &str, ty: FieldType) -> Option<Value> {
    let c = cell.trim();
    match ty {
        FieldType::String => Some(Value::String(c.to_string())),
        FieldType::Int => c.parse::<i64>().ok().map(Into::into),
        FieldType::Float => c
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        FieldType::Bool => match c.to_lowercase().as_str() {
            "true" | "1" | "yes" => Some(Value::Bool(true)),
            "false" | "0" | "no" => Some(Value::Bool(false)),
            _ => None,
        },
    }
}

/// Découpe CSV minimale gérant les guillemets. Partagée avec le sniffer
/// pour garantir un découpage cohérent (alignement des colonnes).
pub fn split_csv(line: &str, sep: char, quote: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == quote {
            if in_q && chars.peek() == Some(&quote) {
                cur.push(quote);
                chars.next();
            } else {
                in_q = !in_q;
            }
        } else if ch == sep && !in_q {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(ch);
        }
    }
    out.push(cur);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::*;

    fn recipe() -> Recipe {
        Recipe {
            signature: "t".into(),
            version: 1,
            backend: "csv".into(),
            config: CsvConfig {
                sep: ';',
                encoding: "utf-8".into(),
                header: true,
                quote: '"',
            },
            schema: vec![
                Field {
                    name: "name".into(),
                    ty: FieldType::String,
                    from: 0,
                    split: None,
                    take: None,
                },
                Field {
                    name: "age".into(),
                    ty: FieldType::Int,
                    from: 1,
                    split: None,
                    take: None,
                },
                Field {
                    name: "city".into(),
                    ty: FieldType::String,
                    from: 2,
                    split: Some("|".into()),
                    take: Some(0),
                },
            ],
            confidence_rules: ConfidenceRules::default(),
        }
    }

    #[test]
    fn parses_all_fields_full_confidence() {
        let r = recipe();
        let out = parse_line(&r, "Alice;30;Paris|FR");
        assert_eq!(out.value["name"], serde_json::json!("Alice"));
        assert_eq!(out.value["age"], serde_json::json!(30));
        assert_eq!(out.value["city"], serde_json::json!("Paris"));
        assert!((out.confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn bad_int_lowers_confidence_and_nulls_field() {
        let r = recipe();
        let out = parse_line(&r, "Bob;notanumber;Lyon");
        assert_eq!(out.value["age"], serde_json::Value::Null);
        assert!(out.confidence < 1.0);
    }
}
