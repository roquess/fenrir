use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recipe {
    pub signature: String,
    pub version: u32,
    pub backend: String,
    pub config: CsvConfig,
    pub schema: Vec<Field>,
    #[serde(default)]
    pub confidence_rules: ConfidenceRules,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CsvConfig {
    pub sep: char,
    pub encoding: String,
    pub header: bool,
    #[serde(default = "default_quote")]
    pub quote: char,
}

fn default_quote() -> char {
    '"'
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: FieldType,
    pub from: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfidenceRules {
    pub min_field_match: f64,
}

impl Default for ConfidenceRules {
    fn default() -> Self {
        Self {
            min_field_match: 0.95,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_json_roundtrip() {
        let r = Recipe {
            signature: "csv:sep=;:cols=3:hdr".into(),
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
            ],
            confidence_rules: ConfidenceRules::default(),
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: Recipe = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
