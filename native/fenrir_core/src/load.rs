use loki_weave::{format_data, OutputFormat};
use serde_json::Value;

pub fn load(value: &Value, format: &str) -> Result<String, String> {
    let fmt = OutputFormat::from_str(format).ok_or_else(|| format!("unsupported format: {format}"))?;
    format_data(value, fmt).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn loads_value_to_json() {
        let v = json!({"name": "Alice", "age": 30});
        let out = load(&v, "json").unwrap();
        assert!(out.contains("\"Alice\""));
    }

    #[test]
    fn unknown_format_errors() {
        let v = json!({});
        assert!(load(&v, "no-such-format").is_err());
    }
}
