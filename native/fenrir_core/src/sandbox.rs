use rhai::{Engine, Scope};

/// Runs a generated transform snippet inside a resource-limited sandbox.
/// This is the "generated code" escape hatch: when a transform exceeds the
/// recipe's declarative vocabulary, the AI can emit a small rhai snippet for
/// THIS field. The snippet receives `input` (the raw field value, as a String)
/// and must return a String.
///
/// Limits: operation count and string size are capped → a runaway snippet is
/// interrupted rather than allowed to block the pipeline.
pub fn run_snippet(code: &str, input: &str) -> Result<String, String> {
    let mut engine = Engine::new();
    engine.set_max_operations(10_000);
    engine.set_max_string_size(100_000);
    engine.set_max_call_levels(16);

    let mut scope = Scope::new();
    scope.push("input", input.to_string());

    engine
        .eval_with_scope::<String>(&mut scope, code)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_simple_transform() {
        assert_eq!(run_snippet("input.to_upper()", "abc").unwrap(), "ABC");
    }

    #[test]
    fn can_reshape_value() {
        // Normalize a phone prefix.
        let code = r#"if input.starts_with("0") { "+33" + input.sub_string(1) } else { input }"#;
        assert_eq!(run_snippet(code, "0612345678").unwrap(), "+33612345678");
    }

    #[test]
    fn op_limit_stops_runaway_snippet() {
        // Infinite loop → interrupted by the operation limit.
        let code = "let x = 0; while true { x += 1; } x.to_string()";
        assert!(run_snippet(code, "x").is_err());
    }

    #[test]
    fn invalid_snippet_errors() {
        assert!(run_snippet("this is not rhai @@@", "x").is_err());
    }
}
