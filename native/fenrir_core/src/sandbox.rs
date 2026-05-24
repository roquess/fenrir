use rhai::{Engine, Scope};

/// Exécute un snippet de transformation généré, dans un bac à sable à ressources
/// limitées. C'est l'échappatoire « code généré » : quand une transformation
/// dépasse le vocabulaire déclaratif de la recette, l'IA peut émettre un petit
/// snippet rhai pour CE champ. Le snippet reçoit `input` (la valeur brute du
/// champ, en String) et doit renvoyer une String.
///
/// Limites : nombre d'opérations et taille de chaîne plafonnés → un snippet qui
/// s'emballe est interrompu plutôt que de bloquer le pipeline.
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
        // Normalise un préfixe téléphonique.
        let code = r#"if input.starts_with("0") { "+33" + input.sub_string(1) } else { input }"#;
        assert_eq!(run_snippet(code, "0612345678").unwrap(), "+33612345678");
    }

    #[test]
    fn op_limit_stops_runaway_snippet() {
        // Boucle infinie → interrompue par la limite d'opérations.
        let code = "let x = 0; while true { x += 1; } x.to_string()";
        assert!(run_snippet(code, "x").is_err());
    }

    #[test]
    fn invalid_snippet_errors() {
        assert!(run_snippet("this is not rhai @@@", "x").is_err());
    }
}
