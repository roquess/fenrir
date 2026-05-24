use fenrir_core::{
    engine::parse_line,
    load::load,
    recipe::Recipe,
    sniff::{relearn, sniff},
};

#[rustler::nif(name = "sniff")]
fn sniff_nif(sample: String) -> String {
    let recipe = sniff(&sample);
    serde_json::to_string(&recipe).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[rustler::nif(name = "parse_line")]
fn parse_line_nif(recipe_json: String, line: String) -> (String, f64) {
    let recipe: Recipe = match serde_json::from_str(&recipe_json) {
        Ok(r) => r,
        Err(e) => return (format!("{{\"error\":\"{e}\"}}"), 0.0),
    };
    let out = parse_line(&recipe, &line);
    (serde_json::to_string(&out.value).unwrap_or_default(), out.confidence)
}

#[rustler::nif(name = "relearn")]
fn relearn_nif(prev_recipe_json: String, corpus: String) -> String {
    let prev: Recipe = match serde_json::from_str(&prev_recipe_json) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{e}\"}}"),
    };
    let recipe = relearn(&prev, &corpus);
    serde_json::to_string(&recipe).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[rustler::nif(name = "load")]
fn load_nif(value_json: String, format: String) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(&value_json).map_err(|e| e.to_string())?;
    load(&value, &format)
}

rustler::init!("fenrir_core_nif");
