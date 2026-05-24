use fenrir_core::{
    engine::parse_line,
    load::load,
    recipe::Recipe,
    sniff::{relearn, sniff},
    xml::{detect_format, parse_record_xml, sniff_xml, XmlRecipe},
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

#[rustler::nif(name = "detect_format")]
fn detect_format_nif(sample: String) -> String {
    detect_format(&sample).to_string()
}

#[rustler::nif(name = "sniff_xml")]
fn sniff_xml_nif(sample: String) -> String {
    let recipe = sniff_xml(&sample);
    serde_json::to_string(&recipe).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[rustler::nif(name = "parse_xml")]
fn parse_xml_nif(recipe_json: String, fragment: String) -> (String, f64) {
    let recipe: XmlRecipe = match serde_json::from_str(&recipe_json) {
        Ok(r) => r,
        Err(e) => return (format!("{{\"error\":\"{e}\"}}"), 0.0),
    };
    let out = parse_record_xml(&recipe, &fragment);
    (serde_json::to_string(&out.value).unwrap_or_default(), out.confidence)
}

#[rustler::nif(name = "load")]
fn load_nif(value_json: String, format: String) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(&value_json).map_err(|e| e.to_string())?;
    load(&value, &format)
}

rustler::init!("fenrir_core_nif");
