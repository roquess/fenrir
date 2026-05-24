use fenrir_core::{engine::parse_line, sniff::sniff};

#[test]
fn golden_messy1() {
    let data = include_str!("corpus/messy1.csv");
    let recipe = sniff(data);
    assert_eq!(recipe.config.sep, ';');
    assert!(recipe.config.header);

    let mut lines = data.lines();
    lines.next(); // header

    let alice = parse_line(&recipe, lines.next().unwrap());
    assert_eq!(alice.value["name"], serde_json::json!("Alice"));
    assert!((alice.confidence - 1.0).abs() < 1e-9);

    let bob = parse_line(&recipe, lines.next().unwrap());
    assert_eq!(bob.value["age"], serde_json::Value::Null); // "notanumber"
    assert!(bob.confidence < 1.0);

    let carol = parse_line(&recipe, lines.next().unwrap());
    assert_eq!(carol.value["name"], serde_json::json!("Carol; the great")); // quoted sep
}
