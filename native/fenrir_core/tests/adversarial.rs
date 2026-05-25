use fenrir_core::engine::parse_line;
use fenrir_core::sniff::sniff;

/// Sniff + parse every line; must not panic, confidence stays in range.
fn parse_all(s: &str) {
    let recipe = sniff(s);
    for line in s.lines() {
        let out = parse_line(&recipe, line);
        assert!(out.confidence >= 0.0 && out.confidence <= 1.0);
    }
}

#[test]
fn empty_input() {
    let r = sniff("");
    assert!(!r.schema.is_empty()); // defaults to one column
    parse_all("");
}

#[test]
fn header_only() {
    parse_all("name;age;city\n");
}

#[test]
fn blank_lines_interspersed() {
    parse_all("name;age\n\nAlice;30\n\n\nBob;25\n");
}

#[test]
fn leading_bom_is_stripped() {
    let r = sniff("\u{FEFF}name;age\nAlice;30\nBob;25\n");
    assert_eq!(r.schema[0].name, "name"); // not "\u{FEFF}name"
}

#[test]
fn huge_field_no_blowup() {
    let big = "x".repeat(100_000);
    let s = format!("a;b\n{};2\n", big);
    parse_all(&s);
}

#[test]
fn ragged_rows() {
    parse_all("a;b;c\n1;2\n3;4;5;6\n7\n");
}

#[test]
fn ambiguous_separators() {
    parse_all("a;b,c\n1;2,3\nx;y,z\n");
}

#[test]
fn multibyte_values() {
    parse_all("naïve;âge\nété;30\nNoël;40\n");
}
