use fenrir_core::engine::parse_line;
use fenrir_core::load::load;
use fenrir_core::sandbox::run_snippet;
use fenrir_core::sniff::{relearn, sniff};
use fenrir_core::text::{parse_text, recipe_from_pattern};
use fenrir_core::xml::{parse_record_xml, sniff_xml};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn sniff_then_parse_never_panics(bytes in any::<Vec<u8>>()) {
        let s = String::from_utf8_lossy(&bytes);
        let recipe = sniff(&s);
        for line in s.lines() {
            let out = parse_line(&recipe, line);
            prop_assert!(out.confidence >= 0.0 && out.confidence <= 1.0);
        }
    }

    #[test]
    fn relearn_never_panics(bytes in any::<Vec<u8>>()) {
        let base = sniff("a;b\n1;2\n");
        let s = String::from_utf8_lossy(&bytes);
        let r = relearn(&base, &s);
        prop_assert_eq!(r.version, base.version + 1);
        prop_assert_eq!(r.signature, base.signature);
    }

    #[test]
    fn xml_never_panics(bytes in any::<Vec<u8>>()) {
        let s = String::from_utf8_lossy(&bytes);
        let r = sniff_xml(&s);
        let _ = parse_record_xml(&r, &s);
        prop_assert!(true);
    }

    #[test]
    fn text_never_panics(p in any::<Vec<u8>>(), l in any::<Vec<u8>>()) {
        let pat = String::from_utf8_lossy(&p);
        let line = String::from_utf8_lossy(&l);
        let r = recipe_from_pattern(&pat);
        let out = parse_text(&r, &line);
        prop_assert!(out.confidence >= 0.0 && out.confidence <= 1.0);
    }

    #[test]
    fn sandbox_never_panics(code in any::<Vec<u8>>(), input in any::<Vec<u8>>()) {
        let c = String::from_utf8_lossy(&code);
        let i = String::from_utf8_lossy(&input);
        let _ = run_snippet(&c, &i); // Ok | Err, never a panic
        prop_assert!(true);
    }

    #[test]
    fn load_never_panics(bytes in any::<Vec<u8>>()) {
        let s = String::from_utf8_lossy(&bytes);
        let _ = load(&serde_json::Value::String(s.to_string()), "json");
        prop_assert!(true);
    }
}
