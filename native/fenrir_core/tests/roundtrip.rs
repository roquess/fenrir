use fenrir_core::{engine::parse_line, sniff::sniff};
use proptest::prelude::*;

proptest! {
    #[test]
    fn parse_never_panics_and_confidence_in_range(
        rows in proptest::collection::vec("[a-zA-Z0-9]{1,8}(;[a-zA-Z0-9]{1,8}){0,4}", 1..20)
    ) {
        let sample = format!("{}\n", rows.join("\n"));
        let recipe = sniff(&sample);
        for line in sample.lines() {
            let out = parse_line(&recipe, line);
            prop_assert!(out.confidence >= 0.0 && out.confidence <= 1.0);
        }
    }
}
