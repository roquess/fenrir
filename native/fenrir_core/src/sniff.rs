use crate::engine::split_csv;
use crate::recipe::{ConfidenceRules, CsvConfig, Field, FieldType, Recipe};

const CANDIDATE_SEPS: [char; 4] = [',', ';', '\t', '|'];
const QUOTE: char = '"';

pub fn sniff(sample: &str) -> Recipe {
    let lines: Vec<&str> = sample.lines().filter(|l| !l.is_empty()).collect();
    let sep = detect_sep(&lines);
    // Découpage cohérent avec l'engine (respecte les guillemets).
    let rows: Vec<Vec<String>> = lines.iter().map(|l| split_csv(l, sep, QUOTE)).collect();
    let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(1);
    let header = detect_header(&rows);

    let data_start = if header { 1 } else { 0 };
    let mut schema = Vec::with_capacity(ncols);
    for c in 0..ncols {
        let name = if header {
            rows[0]
                .get(c)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("col_{c}"))
        } else {
            format!("col_{c}")
        };
        let ty = infer_type(&rows[data_start..], c);
        schema.push(Field {
            name,
            ty,
            from: c,
            split: None,
            take: None,
        });
    }

    let signature = format!(
        "csv:sep={}:cols={}:{}",
        sep,
        ncols,
        if header { "hdr" } else { "nohdr" }
    );
    Recipe {
        signature,
        version: 1,
        backend: "csv".into(),
        config: CsvConfig {
            sep,
            encoding: "utf-8".into(),
            header,
            quote: '"',
        },
        schema,
        confidence_rules: ConfidenceRules::default(),
    }
}

fn detect_sep(lines: &[&str]) -> char {
    // Le séparateur dont le nombre d'occurrences est le plus constant entre lignes.
    let mut best = ',';
    let mut best_score = f64::INFINITY;
    for &cand in &CANDIDATE_SEPS {
        let counts: Vec<usize> = lines.iter().map(|l| l.matches(cand).count()).collect();
        let total: usize = counts.iter().sum();
        if total == 0 {
            continue;
        }
        let mean = total as f64 / counts.len() as f64;
        let var =
            counts.iter().map(|&c| (c as f64 - mean).powi(2)).sum::<f64>() / counts.len() as f64;
        // privilégie faible variance, pénalise séparateur jamais vu
        let score = var - mean * 0.001;
        if score < best_score {
            best_score = score;
            best = cand;
        }
    }
    best
}

fn detect_header(rows: &[Vec<String>]) -> bool {
    if rows.len() < 2 {
        return false;
    }
    // Header probable si la 1re ligne n'a aucune cellule numérique alors que
    // les suivantes en ont.
    let first_numeric = rows[0].iter().any(|c| c.trim().parse::<f64>().is_ok());
    let rest_numeric = rows[1..]
        .iter()
        .any(|r| r.iter().any(|c| c.trim().parse::<f64>().is_ok()));
    !first_numeric && rest_numeric
}

/// Seuil de tolérance : un type est retenu si au moins 70% des cellules
/// non vides s'y conforment. Tolère les valeurs aberrantes du monde réel.
const TYPE_THRESHOLD: f64 = 0.7;

fn infer_type(data: &[Vec<String>], col: usize) -> FieldType {
    let cells: Vec<&str> = data
        .iter()
        .filter_map(|r| r.get(col))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if cells.is_empty() {
        return FieldType::String;
    }
    let n = cells.len() as f64;
    let ratio = |f: &dyn Fn(&str) -> bool| cells.iter().filter(|c| f(c)).count() as f64 / n;

    if ratio(&|c| c.parse::<i64>().is_ok()) >= TYPE_THRESHOLD {
        return FieldType::Int;
    }
    if ratio(&|c| c.parse::<f64>().is_ok()) >= TYPE_THRESHOLD {
        return FieldType::Float;
    }
    if ratio(&|c| {
        matches!(
            c.to_lowercase().as_str(),
            "true" | "false" | "0" | "1" | "yes" | "no"
        )
    }) >= TYPE_THRESHOLD
    {
        return FieldType::Bool;
    }
    FieldType::String
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::FieldType;

    #[test]
    fn detects_semicolon_header_and_int_column() {
        let sample = "name;age;city\nAlice;30;Paris\nBob;25;Lyon\n";
        let r = sniff(sample);
        assert_eq!(r.config.sep, ';');
        assert!(r.config.header);
        assert_eq!(r.schema.len(), 3);
        assert_eq!(r.schema[1].name, "age");
        assert_eq!(r.schema[1].ty, FieldType::Int);
        assert_eq!(r.schema[0].ty, FieldType::String);
    }

    #[test]
    fn detects_comma_no_header() {
        let sample = "Alice,30\nBob,25\nCarol,40\n";
        let r = sniff(sample);
        assert_eq!(r.config.sep, ',');
        assert!(!r.config.header);
        assert_eq!(r.schema[0].name, "col_0");
    }

    #[test]
    fn signature_is_stable() {
        let s = "a;b\n1;2\n";
        assert_eq!(sniff(s).signature, sniff(s).signature);
    }
}
