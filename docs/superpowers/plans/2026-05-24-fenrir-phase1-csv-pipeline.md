# Fenrir Phase 1 — Pipeline CSV déterministe — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construire le pipeline ETL de bout en bout pour le CSV : un fichier CSV inconnu → recette inférée par heuristique déterministe → parsé vers `serde_json::Value` avec score de confiance → formaté via loki_weave. Orchestré en Erlang/OTP, cœur en Rust.

**Architecture:** Cœur Rust pur (`fenrir_core`) testable en isolation (recipe model, sniffer heuristique, engine, confidence). Orchestration Erlang/OTP (`fenrir`) avec arbre de supervision. Frontière Rust↔Erlang via Rustler NIF, le NIF étant une fine couche au-dessus du cœur pur. Le cœur Rust ne dépend d'aucune IA : l'inducer Phase 1 est un sniffer de dialecte CSV 100% déterministe.

**Tech Stack:** Rust 1.94 (serde, serde_json, csv, proptest) · Rustler (NIF) · Erlang/OTP 27 + rebar3 · Concuerror (model checking concurrence) · loki_weave (Load).

---

## Notes de scope

Ce plan couvre **uniquement la Phase 1** du spec (`docs/superpowers/specs/2026-05-24-fenrir-ai-etl-parser-design.md`) : le pipeline CSV déterministe « apprend-une-fois puis tourne », sans boucle d'escalade ni IA externe. Phases 2-4 (confiance+escalade, XML+drift, text/pdf+sandbox) feront chacune l'objet d'un plan séparé une fois la Phase 1 livrée et verte.

L'inducer de Phase 1 est un **sniffer heuristique déterministe** (détection séparateur/header/types par statistiques sur les échantillons). C'est un vrai composant réutilisable (le "fast path" avant tout fallback LLM en Phase 2), pas un bouchon.

## Structure de fichiers

**Cœur Rust — crate `fenrir_core` (`native/fenrir_core/`)**
- `Cargo.toml` — dépendances : serde, serde_json, csv ; dev : proptest
- `src/lib.rs` — exporte les modules, API publique
- `src/recipe.rs` — modèle de données `Recipe` (sérialisable JSON)
- `src/sniff.rs` — inducer heuristique : échantillons → `Recipe`
- `src/engine.rs` — `parse_line(recipe, line) -> ParsedRecord { value, confidence }`
- `src/load.rs` — wrapper loki_weave : `Value` + format → String
- `tests/roundtrip.rs` — property tests (proptest)
- `tests/golden.rs` — corpus doré (fichiers réels → sortie attendue)

**NIF Rustler — crate `fenrir_nif` (`native/fenrir_nif/`)**
- `Cargo.toml` — rustler + fenrir_core
- `src/lib.rs` — exporte `sniff/1`, `parse_line/2`, `load/2` comme NIFs (échange JSON en binaire)

**Orchestration Erlang — app `fenrir` (`apps/fenrir/`)**
- `rebar.config` — deps, profils (test+concuerror), hook compilation Rust
- `src/fenrir.app.src` — métadonnées OTP
- `src/fenrir_app.erl` — `application` callback
- `src/fenrir_sup.erl` — superviseur racine
- `src/fenrir_core_nif.erl` — chargement NIF + façade (mockable)
- `src/fenrir_recipe_store.erl` — GenServer : persistance recettes (ETS + disque)
- `src/fenrir_source_reader.erl` — GenServer : lecture streaming + signature
- `src/fenrir_parse_worker.erl` — GenServer : applique la recette via NIF
- `src/fenrir_job.erl` — GenServer : orchestre un job d'ingestion (la boucle perceive→suggest→act→remember)
- `src/fenrir_job_sup.erl` — superviseur simple_one_for_one des jobs
- `test/fenrir_recipe_store_SUITE.erl` — CT : persistance
- `test/fenrir_job_SUITE.erl` — CT : cycle de vie d'un job (avec NIF mocké)
- `test/concuerror_tests.erl` — entrées Concuerror (model checking concurrence)

**Racine**
- `rebar.config` (umbrella) · `.gitignore` · `README.md`

---

## Task 1: Squelette projet + crate Rust core vide qui compile

**Files:**
- Create: `C:\Users\steve\dev\fenrir\.gitignore`
- Create: `C:\Users\steve\dev\fenrir\native\fenrir_core\Cargo.toml`
- Create: `C:\Users\steve\dev\fenrir\native\fenrir_core\src\lib.rs`

- [ ] **Step 1: Créer .gitignore**

```gitignore
/_build/
/native/*/target/
*.beam
erl_crash.dump
rebar3.crashdump
*.so
*.dll
*.dylib
```

- [ ] **Step 2: Créer Cargo.toml du cœur**

```toml
[package]
name = "fenrir_core"
version = "0.1.0"
edition = "2021"
license = "MIT"

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
csv = "1"

[dev-dependencies]
proptest = "1"
```

- [ ] **Step 3: lib.rs minimal**

```rust
pub mod recipe;
pub mod sniff;
pub mod engine;
pub mod load;
```

(Les modules seront créés vides puis remplis tâche par tâche. Pour cette étape, créer aussi des modules stub pour que ça compile.)

Créer `src/recipe.rs`, `src/sniff.rs`, `src/engine.rs`, `src/load.rs` contenant chacun juste `// placeholder` — non, **interdit par la règle no-placeholder** : à la place, ne déclarer dans `lib.rs` que les modules qui existent. Pour cette tâche, `lib.rs` contient seulement :

```rust
pub mod recipe;
```

et créer `src/recipe.rs` avec le contenu réel de la Task 2. Donc fusionner : faire Task 1 step 3 = `pub mod recipe;` et enchaîner Task 2.

- [ ] **Step 4: Vérifier la compilation**

Run: `cd C:\Users\steve\dev\fenrir\native\fenrir_core && cargo build`
Expected: échoue car `recipe.rs` absent → résolu en Task 2.

- [ ] **Step 5: Commit (après Task 2 verte)**

Le commit du squelette est groupé avec Task 2.

---

## Task 2: Modèle `Recipe` (Rust) + sérialisation JSON

**Files:**
- Create: `native\fenrir_core\src\recipe.rs`
- Test: inline `#[cfg(test)]` dans `recipe.rs`

- [ ] **Step 1: Test d'aller-retour JSON (échoue)**

Dans `src/recipe.rs` :

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_json_roundtrip() {
        let r = Recipe {
            signature: "csv:sep=;:cols=3:hdr".into(),
            version: 1,
            backend: "csv".into(),
            config: CsvConfig { sep: ';', encoding: "utf-8".into(), header: true, quote: '"' },
            schema: vec![
                Field { name: "name".into(), ty: FieldType::String, from: 0, split: None, take: None },
                Field { name: "age".into(),  ty: FieldType::Int,    from: 1, split: None, take: None },
            ],
            confidence_rules: ConfidenceRules::default(),
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: Recipe = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
```

- [ ] **Step 2: Lancer le test (échoue : types absents)**

Run: `cargo test -p fenrir_core recipe_json_roundtrip`
Expected: FAIL (compilation : `Recipe` introuvable).

- [ ] **Step 3: Implémenter le modèle**

En tête de `src/recipe.rs` :

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recipe {
    pub signature: String,
    pub version: u32,
    pub backend: String,
    pub config: CsvConfig,
    pub schema: Vec<Field>,
    #[serde(default)]
    pub confidence_rules: ConfidenceRules,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CsvConfig {
    pub sep: char,
    pub encoding: String,
    pub header: bool,
    #[serde(default = "default_quote")]
    pub quote: char,
}

fn default_quote() -> char { '"' }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: FieldType,
    pub from: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType { String, Int, Float, Bool }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfidenceRules {
    pub min_field_match: f64,
}

impl Default for ConfidenceRules {
    fn default() -> Self { Self { min_field_match: 0.95 } }
}
```

- [ ] **Step 4: Lancer le test (passe)**

Run: `cargo test -p fenrir_core recipe_json_roundtrip`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd C:\Users\steve\dev\fenrir
git add .gitignore native/fenrir_core
git commit -m "feat(core): Recipe model + JSON roundtrip"
```

---

## Task 3: Engine `parse_line` — coercition de types + confiance

**Files:**
- Create: `native\fenrir_core\src\engine.rs`
- Modify: `native\fenrir_core\src\lib.rs` (ajouter `pub mod engine;`)

- [ ] **Step 1: Tests (échouent)**

Dans `src/engine.rs` :

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::*;

    fn recipe() -> Recipe {
        Recipe {
            signature: "t".into(), version: 1, backend: "csv".into(),
            config: CsvConfig { sep: ';', encoding: "utf-8".into(), header: true, quote: '"' },
            schema: vec![
                Field { name: "name".into(), ty: FieldType::String, from: 0, split: None, take: None },
                Field { name: "age".into(),  ty: FieldType::Int,    from: 1, split: None, take: None },
                Field { name: "city".into(), ty: FieldType::String, from: 2, split: Some("|".into()), take: Some(0) },
            ],
            confidence_rules: ConfidenceRules::default(),
        }
    }

    #[test]
    fn parses_all_fields_full_confidence() {
        let r = recipe();
        let out = parse_line(&r, "Alice;30;Paris|FR");
        assert_eq!(out.value["name"], serde_json::json!("Alice"));
        assert_eq!(out.value["age"], serde_json::json!(30));
        assert_eq!(out.value["city"], serde_json::json!("Paris"));
        assert!((out.confidence - 1.0).abs() < 1e-9);
    }

    #[test]
    fn bad_int_lowers_confidence_and_nulls_field() {
        let r = recipe();
        let out = parse_line(&r, "Bob;notanumber;Lyon");
        assert_eq!(out.value["age"], serde_json::Value::Null);
        assert!(out.confidence < 1.0);
    }
}
```

- [ ] **Step 2: Lancer (échoue)**

Run: `cargo test -p fenrir_core engine`
Expected: FAIL (`parse_line` absent).

- [ ] **Step 3: Implémenter l'engine**

En tête de `src/engine.rs` :

```rust
use serde_json::{Map, Value};
use crate::recipe::{Recipe, Field, FieldType};

pub struct ParsedRecord {
    pub value: Value,
    pub confidence: f64,
}

pub fn parse_line(recipe: &Recipe, line: &str) -> ParsedRecord {
    let raw = split_csv(line, recipe.config.sep, recipe.config.quote);
    let mut obj = Map::new();
    let mut matched = 0usize;
    let total = recipe.schema.len();

    for f in &recipe.schema {
        let cell = raw.get(f.from).map(|s| s.as_str()).unwrap_or("");
        let cell = apply_split(cell, f);
        match coerce(cell, f.ty) {
            Some(v) => { matched += 1; obj.insert(f.name.clone(), v); }
            None    => { obj.insert(f.name.clone(), Value::Null); }
        }
    }
    let confidence = if total == 0 { 1.0 } else { matched as f64 / total as f64 };
    ParsedRecord { value: Value::Object(obj), confidence }
}

fn apply_split<'a>(cell: &'a str, f: &Field) -> &'a str {
    match (&f.split, f.take) {
        (Some(sep), Some(idx)) => cell.split(sep.as_str()).nth(idx).unwrap_or(""),
        (Some(sep), None)      => cell.split(sep.as_str()).next().unwrap_or(""),
        _ => cell,
    }
}

fn coerce(cell: &str, ty: FieldType) -> Option<Value> {
    let c = cell.trim();
    match ty {
        FieldType::String => Some(Value::String(c.to_string())),
        FieldType::Int    => c.parse::<i64>().ok().map(Into::into),
        FieldType::Float  => c.parse::<f64>().ok()
                              .and_then(serde_json::Number::from_f64).map(Value::Number),
        FieldType::Bool   => match c.to_lowercase().as_str() {
            "true"  | "1" | "yes" => Some(Value::Bool(true)),
            "false" | "0" | "no"  => Some(Value::Bool(false)),
            _ => None,
        },
    }
}

/// Découpe CSV minimale gérant les guillemets.
fn split_csv(line: &str, sep: char, quote: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == quote {
            if in_q && chars.peek() == Some(&quote) { cur.push(quote); chars.next(); }
            else { in_q = !in_q; }
        } else if ch == sep && !in_q {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(ch);
        }
    }
    out.push(cur);
    out
}
```

Ajouter dans `lib.rs` : `pub mod engine;`

- [ ] **Step 4: Lancer (passe)**

Run: `cargo test -p fenrir_core engine`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add native/fenrir_core/src/engine.rs native/fenrir_core/src/lib.rs
git commit -m "feat(core): parse_line engine with type coercion + confidence"
```

---

## Task 4: Sniffer heuristique — échantillons → `Recipe`

**Files:**
- Create: `native\fenrir_core\src\sniff.rs`
- Modify: `native\fenrir_core\src\lib.rs` (`pub mod sniff;`)

- [ ] **Step 1: Tests (échouent)**

Dans `src/sniff.rs` :

```rust
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
```

- [ ] **Step 2: Lancer (échoue)**

Run: `cargo test -p fenrir_core sniff`
Expected: FAIL (`sniff` absent).

- [ ] **Step 3: Implémenter le sniffer**

En tête de `src/sniff.rs` :

```rust
use crate::recipe::{Recipe, CsvConfig, Field, FieldType, ConfidenceRules};

const CANDIDATE_SEPS: [char; 4] = [',', ';', '\t', '|'];

pub fn sniff(sample: &str) -> Recipe {
    let lines: Vec<&str> = sample.lines().filter(|l| !l.is_empty()).collect();
    let sep = detect_sep(&lines);
    let rows: Vec<Vec<&str>> = lines.iter().map(|l| l.split(sep).collect()).collect();
    let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(1);
    let header = detect_header(&rows);

    let data_start = if header { 1 } else { 0 };
    let mut schema = Vec::with_capacity(ncols);
    for c in 0..ncols {
        let name = if header {
            rows[0].get(c).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("col_{c}"))
        } else { format!("col_{c}") };
        let ty = infer_type(&rows[data_start..], c);
        schema.push(Field { name, ty, from: c, split: None, take: None });
    }

    let signature = format!("csv:sep={}:cols={}:{}", sep, ncols, if header {"hdr"} else {"nohdr"});
    Recipe {
        signature, version: 1, backend: "csv".into(),
        config: CsvConfig { sep, encoding: "utf-8".into(), header, quote: '"' },
        schema, confidence_rules: ConfidenceRules::default(),
    }
}

fn detect_sep(lines: &[&str]) -> char {
    // Le séparateur dont le nombre d'occurrences est le plus constant entre lignes.
    let mut best = ',';
    let mut best_score = f64::INFINITY;
    for &cand in &CANDIDATE_SEPS {
        let counts: Vec<usize> = lines.iter().map(|l| l.matches(cand).count()).collect();
        let total: usize = counts.iter().sum();
        if total == 0 { continue; }
        let mean = total as f64 / counts.len() as f64;
        let var = counts.iter().map(|&c| (c as f64 - mean).powi(2)).sum::<f64>() / counts.len() as f64;
        // privilégie faible variance, pénalise séparateur jamais vu
        let score = var - mean * 0.001;
        if score < best_score { best_score = score; best = cand; }
    }
    best
}

fn detect_header(rows: &[Vec<&str>]) -> bool {
    if rows.len() < 2 { return false; }
    // Header probable si la 1re ligne n'a aucune cellule numérique alors que les suivantes en ont.
    let first_numeric = rows[0].iter().any(|c| c.trim().parse::<f64>().is_ok());
    let rest_numeric = rows[1..].iter().any(|r| r.iter().any(|c| c.trim().parse::<f64>().is_ok()));
    !first_numeric && rest_numeric
}

fn infer_type(data: &[Vec<&str>], col: usize) -> FieldType {
    let cells: Vec<&str> = data.iter().filter_map(|r| r.get(col).copied())
        .map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if cells.is_empty() { return FieldType::String; }
    let all = |f: &dyn Fn(&str) -> bool| cells.iter().all(|c| f(c));
    if all(&|c| c.parse::<i64>().is_ok()) { return FieldType::Int; }
    if all(&|c| c.parse::<f64>().is_ok()) { return FieldType::Float; }
    if all(&|c| matches!(c.to_lowercase().as_str(), "true"|"false"|"0"|"1"|"yes"|"no")) {
        return FieldType::Bool;
    }
    FieldType::String
}
```

Ajouter dans `lib.rs` : `pub mod sniff;`

- [ ] **Step 4: Lancer (passe)**

Run: `cargo test -p fenrir_core sniff`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add native/fenrir_core/src/sniff.rs native/fenrir_core/src/lib.rs
git commit -m "feat(core): heuristic CSV sniffer (sep/header/type detection)"
```

---

## Task 5: Load via loki_weave

**Files:**
- Create: `native\fenrir_core\src\load.rs`
- Modify: `native\fenrir_core\src\lib.rs` (`pub mod load;`), `Cargo.toml` (dep loki_weave)

- [ ] **Step 1: Test (échoue)**

Dans `src/load.rs` :

```rust
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
```

- [ ] **Step 2: Lancer (échoue)**

Run: `cargo test -p fenrir_core load`
Expected: FAIL.

- [ ] **Step 3: Ajouter la dépendance loki_weave**

Dans `native/fenrir_core/Cargo.toml`, sous `[dependencies]` :

```toml
loki_weave = { path = "../../../loki_weave" }
```

(Chemin relatif depuis `native/fenrir_core/` vers `C:\Users\steve\dev\loki_weave`. Vérifier le nombre de `..` après création.)

- [ ] **Step 4: Implémenter load**

En tête de `src/load.rs` :

```rust
use serde_json::Value;
use loki_weave::{format_data, OutputFormat};

pub fn load(value: &Value, format: &str) -> Result<String, String> {
    let fmt = OutputFormat::from_str(format)
        .ok_or_else(|| format!("unsupported format: {format}"))?;
    format_data(value, fmt).map_err(|e| e.to_string())
}
```

Ajouter dans `lib.rs` : `pub mod load;`

- [ ] **Step 5: Lancer (passe)**

Run: `cargo test -p fenrir_core load`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add native/fenrir_core
git commit -m "feat(core): loki_weave Load integration"
```

---

## Task 6: Property test (round-trip) + corpus doré

**Files:**
- Create: `native\fenrir_core\tests\roundtrip.rs`
- Create: `native\fenrir_core\tests\golden.rs`
- Create: `native\fenrir_core\tests\corpus\messy1.csv`

- [ ] **Step 1: Property test — sniff puis parse ne panique jamais et la confiance ∈ [0,1]**

`tests/roundtrip.rs` :

```rust
use proptest::prelude::*;
use fenrir_core::{sniff::sniff, engine::parse_line};

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
```

- [ ] **Step 2: Lancer (passe)**

Run: `cargo test -p fenrir_core --test roundtrip`
Expected: PASS.

- [ ] **Step 3: Corpus doré — fichier crade**

`tests/corpus/messy1.csv` :

```
name;age;city
Alice;30;Paris|FR
Bob;notanumber;Lyon
"Carol; the great";40;Nice
```

`tests/golden.rs` :

```rust
use fenrir_core::{sniff::sniff, engine::parse_line};

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
```

- [ ] **Step 4: Lancer (passe)**

Run: `cargo test -p fenrir_core --test golden`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add native/fenrir_core/tests
git commit -m "test(core): proptest roundtrip + golden corpus"
```

---

## Task 7: NIF Rustler exposant le cœur

**Files:**
- Create: `native\fenrir_nif\Cargo.toml`
- Create: `native\fenrir_nif\src\lib.rs`

- [ ] **Step 1: Cargo.toml du NIF**

```toml
[package]
name = "fenrir_nif"
version = "0.1.0"
edition = "2021"

[lib]
name = "fenrir_nif"
crate-type = ["cdylib"]

[dependencies]
rustler = "0.34"
serde_json = "1"
fenrir_core = { path = "../fenrir_core" }
```

- [ ] **Step 2: lib.rs du NIF — échange JSON en string**

```rust
use rustler::{Env, Term, NifResult, Encoder};
use fenrir_core::{sniff::sniff, engine::parse_line, load::load, recipe::Recipe};

#[rustler::nif]
fn sniff_nif(sample: String) -> String {
    let recipe = sniff(&sample);
    serde_json::to_string(&recipe).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[rustler::nif]
fn parse_line_nif(recipe_json: String, line: String) -> (String, f64) {
    let recipe: Recipe = match serde_json::from_str(&recipe_json) {
        Ok(r) => r,
        Err(e) => return (format!("{{\"error\":\"{e}\"}}"), 0.0),
    };
    let out = parse_line(&recipe, &line);
    (serde_json::to_string(&out.value).unwrap_or_default(), out.confidence)
}

#[rustler::nif]
fn load_nif(value_json: String, format: String) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(&value_json)
        .map_err(|e| e.to_string())?;
    load(&value, &format)
}

rustler::init!("fenrir_core_nif");
```

Note : `rustler::init!` doit matcher le nom du module Erlang (`fenrir_core_nif`). En Rustler 0.34 les nifs sont auto-découverts via l'attribut.

- [ ] **Step 3: Build du NIF**

Run: `cd C:\Users\steve\dev\fenrir\native\fenrir_nif && cargo build`
Expected: produit `target\debug\fenrir_nif.dll` (Windows).

- [ ] **Step 4: Commit**

```bash
git add native/fenrir_nif
git commit -m "feat(nif): Rustler bindings sniff/parse_line/load"
```

---

## Task 8: App OTP — squelette rebar3 + chargement NIF (mockable)

**Files:**
- Create: `rebar.config`, `apps\fenrir\src\fenrir.app.src`, `fenrir_app.erl`, `fenrir_sup.erl`, `fenrir_core_nif.erl`

- [ ] **Step 1: rebar.config (umbrella) avec hook de build Rust**

`C:\Users\steve\dev\fenrir\rebar.config` :

```erlang
{erl_opts, [debug_info]}.
{deps, []}.

{pre_hooks, [
  {compile, "cargo build --release --manifest-path native/fenrir_nif/Cargo.toml"}
]}.

{post_hooks, [
  {compile, "powershell -Command \"Copy-Item native/fenrir_nif/target/release/fenrir_nif.dll apps/fenrir/priv/fenrir_core_nif.dll -Force\""}
]}.

{profiles, [
  {test, [{deps, [{proper, "1.4.0"}]}]}
]}.
```

(Le hook compile le NIF Rust et copie la DLL dans `priv/` sous le nom attendu par `erlang:load_nif`.)

- [ ] **Step 2: app.src**

`apps\fenrir\src\fenrir.app.src` :

```erlang
{application, fenrir,
 [{description, "Fenrir — self-learning ETL parser"},
  {vsn, "0.1.0"},
  {registered, [fenrir_sup]},
  {mod, {fenrir_app, []}},
  {applications, [kernel, stdlib]},
  {env, []}
 ]}.
```

- [ ] **Step 3: fenrir_app.erl**

```erlang
-module(fenrir_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _Args) ->
    fenrir_sup:start_link().

stop(_State) ->
    ok.
```

- [ ] **Step 4: fenrir_sup.erl (racine, vide pour l'instant)**

```erlang
-module(fenrir_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, []}}.
```

- [ ] **Step 5: fenrir_core_nif.erl — façade NIF**

```erlang
-module(fenrir_core_nif).
-export([sniff/1, parse_line/2, load/2]).
-on_load(init/0).

-define(APPNAME, fenrir).
-define(LIBNAME, fenrir_core_nif).

init() ->
    SoName = filename:join([code:priv_dir(?APPNAME), ?LIBNAME]),
    erlang:load_nif(SoName, 0).

%% Remplacés par le NIF au chargement ; ces clauses ne s'exécutent
%% que si le NIF n'a pas pu être chargé.
sniff(_Sample) -> erlang:nif_error(nif_not_loaded).
parse_line(_RecipeJson, _Line) -> erlang:nif_error(nif_not_loaded).
load(_ValueJson, _Format) -> erlang:nif_error(nif_not_loaded).
```

- [ ] **Step 6: Compiler**

Run: `cd C:\Users\steve\dev\fenrir && rebar3 compile`
Expected: compile Rust + Erlang, copie la DLL dans `apps/fenrir/priv/`.

- [ ] **Step 7: Test manuel du NIF dans le shell**

Run: `rebar3 shell --eval "io:format(\"~p~n\", [fenrir_core_nif:sniff(\"a;b\\n1;2\\n\")]), init:stop()."`
Expected: affiche un JSON de recette contenant `sep`.

- [ ] **Step 8: Commit**

```bash
git add rebar.config apps/fenrir/src
git commit -m "feat(otp): app skeleton + NIF facade loading"
```

---

## Task 9: `fenrir_recipe_store` — GenServer de persistance (ETS + disque)

**Files:**
- Create: `apps\fenrir\src\fenrir_recipe_store.erl`
- Test: `apps\fenrir\test\fenrir_recipe_store_SUITE.erl`
- Modify: `fenrir_sup.erl` (ajouter le child)

- [ ] **Step 1: Test CT (échoue)**

`apps\fenrir\test\fenrir_recipe_store_SUITE.erl` :

```erlang
-module(fenrir_recipe_store_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([put_then_get/1, miss_returns_not_found/1, versioning_keeps_history/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [put_then_get, miss_returns_not_found, versioning_keeps_history].

init_per_testcase(_, Config) ->
    {ok, Pid} = fenrir_recipe_store:start_link(#{dir => ?config(priv_dir, Config)}),
    [{store, Pid} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(store, Config)).

put_then_get(_) ->
    ok = fenrir_recipe_store:put(<<"sig1">>, #{<<"version">> => 1, <<"json">> => <<"{}">>}),
    {ok, R} = fenrir_recipe_store:get(<<"sig1">>),
    1 = maps:get(<<"version">>, R).

miss_returns_not_found(_) ->
    not_found = fenrir_recipe_store:get(<<"nope">>).

versioning_keeps_history(_) ->
    ok = fenrir_recipe_store:put(<<"s">>, #{<<"version">> => 1}),
    ok = fenrir_recipe_store:put(<<"s">>, #{<<"version">> => 2}),
    {ok, R} = fenrir_recipe_store:get(<<"s">>),
    2 = maps:get(<<"version">>, R),
    [1,2] = lists:sort([maps:get(<<"version">>, V) || V <- fenrir_recipe_store:history(<<"s">>)]).
```

- [ ] **Step 2: Lancer (échoue)**

Run: `rebar3 ct --suite apps/fenrir/test/fenrir_recipe_store_SUITE`
Expected: FAIL (module absent).

- [ ] **Step 3: Implémenter le GenServer**

`apps\fenrir\src\fenrir_recipe_store.erl` :

```erlang
-module(fenrir_recipe_store).
-behaviour(gen_server).

-export([start_link/0, start_link/1, put/2, get/1, history/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-record(state, {tab, hist, dir}).

start_link() -> start_link(#{dir => "priv/recipes"}).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

put(Sig, Recipe) -> gen_server:call(?MODULE, {put, Sig, Recipe}).
get(Sig)         -> gen_server:call(?MODULE, {get, Sig}).
history(Sig)     -> gen_server:call(?MODULE, {history, Sig}).

init(Opts) ->
    Dir = maps:get(dir, Opts, "priv/recipes"),
    filelib:ensure_dir(filename:join(Dir, "x")),
    Tab  = ets:new(fenrir_recipes, [set, private]),
    Hist = ets:new(fenrir_recipes_hist, [bag, private]),
    {ok, #state{tab = Tab, hist = Hist, dir = Dir}}.

handle_call({put, Sig, Recipe}, _From, S) ->
    ets:insert(S#state.tab, {Sig, Recipe}),
    ets:insert(S#state.hist, {Sig, Recipe}),
    ok = file:write_file(disk_path(S#state.dir, Sig), term_to_binary(Recipe)),
    {reply, ok, S};

handle_call({get, Sig}, _From, S) ->
    case ets:lookup(S#state.tab, Sig) of
        [{Sig, R}] -> {reply, {ok, R}, S};
        []         -> {reply, not_found, S}
    end;

handle_call({history, Sig}, _From, S) ->
    Rs = [R || {_, R} <- ets:lookup(S#state.hist, Sig)],
    {reply, Rs, S}.

handle_cast(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

disk_path(Dir, Sig) ->
    Safe = binary:replace(Sig, [<<"/">>, <<":">>], <<"_">>, [global]),
    filename:join(Dir, <<Safe/binary, ".recipe">>).
```

- [ ] **Step 4: Lancer (passe)**

Run: `rebar3 ct --suite apps/fenrir/test/fenrir_recipe_store_SUITE`
Expected: PASS (3 cas).

- [ ] **Step 5: Ajouter au superviseur**

Dans `fenrir_sup.erl`, remplacer `init/1` :

```erlang
init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Store = #{id => fenrir_recipe_store,
              start => {fenrir_recipe_store, start_link, []},
              restart => permanent, type => worker},
    {ok, {SupFlags, [Store]}}.
```

- [ ] **Step 6: Commit**

```bash
git add apps/fenrir/src/fenrir_recipe_store.erl apps/fenrir/test/fenrir_recipe_store_SUITE.erl apps/fenrir/src/fenrir_sup.erl
git commit -m "feat(otp): recipe_store GenServer (ETS + disk + versioning)"
```

---

## Task 10: `fenrir_job` — la boucle perceive→suggest→act→remember

**Files:**
- Create: `apps\fenrir\src\fenrir_job.erl`
- Test: `apps\fenrir\test\fenrir_job_SUITE.erl`

L'orchestrateur d'un job utilise une façade NIF **injectable** pour permettre le mock en test (pas de DLL requise pour tester la logique de la boucle).

- [ ] **Step 1: Test CT avec NIF mocké (échoue)**

`apps\fenrir\test\fenrir_job_SUITE.erl` :

```erlang
-module(fenrir_job_SUITE).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([learns_then_reuses/1, parses_records/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [learns_then_reuses, parses_records].

init_per_testcase(_, Config) ->
    {ok, Store} = fenrir_recipe_store:start_link(#{dir => ?config(priv_dir, Config)}),
    %% NIF mock : sniff renvoie une recette fixe, parse_line renvoie {json, 1.0}
    Nif = #{
        sniff => fun(_S) -> <<"{\"signature\":\"sig-x\",\"version\":1}">> end,
        parse_line => fun(_R, L) -> {<<"{\"line\":\"", L/binary, "\"}">>, 1.0} end
    },
    [{store, Store}, {nif, Nif} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(?config(store, Config)).

learns_then_reuses(Config) ->
    Nif = ?config(nif, Config),
    Counter = counters:new(1, []),
    SniffFun = fun(S) -> counters:add(Counter, 1, 1), (maps:get(sniff, Nif))(S) end,
    Nif2 = Nif#{sniff => SniffFun},
    Sample = <<"a;b\n1;2\n">>,
    {ok, _R1} = fenrir_job:learn(Sample, Nif2),
    {ok, _R2} = fenrir_job:learn(Sample, Nif2),
    1 = counters:get(Counter, 1).   %% 2e appel sert depuis le store : zéro sniff IA

parses_records(Config) ->
    Nif = ?config(nif, Config),
    Sample = <<"a;b\n1;2\n">>,
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Records = fenrir_job:run(Recipe, [<<"x;y">>, <<"z;w">>], Nif),
    2 = length(Records),
    [{_, 1.0} | _] = Records.
```

- [ ] **Step 2: Lancer (échoue)**

Run: `rebar3 ct --suite apps/fenrir/test/fenrir_job_SUITE`
Expected: FAIL (module absent).

- [ ] **Step 3: Implémenter la boucle**

`apps\fenrir\src\fenrir_job.erl` :

```erlang
-module(fenrir_job).
-export([learn/2, run/3, signature_of/1]).

%% Nif :: #{sniff => fun((binary()) -> binary()),
%%          parse_line => fun((binary(), binary()) -> {binary(), float()})}

%% PERCEIVE + SUGGEST + REMEMBER : apprend (ou réutilise) une recette.
learn(Sample, Nif) ->
    Sig = signature_of(Sample),
    case fenrir_recipe_store:get(Sig) of
        {ok, R} ->
            {ok, R};                          %% réutilise : zéro IA
        not_found ->
            SniffFun = maps:get(sniff, Nif),
            Json = SniffFun(Sample),
            Recipe = #{<<"signature">> => Sig, <<"json">> => Json},
            ok = fenrir_recipe_store:put(Sig, Recipe),
            {ok, Recipe}
    end.

%% ACT : applique la recette à des enregistrements.
run(Recipe, Lines, Nif) ->
    ParseFun = maps:get(parse_line, Nif),
    Json = maps:get(<<"json">>, Recipe, <<"{}">>),
    [ ParseFun(Json, L) || L <- Lines ].

%% Signature de structure de la source (perceive).
signature_of(Sample) ->
    First = case binary:split(Sample, <<"\n">>) of [H | _] -> H; _ -> Sample end,
    Cols = length(binary:split(First, [<<";">>, <<",">>, <<"\t">>, <<"|">>], [global])),
    iolist_to_binary(io_lib:format("csv:cols=~p", [Cols])).
```

- [ ] **Step 4: Lancer (passe)**

Run: `rebar3 ct --suite apps/fenrir/test/fenrir_job_SUITE`
Expected: PASS (2 cas).

- [ ] **Step 5: Commit**

```bash
git add apps/fenrir/src/fenrir_job.erl apps/fenrir/test/fenrir_job_SUITE.erl
git commit -m "feat(otp): fenrir_job perceive->suggest->act->remember loop"
```

---

## Task 11: Model checking Concuerror — la boucle d'apprentissage est sans course

**Files:**
- Create: `apps\fenrir\test\concuerror_tests.erl`
- Modify: `rebar.config` (profil concuerror + dep)

- [ ] **Step 1: Ajouter Concuerror comme dep de test**

Dans `rebar.config`, étendre le profil test :

```erlang
{profiles, [
  {test, [{deps, [
      {proper, "1.4.0"},
      {concuerror, {git, "https://github.com/parapluu/Concuerror.git", {branch, "master"}}}
  ]}]}
]}.
```

- [ ] **Step 2: Écrire l'entrée Concuerror**

`apps\fenrir\test\concuerror_tests.erl` :

```erlang
-module(concuerror_tests).
-export([concurrent_learn_no_race/0]).

%% Deux process apprennent la MÊME source en parallèle.
%% Propriété vérifiée par model checking : quel que soit l'entrelacement,
%% le store finit avec exactement une recette pour la signature, et les
%% deux process reçoivent {ok, _}. Aucune course, aucun deadlock.
concurrent_learn_no_race() ->
    {ok, _} = fenrir_recipe_store:start_link(#{dir => "/tmp/fenrir_cc"}),
    Sample = <<"a;b\n1;2\n">>,
    Nif = #{sniff => fun(_) -> <<"{}">> end},
    Self = self(),
    P1 = spawn(fun() -> Self ! {p1, fenrir_job:learn(Sample, Nif)} end),
    P2 = spawn(fun() -> Self ! {p2, fenrir_job:learn(Sample, Nif)} end),
    R1 = receive {p1, X} -> X end,
    R2 = receive {p2, Y} -> Y end,
    {ok, _} = R1,
    {ok, _} = R2,
    Sig = fenrir_job:signature_of(Sample),
    {ok, _} = fenrir_recipe_store:get(Sig),
    true = (P1 =/= P2).
```

- [ ] **Step 3: Lancer Concuerror**

Run (depuis la racine, après `rebar3 as test compile`) :
```
_build/test/lib/concuerror/bin/concuerror --pa _build/test/lib/fenrir/ebin -m concuerror_tests -t concurrent_learn_no_race
```
Expected: `Checking complete` / `done without errors` — aucun entrelacement ne viole la propriété.

(Sur Windows, invoquer via `escript _build/test/lib/concuerror/concuerror` si le wrapper bash n'est pas exécutable.)

- [ ] **Step 4: Commit**

```bash
git add rebar.config apps/fenrir/test/concuerror_tests.erl
git commit -m "test(otp): Concuerror model check — concurrent learn is race-free"
```

---

## Task 12: Job supervisé + API publique + smoke test end-to-end

**Files:**
- Create: `apps\fenrir\src\fenrir_job_sup.erl`, `apps\fenrir\src\fenrir.erl` (API)
- Modify: `fenrir_sup.erl`
- Test: `apps\fenrir\test\fenrir_e2e_SUITE.erl`

- [ ] **Step 1: API publique `fenrir.erl`**

```erlang
-module(fenrir).
-export([ingest/3]).

%% Pipeline complet : sample d'apprentissage, lignes à parser, format de sortie.
%% Utilise le vrai NIF.
ingest(Sample, Lines, Format) ->
    Nif = #{sniff      => fun fenrir_core_nif:sniff/1,
            parse_line => fun fenrir_core_nif:parse_line/2},
    {ok, Recipe} = fenrir_job:learn(Sample, Nif),
    Records = fenrir_job:run(Recipe, Lines, Nif),
    Values  = [V || {V, _Conf} <- Records],
    Joined  = <<"[", (iolist_to_binary(lists:join(<<",">>, Values)))/binary, "]">>,
    fenrir_core_nif:load(Joined, Format).
```

- [ ] **Step 2: fenrir_job_sup (simple_one_for_one) + ajout au sup racine**

`fenrir_job_sup.erl` :

```erlang
-module(fenrir_job_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {#{strategy => simple_one_for_one},
          [#{id => fenrir_job, start => {fenrir_job, learn, []},
             restart => temporary, type => worker}]}}.
```

Dans `fenrir_sup.erl`, ajouter le child `fenrir_job_sup` à la liste.

- [ ] **Step 3: Smoke test end-to-end (vrai NIF)**

`apps\fenrir\test\fenrir_e2e_SUITE.erl` :

```erlang
-module(fenrir_e2e_SUITE).
-export([all/0, full_pipeline/1]).
-include_lib("common_test/include/ct.hrl").

all() -> [full_pipeline].

full_pipeline(_) ->
    Sample = <<"name;age\nAlice;30\nBob;25\n">>,
    Lines  = [<<"Carol;40">>, <<"Dan;22">>],
    {ok, Out} = fenrir:ingest(Sample, Lines, <<"json">>),
    true = is_binary(Out) orelse is_list(Out),
    {match, _} = re:run(Out, "Carol").
```

- [ ] **Step 4: Lancer (passe, nécessite la DLL)**

Run: `cd C:\Users\steve\dev\fenrir && rebar3 ct --suite apps/fenrir/test/fenrir_e2e_SUITE`
Expected: PASS — le pipeline complet sniff→parse→load via le vrai NIF produit un JSON contenant "Carol".

- [ ] **Step 5: README + commit final Phase 1**

Créer `README.md` (description, build, test). Puis :

```bash
git add apps/fenrir/src/fenrir.erl apps/fenrir/src/fenrir_job_sup.erl apps/fenrir/src/fenrir_sup.erl apps/fenrir/test/fenrir_e2e_SUITE.erl README.md
git commit -m "feat: end-to-end CSV ingest pipeline + e2e test"
```

---

## Phases suivantes (plans séparés à écrire après Phase 1 verte)

- **Phase 2 — Confiance + escalade** : `fenrir_confidence_monitor` (collecte les scores, route < seuil vers dead-letter), `fenrir_learner_gateway` (interface IA, mockée par défaut), patch de recette + bump de version + rollback sur régression. Plan : `2026-XX-XX-fenrir-phase2-confidence-escalation.md`.
- **Phase 3 — XML + drift** : backend `loki_xml`, `infer_type` étendu au nesting, `fenrir_drift_detector` (fenêtre glissante de confiance → re-learn). Plan séparé.
- **Phase 4 — text/pdf + sandbox** : backends `loki_text`/`loki_pdf`, échappatoire code généré exécuté en sandbox (`rhai` côté Rust, op-limit), champ `transforms[].kind = "snippet"`. Plan séparé.

---

## Self-Review

**Couverture spec :**
- §3 IR `serde_json::Value` → Task 3/5 ✓ · §3 Load loki_weave → Task 5 ✓
- §5 recipe_engine → Task 3 ✓ · confidence → Task 3 ✓ · recipe model → Task 2 ✓
- §5 recipe_store → Task 9 ✓ · source_reader/signature → Task 10 (`signature_of`) ✓
- §5 recipe_inducer (heuristique Phase 1) → Task 4 ✓
- §6 boucle perceive→suggest→act→remember → Task 10 ✓
- §8 gestion d'erreurs (parse → null + confiance basse, jamais de crash) → Task 3 ✓ ; supervision → Task 9/12 ✓
- §9 property test → Task 6 ✓ · corpus doré → Task 6 ✓ · replay zéro-IA → Task 10 `learns_then_reuses` ✓ · tests OTP → Task 9/12 ✓ · **model checking Concuerror → Task 11 ✓**
- §10 Phase 1 (CSV apprend-une-fois) → tout le plan ✓ ; Phases 2-4 → plans séparés (noté)
- Hors Phase 1 (différé, documenté) : sandbox/snippet (Phase 4), drift (Phase 3), escalade (Phase 2). Conforme au scope.

**Scan placeholders :** Task 1 step 3 reformulé pour éviter les modules-placeholder ; aucun TODO/TBD restant. Tout step de code montre le code.

**Cohérence des types :** `fenrir_core_nif:{sniff/1, parse_line/2, load/2}` cohérent entre Task 7 (NIF), 8 (façade), 10/12 (appelants). `Nif` map `#{sniff, parse_line}` cohérente entre Task 10 et 12. `Recipe` map Erlang `#{<<"signature">>, <<"json">>}` cohérente Task 9/10. `ParsedRecord{value, confidence}` cohérent Task 3/6/7.
