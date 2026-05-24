# Fenrir 🐺

> *Le loup, fils de Loki.* Membre de la famille [`loki_*`](https://github.com/roquess).

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)
[![Erlang/OTP](https://img.shields.io/badge/erlang%2FOTP-27-red.svg)](https://www.erlang.org/)
[![Model checked](https://img.shields.io/badge/concurrency-Concuerror%20verified-green.svg)](https://concuerror.com/)

**Parser ETL/ELT auto-apprenant.** Fenrir forge ses propres parsers et apprend ce
qu'il dévore : au lieu d'écrire un connecteur à la main pour chaque source, il
**apprend son parsing** à partir d'échantillons, en produit un **artefact
déterministe réutilisable** (une *recette*), le fait tourner à grande échelle
**sans IA**, et n'escalade vers l'apprentissage que pour les cas à faible
confiance ou lors d'une dérive de format.

---

## Pourquoi

Tout pipeline ETL exige un connecteur écrit à la main par source : deviner le
délimiteur, l'encodage, les types, le mapping de schéma… et ça casse
silencieusement dès que le format dérive. C'est répétitif, fragile, non
réutilisable.

Fenrir transforme ce travail en un **état émergent de l'usage** : il observe,
infère une recette, la persiste, et ne réapprend que si nécessaire.

## Vision complète

À terme, Fenrir est un **moteur ETL/ELT qui se répare tout seul**. On lui pointe
une source — quelconque, inconnue, qui dérive avec le temps — et il s'en débrouille :

```
          ┌──────────────────────────────────────────────────────────┐
          │                      SOURCE INCONNUE                       │
          │         CSV · XML · logs · texte · PDF · …                 │
          └────────────────────────────┬─────────────────────────────┘
                                        ▼
   ┌───────────  PERCEIVE  ─────────────────────────────────────────────┐
   │  échantillonne · calcule une signature de structure                │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  SUGGEST  ──────────────────────────────────────────────┐
   │  recette connue ?                                                   │
   │    ├─ oui → réutilise (ZÉRO apprentissage)                          │
   │    └─ non → induit une recette (single-flight : 1 seul leader)      │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  ACT  ──────────────────────────────────────────────────┐
   │  applique la recette à grande échelle → serde_json::Value           │
   │    ├─ confiance haute  → loki_weave (ZÉRO IA)                        │
   │    └─ confiance basse  → dead-letter ──► ESCALADE                    │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  REMEMBER  ─────────────────────────────────────────────┐
   │  persiste la recette versionnée · suit la confiance dans le temps   │
   │    └─ dérive de format détectée → relance l'apprentissage           │
   │    └─ régression après patch → rollback de version                  │
   └─────────────────────────────────────────────────────────────────────┘
```

Les propriétés qui font la valeur du système :

- **Auto-apprentissage** — aucune écriture de connecteur à la main ; la recette
  est inférée puis raffinée.
- **Déterministe et auditable** — l'exécution n'utilise jamais l'IA ; la recette
  est un artefact JSON lisible, éditable, versionné dans git.
- **Auto-réparation** — quand le format dérive, la confiance chute, Fenrir
  ré-apprend ; un patch qui régresse est annulé (rollback).
- **Économe** — single-flight + cache de recettes : l'apprentissage coûteux ne
  tourne qu'une fois par signature, même sous charge concurrente.
- **Sûr par défaut** — les transformations complexes que la recette déclarative
  ne peut exprimer s'exécutent dans un bac à sable [rhai](https://rhai.rs/) à
  ressources limitées (nombre d'opérations plafonné).
- **Multi-format** — CSV (avec détection de dialecte), XML (élément répété +
  champs), texte/semi-structuré (patron regex à groupes nommés, ex. logs).

> **Où en est-on ?** Les quatre phases sont livrées : la boucle déterministe
> (perceive → suggest → act → remember, single-flight model-checké), l'escalade
> avec rollback anti-régression, la détection de dérive, les backends CSV / XML /
> texte, et le bac à sable rhai pour le code généré. Voir la
> [feuille de route](#feuille-de-route).

## Principe directeur

L'IA (l'apprentissage) est un **service séparable**, utilisé uniquement en
*mode apprentissage*.

| Mode | Dépend de l'IA ? | Propriétés |
|------|------------------|------------|
| **Apprentissage** (en ligne) | oui | N échantillons → produit/patche une recette |
| **Exécution** (hors-ligne) | **non** | ne lit que l'artefact → rapide, reproductible, auditable, déployable partout |

> En Phase 1, l'inducer est un **sniffer heuristique 100 % déterministe**
> (détection séparateur / en-tête / types) — zéro dépendance externe.

## Architecture

```
Extract (octets bruts)
   │
   ▼
Transform = [recette apprise] ──▶ serde_json::Value      ← cœur Fenrir
   │
   ▼
Load = loki_weave ──▶ JSON / YAML / TOML / XML / TOON
```

Trois couches :

```
┌──────────────────────────────────────────────────────────┐
│  ORCHESTRATION — Erlang/OTP (apps/fenrir)                  │
│  supervision · boucle cognitive · single-flight           │
└───────────────┬──────────────────────────┬────────────────┘
                │ Rustler NIF              │ (réseau, séparable)
┌───────────────▼─────────────┐  ┌─────────▼──────────────────┐
│  CŒUR DÉTERMINISTE — Rust    │  │  SERVICE IA (apprentissage) │
│  recipe · sniff · engine     │  │  inducer (heuristique en P1)│
│  confidence · load(weave)    │  │                             │
└──────────────────────────────┘  └─────────────────────────────┘
     ZÉRO IA en mode exécution
```

- **Orchestration** — Erlang/OTP (`apps/fenrir/`) : arbre de supervision, boucle
  d'un job, coordination single-flight.
- **Cœur déterministe** — Rust (`native/fenrir_core/`) exposé via Rustler NIF
  (`native/fenrir_nif/`) : modèle de recette, sniffer heuristique, engine de
  parsing, scoring de confiance, Load délégué à
  [`loki_weave`](https://github.com/roquess/loki_weave).

## La boucle cognitive

Un job d'ingestion suit `perceive → suggest → act → remember` :

| Primitive | Étape | Détail |
|-----------|-------|--------|
| **Perceive** | échantillonne la source | calcule une *signature* de structure |
| **Suggest**  | recette connue ?         | oui → réutilise (**zéro IA**) ; non → induit |
| **Act**      | applique la recette      | → `serde_json::Value` → loki_weave |
| **Remember** | persiste                 | recette versionnée, clé = signature |

Chaque source vue enrichit le `recipe_store` : une source déjà apprise ne
sollicite **plus jamais** l'apprentissage.

## L'artefact : la recette

Déterministe, lisible, versionnée, diffable dans git :

```json
{
  "signature": "csv:sep=;:cols=3:hdr",
  "version": 1,
  "backend": "csv",
  "config": { "sep": ";", "encoding": "utf-8", "header": true, "quote": "\"" },
  "schema": [
    { "name": "name", "type": "string", "from": 0 },
    { "name": "age",  "type": "int",    "from": 1 },
    { "name": "city", "type": "string", "from": 2, "split": "|", "take": 0 }
  ],
  "confidence_rules": { "min_field_match": 0.95 }
}
```

Chaque enregistrement parsé porte un **score de confiance** (ratio de champs
correctement typés). Les champs qui échouent deviennent `null` sans faire planter
le pipeline — la confiance basse est le signal d'escalade (Phase 2).

## Prérequis

- [Rust](https://www.rust-lang.org/) 1.94+
- [Erlang/OTP](https://www.erlang.org/) 27
- [rebar3](https://rebar3.org/)
- [`loki_weave`](https://github.com/roquess/loki_weave) cloné en dépôt frère
  (`../loki_weave`)
- [Concuerror](https://concuerror.com/) (optionnel, pour le model checking)

## Build

```bash
rebar3 compile
```

Le build enchaîne automatiquement : compilation du NIF Rust → copie de la
bibliothèque dans `apps/fenrir/priv/` → compilation de l'app Erlang.

## Utilisation

```erlang
%% Apprend depuis un échantillon, parse des lignes, formate en JSON.
Sample = <<"name;age\nAlice;30\nBob;25\n">>,
Lines  = [<<"Carol;40">>, <<"Dan;22">>],
{ok, Json} = fenrir:ingest(Sample, Lines, <<"json">>).
%% => [{"name":"Carol","age":40},{"name":"Dan","age":22}]
```

## Tests

```bash
# Cœur Rust : modèle, engine, sniffer, Load, property tests, corpus doré
cd native/fenrir_core && cargo test

# Orchestration OTP : recipe_store, boucle job, single-flight, e2e via NIF réel
rebar3 ct

# Model checking de la concurrence (tous les entrelacements)
scripts/model_check.sh
```

### Model checking

La coordination single-flight (qui garantit qu'un démarrage à froid concurrent
ne déclenche **qu'un seul** apprentissage coûteux) est vérifiée formellement par
[Concuerror](https://concuerror.com/) : il explore **tous** les entrelacements
d'ordonnancement et prouve l'absence de course et de deadlock.

```
Summary: 0 errors, 4/4 interleavings explored
```

## Structure

```
fenrir/
├── apps/fenrir/            # Application OTP (orchestration)
│   ├── src/
│   │   ├── fenrir.erl              # API publique (ingest/3)
│   │   ├── fenrir_job.erl          # boucle perceive→suggest→act→remember
│   │   ├── fenrir_recipe_store.erl # persistance (ETS + disque + versions)
│   │   ├── fenrir_singleflight.erl # dédup du cold-start concurrent
│   │   ├── fenrir_core_nif.erl     # façade NIF
│   │   └── fenrir_{app,sup,job_sup}.erl
│   └── test/                       # Common Test + entrée Concuerror
├── native/
│   ├── fenrir_core/        # cœur Rust pur (testable en isolation)
│   └── fenrir_nif/         # bindings Rustler
├── scripts/model_check.sh
└── docs/superpowers/       # spec + plans d'implémentation
```

## Feuille de route

| Phase | Contenu | Statut |
|-------|---------|--------|
| **1** | Pipeline CSV déterministe « apprend-une-fois puis tourne » | ✅ **livré** |
| **2** | Confiance + escalade : `confidence_monitor`, gateway d'apprentissage, patch de recette + rollback anti-régression | ✅ **livré** |
| **3** | Backend XML + détection de dérive de format (`drift_detector`) | ✅ **livré** |
| **4** | Backend texte/semi-structuré (regex) + échappatoire code généré sandboxé (rhai) | ✅ **livré** |

Spec et plans détaillés dans [`docs/superpowers/`](docs/superpowers/).

## Famille loki

Fenrir s'appuie sur et complète l'écosystème :

- [`loki_weave`](https://github.com/roquess/loki_weave) — normalise et formate (le *Load*)
- [`loki_csv`](https://github.com/roquess/loki_csv) — parsing CSV (Erlang/OTP)
- [`loki_xml`](https://github.com/roquess/loki_xml) — parsing XML (Erlang/OTP)
- [`loki_text`](https://github.com/roquess/loki_text) — manipulation de texte (Rust)
- [`loki_pdf`](https://github.com/roquess/loki_pdf) — PDF (Rust/WASM)

## Licence

[MIT](LICENSE) © 2026 Roques Steve
