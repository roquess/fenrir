# Fenrir — Parser ETL/ELT auto-apprenant

> **Fenrir**, le loup fils de Loki. Membre de la famille `loki_*`.
> *"Forge ses propres parsers, apprend ce qu'il dévore."*

**Date** : 2026-05-24
**Statut** : Design validé — prêt pour plan d'implémentation

---

## 1. Problème

Tout pipeline ETL/ELT exige un connecteur écrit à la main par source : on devine
le délimiteur, l'encodage, les types, le mapping de schéma, et le pipeline casse
silencieusement quand le format dérive. C'est du travail répétitif, fragile, et
non réutilisable d'une source à l'autre.

**Fenrir** remplace l'écriture manuelle de connecteurs par un moteur qui **apprend
son propre parsing** à partir d'échantillons, produit un **artefact déterministe
réutilisable**, le fait tourner à grande échelle **sans IA**, et ne revient à l'IA
que pour les cas à faible confiance ou en cas de dérive de format.

## 2. Principe directeur

L'IA est un **service séparable**, utilisé uniquement en *mode apprentissage*.

- **Mode apprentissage** (en ligne, IA) : N échantillons → produit/patche une recette.
- **Mode exécution** (hors-ligne, zéro IA) : ne dépend que de l'artefact → rapide,
  reproductible, auditable, déployable partout.

## 3. Périmètre

**MVP** : les formats déjà couverts par la famille `loki_*` :
`loki_csv` (CSV/TSV), `loki_xml` (XML), puis `loki_text` / `loki_pdf`.

L'IR universel cible est **`serde_json::Value`** (natif `loki_weave`).
Le Load final est délégué à **`loki_weave`** (JSON/YAML/TOML/XML/TOON).

Hors périmètre MVP : semi-structuré libre (logs hétérogènes, emails), non-structuré
pur. Architecture pensée pour les brancher ensuite (plugins d'apprentissage).

## 4. Architecture — 3 couches

```
┌─────────────────────────────────────────────────────────┐
│  COUCHE ORCHESTRATION — Erlang/OTP (application `fenrir`) │
│  supervision · distribution · boucle auto-réparatrice     │
└───────────────┬─────────────────────────┬─────────────────┘
                │ Rustler NIF             │ (réseau, séparable)
┌───────────────▼──────────────┐  ┌───────▼──────────────────┐
│  CŒUR DÉTERMINISTE — Rust     │  │  SERVICE IA (mode appren.) │
│  recipe_engine · confidence   │  │  recipe_inducer            │
│  sandbox · loki_weave (Load)  │  │  escalation_handler        │
└──────────────────────────────┘  └────────────────────────────┘
        ZÉRO IA en mode exécution        appelé seulement à l'apprentissage
```

### Choix de pile
- **Orchestration** : Erlang/OTP. La supervision, la distribution et la boucle
  auto-réparatrice sont le terrain de jeu naturel de BEAM. Réutilise directement
  `loki_csv` / `loki_xml` (Erlang).
- **Cœur déterministe** : Rust, exposé via **Rustler NIF**. L'IR `serde_json::Value`
  et le Load `loki_weave` vivent déjà dans le monde Rust/serde.
- **Service IA** : séparable (frontière réseau). Jamais sollicité en mode exécution.

## 5. Composants

### Orchestration (Erlang/OTP)
| Module | Rôle | Type |
|---|---|---|
| `source_reader` | Extrait les octets en streaming (chunks), calcule la **signature** de la source | GenServer |
| `recipe_store` | Persiste les recettes apprises (ETS + disque), clé = signature | GenServer |
| `parse_worker` | Applique la recette via NIF Rust (pool) | pool de workers |
| `confidence_monitor` | Collecte la confiance par enregistrement, route les faibles | GenServer |
| `drift_detector` | Surveille les stats ; format qui dérive → relance l'apprentissage | GenServer |
| `learner_gateway` | Dialogue avec le service IA | GenServer |
| `fenrir_sup` | Arbre de supervision (un sous-superviseur par job d'ingestion) | Supervisor |

### Cœur Rust (NIF)
| Module | Rôle |
|---|---|
| `recipe` | Modèle de données de la recette (sérialisable) |
| `recipe_engine` | Interprète une recette → `serde_json::Value` (déterministe, rapide) |
| `confidence` | Score chaque parse (types corrects, champs manquants, fit regex) |
| `sandbox` | Exécute les snippets générés (rhai/wasm, limité en ressources) |
| `weave_load` | Intégration `loki_weave` : Load de l'IR vers le format cible |

### Service IA (séparable, mode apprentissage)
| Module | Rôle |
|---|---|
| `recipe_inducer` | N échantillons + schéma cible optionnel → propose une recette |
| `escalation_handler` | Enregistrements faible-confiance + recette → patch (raffine la recette OU émet un snippet) |

## 6. Flux de données = boucle cognitive (perceive → suggest → act → remember)

| Primitive | Étape | Détail |
|---|---|---|
| **Perceive** | `source_reader` échantillonne N enregistrements | calcule une signature de structure (fingerprint) |
| **Suggest** | lookup `recipe_store` par signature | Miss → l'IA *induit* une recette. Hit → réutilise (zéro IA) |
| **Act** | `parse_worker` applique la recette | → `serde_json::Value` → streamé vers `loki_weave` |
| **Remember** | persistance + versionnement | recette stockée sous sa signature ; confiance trackée ; dérive → réapprentissage |

**Apprentissage intrinsèque** : chaque source vue enrichit le `recipe_store`. Une
source déjà apprise ne sollicite plus jamais l'IA.

## 7. L'artefact — recette versionnée (git-friendly)

La recette déclarative couvre le cas commun. Une **échappatoire code généré**
(snippet sandboxé) couvre les transformations que le vocabulaire déclaratif ne peut
exprimer. La boucle de confiance décide quand escalader vers du code.

```json
{
  "signature": "csv:sep=;:cols=3:hdr",
  "version": 3,
  "backend": "loki_csv",
  "config": { "sep": ";", "encoding": "utf-8", "header": true, "quote": "\"" },
  "schema": [
    {"name": "name", "type": "string", "from": 0},
    {"name": "age",  "type": "int",    "from": 1},
    {"name": "city", "type": "string", "from": 2, "split": "|", "take": 0}
  ],
  "transforms": [
    {"field": "age", "kind": "snippet", "lang": "rhai", "ref": "tx_age_v3.rhai"}
  ],
  "confidence_rules": { "min_field_match": 0.95 }
}
```

Lisible, éditable à la main, diffable dans git. L'IA la produit/patche ; un humain
peut la corriger. Le snippet référencé est stocké à côté, versionné avec la recette.

## 8. Gestion d'erreurs

- Erreur de parse par enregistrement → **dead-letter stream** + signal au
  `confidence_monitor`. On ne crash pas sur de la donnée ; le "let it crash" OTP
  est réservé aux fautes d'infra.
- Crash d'un `parse_worker` → restart par le superviseur, enregistrement re-queué.
- Snippet généré qui s'emballe → timeout sandbox (op-limit rhai / fuel wasm) →
  dead-letter + escalade vers l'IA.
- IA indisponible → mode exécution intact ; mode apprentissage met en file et
  réessaie avec backoff.
- Régression de confiance après un patch → **rollback** vers la version précédente
  de la recette (les versions sont conservées dans le `recipe_store`).

## 9. Stratégie de test

- **Rust** : property tests sur `recipe_engine` (round-trip : données + recette →
  `Value` attendue) ; tests de calibration de `confidence`.
- **Corpus doré** : ensemble de fichiers réels crades + sortie normalisée attendue.
- **Test "replay"** : recette apprise → mode exécution → assertion **zéro appel IA**
  + sortie déterministe bit-à-bit.
- **Erlang** : tests OTP — cycle de vie d'un job, restart superviseur, déclenchement
  de la détection de dérive, routage dead-letter.

## 10. Découpage par phases

| Phase | Contenu | Sortie |
|---|---|---|
| **1** | CSV seul (`loki_csv`) : induction de recette + `recipe_engine` + `loki_weave` Load + `recipe_store`. Apprend-une-fois puis tourne. | Pipeline CSV→IR→format fonctionnel |
| **2** | `confidence` + boucle d'escalade (l'hybride auto-apprenant complet) | Boucle de feedback opérationnelle |
| **3** | XML (`loki_xml`) + `drift_detector` | Multi-format + auto-réparation |
| **4** | `loki_text` / `loki_pdf` + échappatoire code généré (sandbox) | Couverture loki_ complète |

## 11. Décisions actées

| Décision | Choix | Raison |
|---|---|---|
| Rôle de l'IA | Hybride auto-apprenant | artefact déterministe + escalade faible-confiance + détection de dérive |
| Périmètre sources | Formats `loki_*` (CSV, XML, text, PDF) | réutilise des parsers éprouvés ; borne le problème |
| Forme de l'artefact | Recette déclarative + échappatoire code généré | auditable par défaut, expressif au besoin |
| IR cible | `serde_json::Value` | natif `loki_weave`, sérialisable |
| Pile | Orchestration BEAM/OTP + cœur Rust (Rustler NIF) | colle à l'écosystème, ETL tolérant aux pannes, boucle cognitive native OTP |
| Dépendance IA | Service séparable, mode apprentissage uniquement | mode exécution reproductible et déployable sans IA |
