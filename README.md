# Fenrir 🐺

> *Le loup, fils de Loki.* Membre de la famille `loki_*`.
> **Parser ETL/ELT auto-apprenant** : forge ses propres parsers, apprend ce qu'il dévore.

Fenrir remplace l'écriture manuelle de connecteurs ETL par un moteur qui **apprend
son parsing** à partir d'échantillons, produit un **artefact déterministe réutilisable**
(une *recette*), le fait tourner à grande échelle **sans IA**, et n'escalade que les
cas à faible confiance ou en cas de dérive de format.

## Architecture

```
Extract (octets bruts)
   ↓
Transform = [recette apprise] → serde_json::Value   ← cœur Fenrir
   ↓
Load = loki_weave → JSON/YAML/TOML/XML/TOON
```

- **Orchestration** : Erlang/OTP (`apps/fenrir`) — supervision, boucle cognitive,
  single-flight.
- **Cœur déterministe** : Rust (`native/fenrir_core`) exposé via Rustler NIF
  (`native/fenrir_nif`) — modèle de recette, sniffer heuristique, engine de parsing,
  scoring de confiance, Load via `loki_weave`.
- **IA** : service séparable, mode apprentissage uniquement. En Phase 1 l'inducer est
  un **sniffer heuristique 100% déterministe** (zéro dépendance externe).

La boucle d'un job suit `perceive → suggest → act → remember` :

| Primitive | Étape |
|-----------|-------|
| Perceive  | échantillonne la source → signature de structure |
| Suggest   | recette connue ? réutilise (zéro IA) : sinon induit |
| Act       | applique la recette → `serde_json::Value` → loki_weave |
| Remember  | persiste la recette versionnée sous sa signature |

## Build

Prérequis : Rust 1.94+, Erlang/OTP 27, rebar3.

```bash
rebar3 compile     # compile le NIF Rust + l'app Erlang, copie la DLL dans priv/
```

## Tests

```bash
# Cœur Rust (modèle, engine, sniffer, Load, proptest, corpus doré)
cd native/fenrir_core && cargo test

# Orchestration OTP (recipe_store, job loop, single-flight, e2e via NIF réel)
rebar3 ct

# Model checking de la concurrence (single-flight, tous entrelacements)
scripts/model_check.sh
```

## Statut

**Phase 1 livrée** : pipeline CSV déterministe « apprend-une-fois puis tourne ».

Phases suivantes (plans dans `docs/superpowers/plans/`) :
- Phase 2 — confiance + escalade (boucle hybride complète, gateway IA)
- Phase 3 — XML (`loki_xml`) + détection de dérive
- Phase 4 — text/pdf + échappatoire code généré sandboxé
