# Fenrir 🐺

> *The wolf, son of Loki.* Part of the [`loki_*`](https://github.com/roquess) family.

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)
[![Erlang/OTP](https://img.shields.io/badge/erlang%2FOTP-27-red.svg)](https://www.erlang.org/)
[![Model checked](https://img.shields.io/badge/concurrency-Concuerror%20verified-green.svg)](https://concuerror.com/)

**Self-learning ETL/ELT parser.** Fenrir forges its own parsers and learns what
it devours: instead of hand-writing a connector for every source, it **learns
its parsing** from samples, produces a **deterministic, reusable artifact** (a
*recipe*), runs it at scale **without AI**, and only escalates to learning for
low-confidence cases or when a format drifts.

---

## Why

Every ETL pipeline requires a hand-written connector per source: guess the
delimiter, the encoding, the types, the schema mapping… and it breaks silently
as soon as the format drifts. It's repetitive, fragile, non-reusable.

Fenrir turns this work into **emergent state from usage**: it observes, infers a
recipe, persists it, and only re-learns when needed.

## Complete vision

Ultimately, Fenrir is a **self-healing ETL/ELT engine**. You point it at a source
— any, unknown, drifting over time — and it copes:

```
          ┌──────────────────────────────────────────────────────────┐
          │                      UNKNOWN SOURCE                        │
          │         CSV · XML · logs · text · PDF · …                  │
          └────────────────────────────┬─────────────────────────────┘
                                        ▼
   ┌───────────  PERCEIVE  ─────────────────────────────────────────────┐
   │  sample · compute a structural signature                           │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  SUGGEST  ──────────────────────────────────────────────┐
   │  recipe known ?                                                     │
   │    ├─ yes → reuse (ZERO learning)                                   │
   │    └─ no  → induce a recipe (single-flight: one leader only)        │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  ACT  ──────────────────────────────────────────────────┐
   │  apply the recipe at scale → serde_json::Value                      │
   │    ├─ high confidence → loki_weave (ZERO AI)                        │
   │    └─ low confidence  → dead-letter ──► ESCALATE                    │
   └────────────────────────────┬───────────────────────────────────────┘
                                 ▼
   ┌───────────  REMEMBER  ─────────────────────────────────────────────┐
   │  persist the versioned recipe · track confidence over time         │
   │    └─ format drift detected → re-trigger learning                   │
   │    └─ regression after a patch → roll back the version              │
   └─────────────────────────────────────────────────────────────────────┘
```

The properties that make the system valuable:

- **Self-learning** — no hand-written connectors; the recipe is inferred then
  refined.
- **Deterministic and auditable** — execution never uses AI; the recipe is a
  readable, editable, git-versioned JSON artifact.
- **Autonomously self-healing** — a `fenrir_healer` process reacts to drift
  (push notification + reconciliation tick), re-learns on its own, keeps the
  patch only if it improves, and quarantines (emitting `{needs_attention, Sig}`)
  when it can't — no thrashing, no human in the loop.
- **Economical** — single-flight + recipe cache: the expensive learning runs
  once per signature, even under concurrent load.
- **Safe by default** — complex transforms the declarative recipe cannot express
  run in a resource-limited sandbox.
- **Multi-format** — CSV (with dialect detection), XML (repeated element +
  fields), text/semi-structured (named-capture regex pattern, e.g. logs).
- **Parallel streaming** — `fenrir_stream` parses across a worker pool with
  demand-driven backpressure (bounded memory) and at-least-once fault tolerance;
  per-record confidence still feeds the healer. Single-node now, multi-node by
  design (pid-based demand protocol).

> **Where are we?** All four phases are delivered: the deterministic loop
> (perceive → suggest → act → remember, model-checked single-flight), escalation
> with anti-regression rollback, drift detection, the CSV / XML / text backends,
> and the rhai sandbox for generated code. See the [roadmap](#roadmap).

## Guiding principle

The AI (the learning) is a **separable service**, used only in *learning mode*.

| Mode | Depends on AI? | Properties |
|------|----------------|------------|
| **Learning** (online) | yes | N samples → produce/patch a recipe |
| **Execution** (offline) | **no** | reads only the artifact → fast, reproducible, auditable, deployable anywhere |

> In Phase 1, the inducer is a **100% deterministic heuristic sniffer**
> (separator / header / type detection) — zero external dependency.

## Architecture

```
Extract (raw bytes)
   │
   ▼
Transform = [learned recipe] ──▶ serde_json::Value      ← Fenrir core
   │
   ▼
Load = loki_weave ──▶ JSON / YAML / TOML / XML / TOON
```

Three layers:

```
┌──────────────────────────────────────────────────────────┐
│  ORCHESTRATION — Erlang/OTP (apps/fenrir)                  │
│  supervision · cognitive loop · single-flight             │
└───────────────┬──────────────────────────┬────────────────┘
                │ Rustler NIF              │ (network, separable)
┌───────────────▼─────────────┐  ┌─────────▼──────────────────┐
│  DETERMINISTIC CORE — Rust   │  │  AI SERVICE (learning mode) │
│  recipe · sniff · engine     │  │  inducer (heuristic in P1)  │
│  confidence · load(weave)    │  │                             │
└──────────────────────────────┘  └─────────────────────────────┘
     ZERO AI in execution mode
```

- **Orchestration** — Erlang/OTP (`apps/fenrir/`): supervision tree, job loop,
  single-flight coordination.
- **Deterministic core** — Rust (`native/fenrir_core/`) exposed via a Rustler
  NIF (`native/fenrir_nif/`): recipe model, heuristic sniffer, parse engine,
  confidence scoring, Load delegated to
  [`loki_weave`](https://github.com/roquess/loki_weave).

## The cognitive loop

An ingestion job follows `perceive → suggest → act → remember`:

| Primitive | Step | Detail |
|-----------|------|--------|
| **Perceive** | sample the source | compute a *structural signature* |
| **Suggest**  | recipe known?     | yes → reuse (**zero AI**); no → induce |
| **Act**      | apply the recipe  | → `serde_json::Value` → loki_weave |
| **Remember** | persist           | versioned recipe, key = signature |

Each seen source enriches the `recipe_store`: a source already learned **never**
calls learning again.

## The artifact: the recipe

Deterministic, readable, versioned, git-diffable:

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

Each parsed record carries a **confidence score** (ratio of correctly typed
fields). Fields that fail become `null` without crashing the pipeline — low
confidence is the escalation signal.

## Requirements

- [Rust](https://www.rust-lang.org/) 1.94+
- [Erlang/OTP](https://www.erlang.org/) 27
- [rebar3](https://rebar3.org/)
- [`loki_weave`](https://github.com/roquess/loki_weave) cloned as a sibling repo
  (`../loki_weave`)
- [Concuerror](https://concuerror.com/) (optional, for model checking)

## Build

```bash
rebar3 compile
```

The build chains automatically: compile the Rust NIF → copy the shared library
into `apps/fenrir/priv/` → compile the Erlang app.

## Usage

```erlang
%% Learn from a sample, parse lines, format as JSON.
Sample = <<"name;age\nAlice;30\nBob;25\n">>,
Lines  = [<<"Carol;40">>, <<"Dan;22">>],
{ok, Json} = fenrir:ingest(Sample, Lines, <<"json">>).
%% => [{"name":"Carol","age":40},{"name":"Dan","age":22}]
```

The full self-healing loop (learn → parse while observing confidence → escalate
on drop → keep the patch only if it improves → report):

```erlang
Report = fenrir:ingest_adaptive(Sample, Lines).
%% => #{escalated => true, improved => true, old_conf => 0.5, new_conf => 1.0, ...}
```

## Tests

```bash
# Rust core: model, engine, sniffer, Load, property tests, golden corpus
cd native/fenrir_core && cargo test

# OTP orchestration: recipe_store, job loop, single-flight, e2e via the real NIF
rebar3 ct

# Concurrency model checking (all interleavings)
scripts/model_check.sh
```

### Model checking

The single-flight coordination (which guarantees a concurrent cold start
triggers **only one** expensive learning) is formally verified by
[Concuerror](https://concuerror.com/): it explores **all** scheduling
interleavings and proves the absence of races and deadlocks.

```
Summary: 0 errors, 4/4 interleavings explored
```

## Structure

```
fenrir/
├── apps/fenrir/            # OTP application (orchestration)
│   ├── src/
│   │   ├── fenrir.erl                  # public API (ingest/3, ingest_adaptive/2)
│   │   ├── fenrir_job.erl              # perceive→suggest→act→remember + escalate
│   │   ├── fenrir_recipe_store.erl     # persistence (ETS + disk + versions + rollback)
│   │   ├── fenrir_confidence_monitor.erl
│   │   ├── fenrir_drift_detector.erl   # edge-triggered drift + enumeration
│   │   ├── fenrir_healer.erl           # autonomous self-healing loop
│   │   ├── fenrir_learner_gateway.erl  # injectable learning (heuristic / AI)
│   │   ├── fenrir_singleflight.erl     # concurrent cold-start dedup
│   │   ├── fenrir_stream.erl           # demand-driven parallel coordinator
│   │   ├── fenrir_stream_worker.erl    # streaming parse worker
│   │   ├── fenrir_core_nif.erl         # NIF facade
│   │   └── fenrir_{app,sup,job_sup}.erl
│   └── test/                           # Common Test + Concuerror entry point
├── native/
│   ├── fenrir_core/        # pure Rust core (testable in isolation)
│   │   └── src/            # recipe · engine · sniff · xml · text · sandbox · load
│   └── fenrir_nif/         # Rustler bindings
├── scripts/model_check.sh
└── docs/superpowers/       # spec + implementation plans
```

## Roadmap

| Phase | Content | Status |
|-------|---------|--------|
| **1** | Deterministic CSV pipeline, "learn-once then run" | ✅ **delivered** |
| **2** | Confidence + escalation: `confidence_monitor`, learning gateway, recipe patch + anti-regression rollback | ✅ **delivered** |
| **3** | XML backend + format drift detection (`drift_detector`) | ✅ **delivered** |
| **4** | Text/semi-structured backend (regex) + sandboxed generated-code escape hatch (rhai) | ✅ **delivered** |

Spec and detailed plans in [`docs/superpowers/`](docs/superpowers/).

## The loki family

Fenrir builds on and complements the ecosystem:

- [`loki_weave`](https://github.com/roquess/loki_weave) — normalize and format (the *Load*)
- [`loki_csv`](https://github.com/roquess/loki_csv) — CSV parsing (Erlang/OTP)
- [`loki_xml`](https://github.com/roquess/loki_xml) — XML parsing (Erlang/OTP)
- [`loki_text`](https://github.com/roquess/loki_text) — text manipulation (Rust)
- [`loki_pdf`](https://github.com/roquess/loki_pdf) — PDF (Rust/WASM)

## License

[MIT](LICENSE) © 2026 Roques Steve
