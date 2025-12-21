# Data Schema

This document defines the canonical tables and rules used to construct training examples for the prospect valuation modeling project.

**Key principle:** A “report year” `N` represents an **off-season snapshot**. For report year `N`, **all performance statistics are drawn only from seasons ≤ `N-1`**, and the player’s `assigned_level` is defined **as-of the moment season `N-1` ended**.

Nothing in this document should be interpreted as a claim of modeling performance or results.

---

## Glossary

- **Report year (`report_year`)**: The year of the scouting/valuation snapshot (valid: 2021–2025).
- **As-of cutoff**: The end of season `report_year - 1`.
- **Assigned level (`assigned_level`)**: The level the player was most recently assigned to **as of the cutoff**, even if they did not record stats at that level.
- **Played level (`level_played`)**: The minor league / MLB level at which the statistical line was accrued.
- **MLBAM ID (`mlbam_id`)**: The canonical, stable player identifier used across all modeling tables.
- **FGID (`fgid`)**: FanGraphs player identifier; treated as a source-specific alias, not a stable key.

---

## Allowed Categorical Values

### Role
- `hitter`
- `pitcher`
- `two_way`

### Position (detail)
- Field: `C`, `1B`, `2B`, `3B`, `SS`, `LF`, `CF`, `RF`
- Pitcher usage: `SP`, `MIRP`, `SIRP`

### Levels
- `DSL`, `CPX`, `A`, `A+`, `AA`, `AAA`, `MLB`

### Bats
- `L`, `R`, `S`

### Throws
- `L`, `R`

### Signing vector
- `draft`
- `IAFA` (international amateur free agent)

### FV values
Allowed FV values are discrete:
- `20`, `30`, `35`, `40`, `42.5`, `45`, `47.5`, `50`, `55`, `60`, `65`, `70`

---

## Table 1: `player_season`

**Grain:** one row per `(mlbam_id, report_year)`.

This table stores the scouting snapshot and player attributes for the given report year. It also stores the FV for that year (used for diagnostics and optional modeling variants), but the primary supervised task uses `fv_next` as the label constructed from the next year’s FV.

### Primary key
- `mlbam_id` (integer or string; treated as stable identifier)
- `report_year` (integer; one of 2021–2025)

### Columns

#### Identifiers (non-features but required)
- `mlbam_id` (PK)
- `player_name` (string; for legibility/debugging)
- `report_year` (PK)

#### Snapshot categorical attributes
- `role` (enum: hitter/pitcher/two_way)
- `position` (enum listed above)
- `assigned_level` (enum listed above)

#### Snapshot valuation (feature-optional; not the primary label)
- `fv` (enum numeric values listed above; FV for report year `N`)

#### Biographical / physical
- `dob` (date; YYYY-MM-DD)
- `height_in` (integer; inches)
- `weight_lb` (integer; pounds)
- `bats` (enum: L/R/S)
- `throws` (enum: L/R)

#### Acquisition / signing
- `signing_vector` (enum: draft/IAFA)
- `signing_bonus_usd` (integer; dollars; allow null if unknown)

#### Scouting traits (ordinal / categorical)
- `frame` (integer: 2, 1, 0, -1, -2)
- `athleticism` (integer: 2, 1, 0, -1, -2)
- `levers` (enum: long/med/short)

#### Pitcher-only trait
- `delivery` (integer: 2, 1, 0, -1, -2; **nullable** for non-pitchers)

#### Scouting report text
- `scouting_report_text` (string; FV language pruned)
- Optional diagnostics (not required):
  - `scouting_report_word_count` (integer)
  - `scouting_report_char_count` (integer)

### Notes / rules
- `assigned_level` is defined **as-of end of season `report_year - 1`** (the off-season snapshot cutoff), even if the player did not accrue stats at that level.
- Newly drafted / newly signed players may have **no prior-season stats** available for early report years. This is allowed; the stats history may be empty.
- `player_name` is stored for readability and is not intended as a modeling feature.

---

## Table 2: `player_season_stats`

**Grain:** one row per player per season per level played per org.

A player may have multiple rows per season if they played at multiple levels. Stats are **split**, not combined.

### Primary key / Required columns
- `mlbam_id` (FK to `player_season.mlbam_id`)
- `season_year` (integer; 2020–2024 are relevant for report years 2021–2025)
- `level_played` (enum listed above)
- `org_id` (integer)

### Suggested metadata columns (optional but useful)
- `league` (string; nullable)

### Stat line columns
This project is agnostic to the exact stat set; however, each stat must be defined consistently and documented.

Recommended practice:
- Maintain separate stat tables for hitters and pitchers, or
- Use a unified table with role-appropriate nulls.

**Hitters (examples):**
- `g`, `pa`, `ab`, `h`, `2b`, `3b`, `hr`, `bb`, `so`, `sb`, `cs`

**Pitchers (examples):**
- `g`, `gs`, `ip`, `bf`, `h_allowed`, `hr_allowed`, `bb`, `so`

---

## Identity Resolution and Canonical Player IDs

### Canonical Identifier

All canonical modeling tables in this project are keyed by **MLBAM ID** (`mlbam_id`), which is treated as the stable, cross-source player identifier.

FanGraphs player IDs (`fgid`) and player names are treated as **source-specific identifiers** and are never used as primary keys in modeling tables.

---

### Stage A: MLBAM People Spine Construction

A standalone **people spine** is constructed using the MLB Stats API, enumerating players who appear on minor-league rosters (DSL, CPX, A, A+, AA, AAA) during seasons 2021–2025.

This table provides:
- `mlbam_id`
- full name and name components
- date of birth (Y/M/D)
- bats / throws
- height / weight
- primary position
- seasons observed (for provenance)

**Output:**
data/processed/mlbam_people_spine_2021_2025.csv

This spine is treated as an external reference table and is not recomputed implicitly by downstream steps.

---

### Stage B: FGID → MLBAM Identity Mapping

FanGraphs ingestion and parsing stages identify players via:
- `fgid` (when present), and
- player name text.

To bridge FanGraphs-derived data with MLBAM-keyed modeling tables, the project includes an explicit **identity mapping step**.

#### Mapping Inputs
- FanGraphs-derived identities (`fgid`, `player_name`)
- MLBAM people spine (name + DOB)

#### Matching Strategy (Deterministic, Ordered)
Matching is attempted in strict order and stops at the first unique match:

1. Exact normalized first name + last name + full DOB  
2. Exact normalized last name + full DOB  
3. Exact normalized full name (only if a single MLBAM candidate exists)

If multiple MLBAM candidates satisfy a rule, the match is marked ambiguous and rejected.

#### Mapping Outcomes
Each FG identity is assigned one of:
- `matched_exact_name_dob`
- `matched_lastname_dob`
- `matched_name_only_unique`
- `ambiguous_multiple_candidates`
- `unmatched_no_candidate`

#### Output
data/intermediate/identity_map_fgid_to_mlbam.csv


This table is an explicit dependency for all downstream joins.

---

### Canonical Usage Rules

- All modeling tables (`player_season`, `player_season_stats`, etc.) require a resolved `mlbam_id`.
- `fgid` is retained only for traceability and debugging.
- Rows without a resolved `mlbam_id` are excluded from supervised modeling joins but may be retained for diagnostics or future resolution.

Identity resolution outputs are **versioned data artifacts**, not ephemeral logic.

---

## Training Example Construction

### Primary supervised task
Predict `FV(N+1)` using all information available up to report year `N`.

- **Features at report year `N`:**
  - All snapshot fields from `player_season` for `(mlbam_id, report_year=N)`
  - Scouting text from `player_season.scouting_report_text` at year `N`
  - **All stats up through seasons ≤ `N-1`**, drawn from `player_season_stats`

- **Label:**
  - `fv_next = FV at report year (N+1)`, joined from `player_season` where `(mlbam_id, report_year=N+1)`

### Eligibility rules
A training example exists for `(mlbam_id, N)` if:
1. `player_season` row exists for `(mlbam_id, N)` (features)
2. `player_season` row exists for `(mlbam_id, N+1)` with non-null `fv` (label)

If (2) is missing, the example is **not eligible** for the supervised FV(N+1) task.

---

## As-of / Leakage Rules

For report year `N`:
- Included stats must satisfy `season_year ≤ N-1`.
- `assigned_level` reflects the most recent assignment **as-of the end of season N-1**.

---

## Evaluation Similarity Neighborhoods (Diagnostics)

Similarity neighborhoods are used only for evaluation diagnostics (conditional variance, neighborhood coherence).

Initial definition:
- Hard strata: `role`, `assigned_level`, `age_bin` (raw age binned at report year `N`)
- Soft distance: numeric feature vectors (defined during feature engineering)
- Explicit sparsity fallback rule (documented prior to metric computation)

Revisions must be recorded in `RESEARCH_LOG.md` and, if binding, in `DESIGN_DECISIONS.md`.

---

## Evaluation Protocol (Operational)

Protocol C is used:
- Primary: forward-chaining by year
- Secondary: player-holdout robustness checks

Exact splits and sampling rules are defined in evaluation code and experiment configs.

---
