# Research Log

This document records ongoing reasoning, alternatives considered, and design evolution
for the prospect valuation modeling project.

Nothing in this file should be treated as a binding decision unless it is also
documented in DESIGN_DECISIONS.md.

Entries are chronological in intent. At this stage, multiple entries are recorded
on the same date to capture reasoning established during initial project specification.
Future entries will append chronologically.

---

## Topic Index

- Project Framing
  - Asset valuation vs outcome prediction

- Evaluation Philosophy
  - Information parity assumption
  - Admissible improvement criteria
  - Smoothness explicitly rejected

- Similarity / Neighborhood Definition
  - Need for explicit similarity definition
  - Hard vs soft treatment of level
  - Level treated as hard stratum pre-normalization

- Temporal Modeling
  - Sequence framing
  - FV(N+1) as proxy target
  - FV(N) as input (open)

- Evaluation Protocol
  - Forward-chaining + player holdout

---

## 2025-12-20 — Project Framing and Scope

### Topics
- Project Framing
- Evaluation Philosophy

### Notes
The project treats Future Value (FV) as an asset-valuation proxy reflecting how the
baseball industry currently values a prospect, rather than as ground-truth future
performance.

The primary objective is not to predict MLB outcomes, but to evaluate whether explicit,
consistent multimodal aggregation of publicly available information can improve
valuation properties relative to implicit human aggregation.

Improvement is evaluated structurally (e.g., variance, calibration, coherence), not
as discovery of hidden information or superior foresight.

### Status
Context-setting. No binding decisions.

---

## 2025-12-20 — Information Parity and Admissible Improvements

### Topics
- Evaluation Philosophy

### Notes
All model inputs (scouting text, tools, stats, bio) are publicly available and plausibly
used by human evaluators, though often inconsistently or informally.

The project assumes information parity between model and human valuation. Any observed
improvement must therefore arise from aggregation discipline (e.g., consistent
weighting, interaction capture, variance reduction), not from privileged information.

Admissible forms of improvement were restricted to:
- lower variance conditional on similar observable inputs,
- better calibration of FV transitions,
- stronger coherence among similar player profiles.

### Status
Decision stabilized; reflected in DESIGN_DECISIONS.md.

---

## 2025-12-20 — Smoothness Rejected as an Intrinsic Objective

### Topics
- Evaluation Philosophy

### Notes
Smooth temporal valuation trajectories were explicitly considered and rejected as an
intrinsic objective.

Prospect valuations can change discontinuously due to injuries, role changes, or
non-linear skill acquisition. Penalizing jaggedness risks suppressing real signal.

This does not imply that volatility is desirable; rather, smoothness is not treated as
a primary optimization target.

### Status
Decision-binding; reflected in DESIGN_DECISIONS.md.

---

## 2025-12-20 — Need for Explicit Similarity Definition

### Topics
- Similarity / Neighborhood Definition

### Notes
Evaluation of conditional variance and neighborhood coherence requires an explicit
definition of player similarity.

Similarity neighborhoods are used strictly for evaluation diagnostics, not for model
training.

A two-stage approach was identified:
- hard strata to enforce contextual comparability,
- soft distance within strata to rank neighbors.

### Status
Exploratory at this stage.

---

## 2025-12-20 — Temporal Modeling Framing

### Topics
- Temporal Modeling

### Notes
The modeling task is framed as predicting FV(N+1) using all information available up to
year N.

Sequence modeling is motivated by:
- multi-year development trajectories,
- temporal aggregation of evidence,
- valuation transition dynamics rather than static assessment.

Open questions remain regarding:
- inclusion of FV(N) as an input feature,
- predicting absolute FV(N+1) versus ΔFV.

### Status
Sequence framing adopted; specific modeling choices remain open.

---

## 2025-12-20 — Hard vs Soft Treatment of Level

### Topics
- Similarity / Neighborhood Definition

### Notes
Level was identified as a major contextual variable affecting the meaning of raw
performance statistics.

Concerns with hard stratification by level include:
- promotion-policy noise,
- potential reduction in neighborhood size,
- exclusion of otherwise similar players at adjacent levels.

An alternative—treating level as a soft feature after stat normalization—was noted
but deferred due to normalization cost.

### Status
Exploratory reasoning; no commitment at this point.

---

## 2025-12-20 — Evaluation Protocol Selection

### Topics
- Evaluation Protocol

### Notes
Protocol C was selected as the evaluation strategy:
- primary evaluation via forward-chaining by year,
- secondary robustness checks via player holdout.

This protocol aligns with the intended forecasting use case while also testing
representation generalization.

### Status
Decision-binding; reflected in DESIGN_DECISIONS.md.

---

## 2025-12-20 — Level Treated as Hard Stratum (Pre-Normalization)

### Topics
- Similarity / Neighborhood Definition
- Data Normalization

### Context
Full stat normalization by level-year is possible but potentially time-consuming.
Raw age is treated as an important indicator of developmental runway.

### Notes
Given unnormalized stats, treating level as a hard stratum was selected as the most
conservative approach for evaluation neighborhoods. This avoids misleading cross-level
comparisons where similar raw stats have different meanings.

The decision is explicitly provisional. If/when stats are normalized, level may be
revisited as a soft feature.

A sparsity fallback rule was adopted to mitigate small neighborhood sizes.

### Relation to Prior Entries
This resolves the earlier hard vs soft level discussion in favor of hard level strata
for the current phase.

### Status
Decision-binding; reflected in DESIGN_DECISIONS.md.

---

## Known Open Questions (Living)

- Whether FV(N) should be included as an input feature
- How to handle terminal seasons with no FV(N+1)
- Sample sizes per (role, level, age) stratum
- When stat normalization is sufficient to relax hard level constraints

These will be addressed prior to model implementation.

---
