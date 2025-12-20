# Design Decisions (Precommitments)

## Data Scope
- Unit: player-season
- Years: 2021–2025
- Modalities: scouting text (FV language pruned), tools, minor-league stats + levels, bio/traits

## Target
Predict FV(N+1) from all information up to year N.
FV values allowed: 20, 30, 35, 40, 42.5, 45, 47.5, 50, 55, 60, 65, 70.

## Evaluation Protocol
Protocol C:
- Primary: forward-chaining by year
- Secondary robustness: player holdout evaluation

## Similarity / Neighborhood Definition (for evaluation diagnostics)
Option 1:
- Hard strata: role, level, age bin
- Soft distance: standardized numeric feature vectors
- Sparsity fallback rule: (to be specified explicitly before metrics are computed)

## Admissible “Better Valuation” Criteria
- Lower conditional variance (given similarity neighborhoods)
- Better calibration (transition probabilities / uncertainty)
- Stronger neighborhood coherence
