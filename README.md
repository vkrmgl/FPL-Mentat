# FPL-Mentat

Predicting Fantasy Premier League returns by simulating matches rather than
regressing on points. Named for the *Dune* Mentat - a human trained to compute.

## What it does

For each upcoming fixture the pipeline estimates whether a player features and
for how long, samples a correlated scoreline for the match, allocates the goals
and assists among the players who appeared, resolves bonus by ranking BPS within
the fixture, and applies that season's scoring rules. A thousand draws give a
distribution per player, not a point estimate - which is what captaincy actually
needs, since it is a right-tail decision.

A mixed-integer program then turns those projections into a squad: fifteen
players under budget, a legal starting eleven, a captain, and a five-gameweek
transfer plan that accounts for banked free transfers and four-point hits.

## Why not just regress on points

Measured on this repo's own data, over 9,955 held-out player-fixtures:

| predictor | RMSE | MAE | Spearman | haul@5 |
|---|---|---|---|---|
| LightGBM, direct points target | 2.824 | 1.957 | 0.429 | 15.8% |
| FPL's own published xP | 2.763 | 1.929 | 0.547 | 35.8% |
| **constant 2.0** | 3.064 | **1.957** | n/a | 5.5% |

A constant predictor *ties* a trained model on MAE. FPL points for a regular
starter have median 2, and MAE is minimised by the median, so it rewards
flatness and is blind to ranking. Around 19% of all points come from hauls of
10+ that occur about 4% of the time, and that tail is where every transfer and
captaincy decision lives. RMSE and ranking metrics are reported instead.

Simulating gets the tail right where a mean cannot: ranking by P(>=10) reaches
haul@5 of 14.0% against 10.0% for ranking by expected points.

Points are also not comparable across seasons - defensive contribution only
began scoring in 2025-26 - so components are modelled and current scoring rules
applied afterwards from a versioned table. That is what keeps all ten seasons of
history usable rather than just the most recent one.

## Layout

```
scripts/
  ingest.py              FPL API -> DuckDB raw layer (idempotent, season-aware)
  recover_history.py     rebuild 2025-26 from archived API response bodies
  backfill_vaastav.py    2016-17 onward from the community archive
  build_scoring_rules.py derive and validate the scoring seed
  train_appearance.py    appearance and minutes model
  train_team_model.py    Dixon-Coles team model, walk-forward
  run_simulation.py      end-to-end: components -> simulation -> evaluation
  score_baseline.py      the old direct-regression model, for reference
  fplm/                  shared library (api, warehouse, features, components,
                         dixon_coles, simulate, optimize, plan, evaluate)
dbt/
  models/staging/        dedupe, type, unify two source lineages, point-in-time
  models/intermediate/   spines, rolling form over appearances, availability
  models/mart/           fct_player_fixture (training), dims, gameweek facts
  seeds/                 validated scoring rules, squad composition
  tests/                 grain, reconciliation and leakage assertions
docs/target-spec.html    full architecture spec
```

## Running it

```bash
uv sync
python scripts/ingest.py --entry <your-id> --league <your-league-id>
cd dbt && dbt build
python scripts/run_simulation.py --sims 1000 --gameweeks 1-5
```

Ingestion is append-only and deduplicated by payload hash, so re-running is a
no-op unless something actually changed. That append history is what makes
point-in-time player attributes possible at all.

## Stack

Python, DuckDB, dbt, LightGBM, SciPy (HiGHS for the MILP), uv.

Orchestration and a hosted front end are not built yet.
