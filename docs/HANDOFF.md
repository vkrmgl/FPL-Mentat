# FPL-Mentat — where things stand

Written 2026-08-19. Gameweek 1 locks **Friday 21 August, 17:30 UTC**.

The project was rebuilt from a single LightGBM points regressor into a component
decomposition with match simulation, and now has a working Streamlit app. `dbt build`
passes 103/103. HEAD is `73c5c92 "Season 2"`; 16 files are staged and uncommitted.

---

## Do these two things first

**1. Capture gameweek 1 the moment the deadline passes.**

```bash
python scripts/ingest.py --entry 5902157 --league 859625
```

Rival picks are only published *after* a deadline and cannot be backfilled. Miss it and
that gameweek's league data is gone the same way 2025-26 nearly was. Worth a calendar
reminder for Friday evening.

**2. Refresh projections once GW1 is played**, then look at how the cold start held up:

```bash
cd dbt && dbt build && cd ..
python scripts/project_horizon.py --horizon 5 --sims 3000
streamlit run app/streamlit_app.py
```

---

## What changed, and the one idea behind it

Everything follows from a single structural decision: **model football events, not points.**

FPL changed its scoring in 2025-26 (defensive contribution began scoring), so `total_points`
means different things in different seasons. A points regressor is therefore only
rule-compatible with about 30,000 rows. Predicting goals, assists, minutes and clean sheets —
which mean the same thing in 2016 as in 2026 — and applying *current* scoring rules afterwards
keeps all 253,831.

The second decision follows from a measurement. On held-out data:

| predictor | RMSE | MAE | Spearman | haul@5 |
|---|---|---|---|---|
| LightGBM, direct points target | 2.824 | 1.957 | 0.429 | 15.8% |
| FPL's own published xP | 2.763 | 1.929 | 0.547 | 35.8% |
| **constant 2.0** | 3.064 | **1.957** | n/a | 5.5% |

**A constant ties a trained model on MAE.** Points for a regular starter have median 2, and
MAE is minimised by the median, so it rewards flatness and is blind to ranking. About 19% of
all points come from hauls of 10+ occurring 4% of the time, and that tail is where every
decision lives. Use RMSE, Spearman and haul@5. Never optimise MAE, and don't use it for early
stopping.

That is also why the pipeline simulates rather than regresses: ranking by P(≥10) reaches
haul@5 of 14.0% against 10.0% for ranking by expected points. A mean cannot express that.

---

## Running the pipeline

```bash
uv sync

# 1. Raw layer. Append-only, deduplicated by payload hash - safe to re-run.
python scripts/ingest.py --entry 5902157 --league 859625

# 2. Transform. 22 models, 103 tests, about 2 seconds.
cd dbt && dbt build && cd ..

# 3. Projections -> fct_player_horizon (592 players x 5 gameweeks).
python scripts/project_horizon.py --horizon 5 --sims 3000

# 4. App.
streamlit run app/streamlit_app.py
```

One-off scripts, already run and not normally repeated:

| script | what it did |
|---|---|
| `recover_history.py` | rebuilt 2025-26 from archived API bodies in `raw_log` |
| `backfill_vaastav.py` | pulled 2016-17..2025-26 from the community archive |
| `build_scoring_rules.py` | derived and validated the scoring seed |

Diagnostics worth keeping: `score_baseline.py` (the old direct regressor, for reference),
`train_appearance.py`, `train_team_model.py`, `run_simulation.py` (end-to-end evaluation).

---

## The layers

**Raw** — 24 tables. Append-only with `season`, `ingested_at`, `ingest_run_id`. The append
history is what makes point-in-time attributes possible at all.

**Staging** — dedupe, type, unify two lineages (`api` and `vaastav`, tagged by `source`).
`stg_player_fixture` is 253,831 rows at **player × fixture** grain, which is the whole point:
the old pipeline collapsed double gameweeks and then fanned them back out, duplicating 327
player-gameweeks and corrupting every rolling window.

**Intermediate** — spines and form. Two ideas: spines make absence explicit (a blank gameweek
is a row with `fixture_cnt = 0`, not a missing row), and rolling windows count **appearances,
not gameweeks**, so a player back from injury is not judged on a run he was never fit for.

**Mart** — `fct_player_fixture` (training), `fct_player_gameweek` (reporting),
`fct_player_horizon` (projections the app reads), plus conformed dimensions.

**Models** — `scripts/fplm/`:

| module | role |
|---|---|
| `components.py` | six per-90 rate models, minutes as a Poisson exposure offset |
| `dixon_coles.py` | bivariate Poisson scorelines, FPL difficulty as a covariate |
| `simulate.py` | the sampling loop; BPS recomputed from simulated events |
| `roles.py` | deputy detection and minutes decay |
| `optimize.py` | MILP squad selection via HiGHS |
| `plan.py` | multi-gameweek transfer planning |
| `chips.py` | optimal-stopping chip policy |
| `advice.py` | squad-level recommendations |

---

## Decisions already settled — don't re-litigate

**Architecture**: component decomposition + Monte Carlo simulation. Chosen for what it
*enables* (P(haul), correlated clean sheets, structurally correct doubles), not for accuracy —
the ceiling is so low that a perfect model scores MAE ~1.9.

**Metric**: RMSE primary, plus Spearman and haul@5. MAE reported only.

**Horizon**: 5 gameweeks for transfers, because `max_extra_free_transfers = 4` caps the bank at
five and there is no strategy at six weeks not already expressible at five. 8–10 for chips.

**Team and player identity**: always FPL's `code`, never `id`. Ids are reassigned every
August — `team_id` 3 has been Bournemouth, Burnley, Brentford and Brighton. Player `code`
follows a career (Raya: one code, six seasons, five ids) and is what makes the cold start work.

**Scoring**: applied from `stg_scoring_rules`, never learned. The seed was derived from
`explain` data and validated by reconstruction against 253,578 historical player-gameweeks
with zero error. Re-check any time with `python scripts/build_scoring_rules.py --validate-only`.

---

## Traps — each of these cost real time

**DuckDB 1.5.1 silently returns wrong columns.** A window function partitioning directly over
a wide table swaps values between columns with no error:

```sql
-- WRONG: player_id comes back holding gameweek values
SELECT player_id, gameweek, row_number() OVER (PARTITION BY player_id, gameweek ...)
FROM fct_player_fixture

-- CORRECT: materialise in a base CTE first, then window over it
WITH base AS (SELECT player_id, gameweek, kickoff_time FROM fct_player_fixture ...),
ranked AS (SELECT *, row_number() OVER (...) rn FROM base)
SELECT player_id, gameweek FROM ranked WHERE rn = 1
```

The warehouse is **not** affected — verified by joining `stg_player_fixture` back to the raw
source: 29,757/29,757 rows match on minutes, points and gameweek. The exposure is ad-hoc
analysis queries. A separate 1.5.1 bug rejects `WHERE ... IN (...)` alongside `QUALIFY` in one
block; see the note in `stg_squad_rules.sql`. Retest both after any DuckDB upgrade.

**`st.html` strips SVG.** The formation pitch rendered as nothing at all — no error. Use
`streamlit.components.v1.html`, which does not sanitise.

**Streamlit sliders swallow the mouse wheel.** Scrolling with the cursor over one silently
changed the horizon from 5 to 1 and every number on the page with it. Controls are now pinned
to the foot of the window, which mostly avoids it. Number inputs would be immune if it recurs.

**`xP` is zero-filled for 71% of 2025-26** in the vaastav archive, including every row in
gameweeks 10–23. `fct_player_fixture` nulls the sentinel. Without that, FPL's own model looks
far worse than it is — it is actually their best season (MAE 1.681).

**Market features exist only in the historical lineage.** `transfers_in`, `selected` etc. are
vaastav-only and the API's equivalents are on different scales. They are reduced to
within-gameweek percentile ranks (`*_rank_pct`) so both lineages agree; the raw columns are
suffixed `_raw` and hard-excluded from training.

---

## Known weaknesses

**Cold start compresses the ranking.** No match has been played, so all form comes from last
season via player code. The overall level is about right but the spread is too narrow, which
systematically favours steady clean-sheet defenders over volatile attackers — hence the 5-4-1.
Should self-correct by around GW5. 22% of players have no prior season at all (promoted clubs,
overseas signings) and are under-rated rather than over-rated.

**Pre-season friendlies are not modelled.** The FPL API does not publish them, and FBref
exposes no scrapeable club-friendlies competition — `soccerdata` was installed and tried
against three season formats, all returning nothing. A player who looked sharp or was played
out of position through August is invisible. Would need a bespoke scraper against a source
verified to cover friendlies with player minutes.

**Monte Carlo noise is not negligible.** At 1,500 draws the top-two captain ordering flipped
with the random seed. Every projection now carries a standard error; at 8,000 draws it is
about 0.05 for a premium, so gaps under ~0.11 are noise. Fernandes and Haaland are currently
inside it — the tie breaks on haul probability, not the mean.

**Role decay is a heuristic.** `roles.py` ranks within club and position by price and prior
minutes, flags anyone blocked by an unavailable player above them, and decays their minutes on
a fixed return curve. It has no view of tactical preference or a manager who simply rates the
deputy. It corrects a known one-directional bias imperfectly, which beats ignoring it.

**Chip policy has estimated parts.** The stopping rule and opportunity distributions are
fitted from history, but the double-gameweek multiplier (1.75×) is my estimate, and Wildcard is
not in the policy at all — it is not a single-week decision. Chip *pairings* (wildcard into a
double, then bench boost) are not modelled; each chip is judged alone.

---

## Open work, roughly in order

1. **Commit the staged 16 files.**
2. **Weekly cadence** — after each deadline, run ingest with `--entry` and `--league`, then
   `dbt build`, then `project_horizon.py`.
3. **Re-run `score_baseline.py` after a few gameweeks** to see whether the simulation beats
   FPL's published xP on live data. Target: RMSE 2.763, haul@5 35.8%.
4. **Wildcard policy** — the one chip with no principled treatment.
5. **Fit the DGW multiplier** rather than assuming 1.75×.
6. **Hosting** — decided as Streamlit Community Cloud + MotherDuck; nothing built. The app
   only ever queries `fct_player_horizon`, so it is already shaped for it.
7. **Orchestration** — Docker and Airflow, deliberately last. Note the README and the public
   writeup still claim these exist; the README has been corrected, the writeup has not.

---

## Reference

- Architecture spec: `docs/target-spec.html`
- GW1 report: `docs/gw1-report.html`
- FPL entry **5902157** (Mainoochester United); mini-league **859625** ("Fight Club", 9 entries)
- 2025-26 archive: `data/backups/export_2025-26/` (Parquet, committed — the FPL API can no
  longer serve that season, so this is the only copy)
