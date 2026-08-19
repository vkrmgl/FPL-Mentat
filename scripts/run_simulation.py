#!/usr/bin/env python
"""End-to-end: fit every component, simulate a held-out season, score it.

This is the whole architecture running as one thing - appearance model, team
model, per-90 component rates, the sampling loop, and the season's scoring rules
- evaluated walk-forward against what actually happened.

The comparison that matters is not MAE. A constant predictor of 2.0 ties a
trained regression on MAE, because FPL points for a starter have median 2 and
MAE is minimised by the median. What decides gameweeks is whether the top of the
ranking actually returns, so the headline numbers are RMSE, Spearman and
haul@5 - the share of each gameweek's five highest-ranked players who went on to
score 10 or more.

Usage
-----
    python scripts/run_simulation.py --sims 500 --gameweeks 30-38
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from fplm.components import ComponentSuite, fit_bps_weights  # noqa: E402
from fplm.dixon_coles import DixonColes  # noqa: E402
from fplm.evaluate import regression_metrics  # noqa: E402
from fplm.features import feature_columns, load_fixtures, minutes_class, season_split  # noqa: E402
from fplm.simulate import GameweekSimulator  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"


def parse_range(spec):
    lo, _, hi = spec.partition("-")
    return int(lo), int(hi or lo)


def fit_appearance(train, val, features):
    model = lgb.LGBMClassifier(
        objective="multiclass", num_class=3, n_estimators=1200,
        learning_rate=0.05, num_leaves=63, min_child_samples=40,
        subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
        random_state=42, verbosity=-1,
    )
    model.fit(
        train[features], train["minutes_class"],
        eval_set=[(val[features], val["minutes_class"])],
        callbacks=[lgb.early_stopping(80, verbose=False)],
    )
    return model


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--test-season", default="2025-26")
    ap.add_argument("--from-season", default="2020-21")
    ap.add_argument("--gameweeks", default="30-38")
    ap.add_argument("--sims", type=int, default=500)
    ap.add_argument("--half-life", type=float, default=730.0)
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    df = load_fixtures(con)
    rules = con.execute("SELECT * FROM stg_scoring_rules").fetchdf()
    matches = con.execute(
        """
        SELECT DISTINCT f.season, f.gameweek, f.fixture_id, f.kickoff_time,
               h.team_code AS home_code, a.team_code AS away_code,
               f.home_team_id, f.away_team_id,
               f.home_goals, f.away_goals, f.home_difficulty, f.away_difficulty
        FROM stg_fixtures f
        JOIN stg_teams h ON h.season=f.season AND h.team_id=f.home_team_id
        JOIN stg_teams a ON a.season=f.season AND a.team_id=f.away_team_id
        WHERE f.finished AND f.home_goals IS NOT NULL AND h.team_code IS NOT NULL
        ORDER BY f.kickoff_time
        """
    ).fetchdf()
    con.close()

    df = df[(df.season >= args.from_season) & df.position.isin([1, 2, 3, 4])].copy()
    df = df[df.target_minutes.notna()]
    df["minutes_class"] = minutes_class(df["target_minutes"])
    features = feature_columns(df, extra_exclude=["minutes_class"])

    train, val, _ = season_split(df, args.test_season)
    print(f"Train {len(train):,} / Val {len(val):,} rows, {len(features)} features")

    print("Fitting appearance model ...")
    appearance = fit_appearance(train, val, features)
    print("Fitting component rate models ...")
    # `val` is the held-out season's early gameweeks, strictly before the window
    # being simulated, so it is a legitimate fallback for components the
    # historical window cannot supply.
    suite = ComponentSuite().fit(train, val, features, fallback_train=val)
    print(f"  fitted : {', '.join(suite.models)}")
    for name, reason in suite.fallback_used.items():
        print(f"  IN-SEASON FALLBACK {name}: {reason}")
    for name, reason in suite.skipped.items():
        print(f"  SKIPPED {name}: {reason}")
    print("\nComponent fit on held-out season (players who appeared)")
    holdout = df[df.season == args.test_season]
    print(suite.evaluate(holdout).to_string(index=False))

    lo, hi = parse_range(args.gameweeks)

    # Empirical minutes within each appearance class, and the BPS residual
    # spread, both measured on training data rather than assumed.
    appeared = train[train.target_minutes > 0]
    minutes_pool = {
        1: appeared.loc[appeared.target_minutes < 60, "target_minutes"].to_numpy(),
        2: appeared.loc[appeared.target_minutes >= 60, "target_minutes"].to_numpy(),
    }
    bps_sd = suite.residual_sd(val, "bps")
    bps_weights = fit_bps_weights(pd.concat([train, val]))
    r2s = ", ".join(f"{names}:{spec['r2']:.2f}" for names, spec in sorted(bps_weights.items()))
    print(f"BPS from events, R2 by position: {r2s}")
    print(f"\nMinutes pool: {len(minutes_pool[1]):,} cameos "
          f"(median {np.median(minutes_pool[1]):.0f}), "
          f"{len(minutes_pool[2]):,} starts (median {np.median(minutes_pool[2]):.0f})")
    print(f"BPS residual sd: {bps_sd:.2f} (was hard-coded at 4.0)")

    simulator = GameweekSimulator(
        rules, n_sims=args.sims, minutes_pool=minutes_pool, bps_noise_sd=bps_sd,
        bps_weights=bps_weights,
    )
    results = []

    print(f"\nSimulating {args.test_season} gameweeks {lo}-{hi} at {args.sims} draws ...")
    for gw in range(lo, hi + 1):
        target_players = holdout[holdout.gameweek == gw]
        gw_matches = matches[(matches.season == args.test_season) & (matches.gameweek == gw)]
        if target_players.empty or gw_matches.empty:
            continue

        cutoff = gw_matches.kickoff_time.min()
        history = matches[matches.kickoff_time < cutoff]
        team_model = DixonColes(half_life_days=args.half_life).fit(
            history.home_code, history.away_code, history.home_goals,
            history.away_goals, history.kickoff_time, as_of=cutoff,
            home_cov=history.home_difficulty, away_cov=history.away_difficulty,
        )

        # Keep both aligned to target_players' index so a fixture's rows can be
        # pulled by label rather than by fragile positional arithmetic.
        probs = pd.DataFrame(
            appearance.predict_proba(target_players[features]),
            index=target_players.index,
        )
        rates = suite.rates(target_players)

        for fixture_id, fixture_players in target_players.groupby("fixture_id"):
            match = gw_matches[gw_matches.fixture_id == fixture_id]
            if match.empty:
                continue
            row = match.iloc[0]

            lam, mu = team_model.rates(
                [row.home_code], [row.away_code],
                home_cov=[row.home_difficulty], away_cov=[row.away_difficulty],
            )
            hg, ag = team_model.sample_scorelines(lam, mu, args.sims, simulator.rng)

            idx = fixture_players.index
            players = pd.DataFrame({
                "position": fixture_players["position"].to_numpy(),
                "is_home": (fixture_players["team_id"] == row.home_team_id).to_numpy(),
                "appearance_probs": list(probs.loc[idx].to_numpy()),
            })
            for component in rates.columns:
                players[component] = rates.loc[idx, component].to_numpy()

            points, _ = simulator.simulate_fixture(
                players, hg[:, 0], ag[:, 0], args.test_season
            )
            summary = simulator.summarise(
                {"season": args.test_season, "gameweek": gw,
                 "player_id": fixture_players["player_id"].to_numpy(),
                 "fixture_id": fixture_id},
                points,
            )
            summary["actual"] = fixture_players["target_total_points"].to_numpy()
            results.append(summary)

    if not results:
        sys.exit("No gameweeks simulated.")

    out = pd.concat(results, ignore_index=True)
    gw_key = out["season"] + "-" + out["gameweek"].astype(str)

    print(f"\nSimulated {len(out):,} player-fixtures across "
          f"{out.gameweek.nunique()} gameweeks\n")
    print("Held-out accuracy   [RMSE primary; MAE shown but not a decision metric]")
    for name, pred in [("Simulation xP", out["xp"]),
                       ("Simulation p50", out["p50"]),
                       ("Constant 2.0", pd.Series(2.0, index=out.index))]:
        m = regression_metrics(out["actual"], pred, gw_key)
        haul = m.get("haul_at_5", float("nan"))
        print(f"  {name:<18} RMSE {m['rmse']:.3f}  MAE {m['mae']:.3f}  "
              f"rho {m['spearman']:.3f}  haul@5 {haul:.1%}")

    print("\nRanking by P(haul) instead of by expected points")
    m = regression_metrics(out["actual"], out["p_haul"], gw_key)
    print(f"  {'P(>=10) ranking':<18} rho {m['spearman']:.3f}  haul@5 {m['haul_at_5']:.1%}")

    print("\nDistribution calibration")
    for label, col, lo_q, hi_q in [("80% interval", None, "p10", "p90")]:
        inside = ((out["actual"] >= out[lo_q]) & (out["actual"] <= out[hi_q])).mean()
        print(f"  actual inside {label}: {inside:.1%}  (target 80%)")
    print(f"  mean predicted xP {out['xp'].mean():.2f} vs actual {out['actual'].mean():.2f}")
    print(f"  predicted P(haul) {out['p_haul'].mean():.3f} vs observed "
          f"{(out['actual'] >= 10).mean():.3f}")

    top = out.nlargest(12, "p_haul")[
        ["gameweek", "player_id", "xp", "p_haul", "p90", "actual"]
    ]
    print("\nHighest P(haul) selections and what they returned")
    print(top.to_string(index=False))


if __name__ == "__main__":
    main()
