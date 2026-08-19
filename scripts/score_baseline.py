#!/usr/bin/env python
"""Re-score the original LightGBM baseline against the rebuilt mart.

Purpose
-------
The old notebook reported a test MAE of ~2.634 on data that had 327 duplicated
player-gameweeks, a permanently-false blank flag, a wrong next-gameweek fixture
count, and rolling windows computed over duplicated rows. This script runs a
comparable model on the corrected mart so the cost of those bugs can actually be
measured, rather than assumed, before the architecture changes underneath it.

It is deliberately *not* the new architecture. It is a direct-regression
points-target model - the thing the rebuild replaces - kept only as a reference
point.

Two baselines are reported alongside it:

  FPL xP      FPL's own published expected points, a free public benchmark.
  Naive       the player's mean points over their last five appearances.

Comparability caveats, stated rather than buried: the old run trained at
gameweek grain on one season with a different feature set, so this is not a
controlled A/B. What it does establish is where a like-for-like model lands on
clean data, and how that compares to a public benchmark on identical rows.

Usage
-----
    python scripts/score_baseline.py                    # 2025-26, filtered
    python scripts/score_baseline.py --all-rows         # no minutes filter
    python scripts/score_baseline.py --seasons 2022-23,2023-24,2024-25,2025-26
"""

import argparse
import sys
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_error, mean_squared_error

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"

# Anything derived from the outcome of the fixture being predicted.
LEAKY_PREFIXES = ("target_", "benchmark_")
NON_FEATURES = {
    "season", "player_id", "fixture_id", "gameweek", "kickoff_time",
    "team_id", "opponent_team_id", "source", "finished_fixture",
}


def load(con, seasons):
    placeholders = ", ".join("?" for _ in seasons)
    return con.execute(
        f"""
        SELECT * FROM fct_player_fixture
        WHERE season IN ({placeholders})
          AND finished_fixture
        ORDER BY season, gameweek, player_id
        """,
        seasons,
    ).fetchdf()


def feature_columns(df):
    return [
        c for c in df.columns
        if c not in NON_FEATURES
        and not c.startswith(LEAKY_PREFIXES)
        and pd.api.types.is_numeric_dtype(df[c])
    ]


def temporal_split(df, train_max=25, val_max=29):
    """Split by gameweek, matching the original notebook's boundaries.

    On multi-season runs every season is split at the same gameweeks, so the
    model never sees a fixture that kicked off after one it is tested on within
    a season.
    """
    train = df[df["gameweek"] <= train_max]
    val = df[(df["gameweek"] > train_max) & (df["gameweek"] <= val_max)]
    test = df[df["gameweek"] > val_max]
    return train, val, test


def report(name, y_true, y_pred, groups=None):
    """Report level and ranking accuracy together.

    MAE is reported but must not be optimised against. FPL points for a regular
    starter have median 2 and mean 2.7, so MAE - which is minimised by the
    conditional median - rewards predicting a flat 2 for everyone. A constant
    predictor genuinely beats a trained model on this metric while having zero
    ability to rank anyone. Meanwhile roughly a fifth of all points come from
    hauls of 10+, which occur about 4% of the time.

    RMSE is therefore the primary metric, and ranking quality is what actually
    drives transfer and captaincy decisions.
    """
    mask = ~(pd.isna(y_true) | pd.isna(y_pred))
    if mask.sum() == 0:
        print(f"  {name:<26} no overlapping rows")
        return None

    yt, yp = y_true[mask], y_pred[mask]
    mae = mean_absolute_error(yt, yp)
    rmse = np.sqrt(mean_squared_error(yt, yp))
    rho = spearmanr(yt, yp).statistic if yp.nunique() > 1 else float("nan")

    line = f"  {name:<26} RMSE {rmse:>6.3f}   MAE {mae:>6.3f}   rho {rho:>6.3f}"

    # Precision@k within gameweek: of the players we ranked top-k this week, how
    # many actually returned a haul? This is the captaincy question.
    if groups is not None:
        g = pd.DataFrame({"g": groups[mask], "y": yt, "p": yp})
        hits, total = 0, 0
        for _, chunk in g.groupby("g"):
            if len(chunk) < 10:
                continue
            top = chunk.nlargest(5, "p")
            hits += (top["y"] >= 10).sum()
            total += len(top)
        if total:
            line += f"   haul@5 {hits / total:>6.1%}"
    print(line)
    return rmse


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--seasons", default="2025-26")
    ap.add_argument("--all-rows", action="store_true",
                    help="skip the minutes filter the original notebook applied")
    args = ap.parse_args()

    seasons = [s.strip() for s in args.seasons.split(",")]
    con = duckdb.connect(args.db, read_only=True)
    df = load(con, seasons)
    con.close()

    print(f"Seasons          : {', '.join(seasons)}")
    print(f"Rows loaded      : {len(df):,}")

    # The original notebook kept only players averaging >20 minutes over the
    # previous three gameweeks. That filter removes the easy-to-predict zeros,
    # which *raises* MAE - so it must be matched before comparing to 2.634.
    if not args.all_rows:
        before = len(df)
        df = df[df["avg_minutes_incl_absent_last3"] > 20]
        print(f"Minutes filter   : {before:,} -> {len(df):,} rows (>20 avg mins, last 3)")
    else:
        print("Minutes filter   : none")

    features = feature_columns(df)
    target = "target_total_points"
    df = df[df[target].notna()]

    train, val, test = temporal_split(df)
    print(f"Features         : {len(features)}")
    print(f"Train/Val/Test   : {len(train):,} / {len(val):,} / {len(test):,}\n")

    if len(val) == 0 or len(test) == 0:
        sys.exit("Not enough data after splitting; widen --seasons.")

    model = lgb.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=62,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    model.fit(
        train[features], train[target],
        eval_set=[(val[features], val[target])],
        eval_metric="l1",
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    test = test.copy()
    test["predicted"] = model.predict(test[features])

    gw_key = test["season"] + "-" + test["gameweek"].astype(str)
    print("TEST SET (fixture grain)   [RMSE is primary - see report() docstring]")
    report("LightGBM on clean mart", test[target], test["predicted"], gw_key)
    report("FPL xP (public baseline)", test[target], test["benchmark_fpl_xp"], gw_key)
    report("Naive: mean last 5", test[target], test["avg_points_last5"], gw_key)
    report("Naive: constant 2.0", test[target], pd.Series(2.0, index=test.index), gw_key)

    # Aggregating to gameweek grain is the comparable view: the old model was
    # built there, and it is the unit FPL settles points in.
    gw = (
        test.groupby(["season", "player_id", "gameweek"])
        .agg(actual=(target, "sum"),
             predicted=("predicted", "sum"),
             fpl_xp=("benchmark_fpl_xp", "sum"))
        .reset_index()
    )
    gw_key2 = gw["season"] + "-" + gw["gameweek"].astype(str)
    print("\nTEST SET (gameweek grain - comparable to the old 2.634)")
    report("LightGBM on clean mart", gw["actual"], gw["predicted"], gw_key2)
    report("FPL xP (public baseline)", gw["actual"], gw["fpl_xp"], gw_key2)
    report("Naive: constant 2.0", gw["actual"], pd.Series(2.0, index=gw.index), gw_key2)

    print("\nTop 15 features")
    imp = (
        pd.DataFrame({"feature": features, "gain": model.feature_importances_})
        .sort_values("gain", ascending=False)
        .head(15)
    )
    for _, r in imp.iterrows():
        print(f"  {r['feature']:<44} {r['gain']:>8.0f}")

    print("\nMAE by position (fixture grain)")
    test["abs_err"] = (test["predicted"] - test[target]).abs()
    pos_names = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD", 5: "AM"}
    by_pos = test.groupby("position")["abs_err"].agg(["mean", "count"])
    for pos, row in by_pos.iterrows():
        print(f"  {pos_names.get(pos, pos):<6} MAE {row['mean']:>6.3f}   n={int(row['count']):>6}")


if __name__ == "__main__":
    main()
