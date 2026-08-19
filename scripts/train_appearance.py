#!/usr/bin/env python
"""Appearance and minutes models - the first components of the decomposition.

Why these first
---------------
The measured gap between the direct-regression baseline and FPL's own published
xP is almost entirely a minutes problem. On four held-out seasons the baseline
picks a haul in its top five 15.8% of the time against FPL's 35.8%, and the one
thing FPL has that the baseline lacks is a real view of who is going to play.

Structure
---------
Points decompose as  E[points] = P(plays) x E[points | plays].  This script
builds the first factor, as a three-class problem whose classes are the FPL
appearance-points thresholds themselves:

    0  no appearance        0 pts
    1  cameo, 1-59 minutes  1 pt
    2  60 or more minutes   2 pts

so the output maps onto points with no second mapping step, and P(60+) is
directly what clean-sheet and defensive-contribution scoring keys off.

Calibration is applied and reported, not assumed. These probabilities are
sampled from a thousand times per gameweek in the simulator; a model that is
confidently wrong at 0.7 biases every draw.

Availability tags
-----------------
`chance_of_playing_next_round` is the strongest single predictor at prediction
time but does not exist point-in-time for historical seasons - only six
snapshots were ever captured for 2025-26, all in April. It is therefore left out
of training and applied at prediction time as a multiplier, which is the same
approach OpenFPL takes.

Usage
-----
    python scripts/train_appearance.py
    python scripts/train_appearance.py --test-season 2024-25
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.isotonic import IsotonicRegression  # noqa: E402

from fplm.evaluate import (  # noqa: E402
    calibration_table,
    classification_metrics,
    expected_calibration_error,
    print_metrics,
)
from fplm.features import (  # noqa: E402
    POSITION_NAMES,
    feature_columns,
    load_fixtures,
    minutes_class,
    season_split,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"
CLASS_LABELS = [0, 1, 2]
CLASS_NAMES = {0: "no appearance", 1: "cameo 1-59", 2: "60+ minutes"}


def fit_appearance(train, val, features):
    model = lgb.LGBMClassifier(
        objective="multiclass",
        num_class=3,
        n_estimators=1500,
        learning_rate=0.05,
        num_leaves=63,
        min_child_samples=40,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        random_state=42,
        verbosity=-1,
    )
    model.fit(
        train[features], train["minutes_class"],
        eval_set=[(val[features], val["minutes_class"])],
        eval_metric="multi_logloss",
        callbacks=[lgb.early_stopping(100, verbose=False)],
    )
    return model


def fit_calibrators(y_true, proba):
    """One isotonic regressor per class, fitted one-vs-rest on validation data.

    Multiclass probabilities are calibrated per class and renormalised, rather
    than trusting the softmax to be well-behaved. Isotonic is used over Platt
    because the miscalibration here is not reliably sigmoid-shaped.
    """
    calibrators = {}
    for i, label in enumerate(CLASS_LABELS):
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1)
        iso.fit(proba[:, i], (y_true == label).astype(float))
        calibrators[label] = iso
    return calibrators


def apply_calibration(calibrators, proba):
    out = np.column_stack([
        calibrators[label].predict(proba[:, i])
        for i, label in enumerate(CLASS_LABELS)
    ])
    # Renormalise so the three classes remain a distribution.
    totals = out.sum(axis=1, keepdims=True)
    return np.divide(out, totals, out=np.full_like(out, 1 / 3), where=totals > 0)


def expected_minutes(proba, train):
    """E[minutes] implied by the class probabilities.

    Uses the observed mean minutes within each class from the training data
    rather than bucket midpoints, since cameos cluster well below 30 and 60+
    appearances cluster well above 75.
    """
    means = (
        train.groupby("minutes_class")["target_minutes"].mean()
        .reindex(CLASS_LABELS).fillna(0.0).to_numpy()
    )
    return proba @ means, means


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--test-season", default="2025-26")
    ap.add_argument("--from-season", default="2020-21",
                    help="earliest season to train on; position is unavailable before 2020-21")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    df = load_fixtures(con)
    con.close()

    df = df[(df["season"] >= args.from_season) & (df["position"].isin([1, 2, 3, 4]))]
    df = df[df["target_minutes"].notna()].copy()
    df["minutes_class"] = minutes_class(df["target_minutes"])

    features = feature_columns(df, extra_exclude=["minutes_class"])
    train, val, test = season_split(df, args.test_season)

    print(f"Seasons trained  : {train['season'].min()} .. {train['season'].max()}")
    print(f"Held out         : {args.test_season}")
    print(f"Features         : {len(features)}")
    print(f"Train/Val/Test   : {len(train):,} / {len(val):,} / {len(test):,}")
    print("\nClass balance (train)")
    for label, share in train["minutes_class"].value_counts(normalize=True).sort_index().items():
        print(f"  {label} {CLASS_NAMES[label]:<16} {share:>6.1%}")

    if min(len(train), len(val), len(test)) == 0:
        sys.exit("Empty split; adjust --test-season or --from-season.")

    model = fit_appearance(train, val, features)
    raw_val = model.predict_proba(val[features])
    raw_test = model.predict_proba(test[features])

    calibrators = fit_calibrators(val["minutes_class"].to_numpy(), raw_val)
    cal_test = apply_calibration(calibrators, raw_test)

    y_test = test["minutes_class"].to_numpy()

    print("\nTEST SET - uncalibrated")
    print_metrics("", classification_metrics(y_test, raw_test, CLASS_LABELS))
    print("TEST SET - calibrated")
    print_metrics("", classification_metrics(y_test, cal_test, CLASS_LABELS))

    print("\nCalibration error (lower is better; >0.02 visibly biases a simulation)")
    for i, label in enumerate(CLASS_LABELS):
        binary = (y_test == label).astype(int)
        before = expected_calibration_error(binary, raw_test[:, i])
        after = expected_calibration_error(binary, cal_test[:, i])
        print(f"  P({CLASS_NAMES[label]:<16}) ECE {before:.4f} -> {after:.4f}")

    print("\nReliability of P(60+ minutes), calibrated")
    table = calibration_table((y_test == 2).astype(int), cal_test[:, 2])
    print(table.to_string(index=False))

    # The quantity the simulator actually consumes.
    exp_mins, class_means = expected_minutes(cal_test, train)
    actual = test["target_minutes"].to_numpy()
    print("\nImplied expected minutes")
    print("  class mean minutes (train): " +
          ", ".join(f"{CLASS_NAMES[c]}={m:.1f}" for c, m in zip(CLASS_LABELS, class_means)))
    print(f"  RMSE vs actual minutes    : {np.sqrt(np.mean((exp_mins - actual) ** 2)):.3f}")
    print(f"  mean predicted / actual   : {exp_mins.mean():.2f} / {actual.mean():.2f}")

    print("\nAccuracy by position")
    test = test.copy()
    test["pred_class"] = np.array(CLASS_LABELS)[cal_test.argmax(axis=1)]
    test["p_play"] = 1 - cal_test[:, 0]
    for pos, chunk in test.groupby("position"):
        acc = (chunk["pred_class"] == chunk["minutes_class"]).mean()
        ece = expected_calibration_error(
            (chunk["minutes_class"] > 0).astype(int), chunk["p_play"]
        )
        print(f"  {POSITION_NAMES.get(pos, pos):<5} n={len(chunk):>6}  accuracy {acc:.3f}  P(play) ECE {ece:.4f}")

    print("\nTop 15 features")
    imp = (
        pd.DataFrame({"feature": features, "gain": model.feature_importances_})
        .sort_values("gain", ascending=False).head(15)
    )
    for _, row in imp.iterrows():
        print(f"  {row['feature']:<46} {row['gain']:>8.0f}")


if __name__ == "__main__":
    main()
