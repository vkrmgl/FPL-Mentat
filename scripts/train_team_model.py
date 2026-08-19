#!/usr/bin/env python
"""Fit and evaluate the Dixon-Coles team model, walk-forward.

Evaluation is walk-forward by gameweek: for each gameweek in the held-out
season the model is refit on every match that kicked off before it, with time
decay anchored at that gameweek's first kickoff. Nothing is fitted on a match it
is then scored against, which is the only way a team-strength model can be
honestly evaluated - a single fit over the whole period would let June inform
August.

Benchmarks, since a likelihood on its own means little:

  bookmaker-free baseline   FPL's own fixture difficulty rating, converted to
                            outcome probabilities by historical frequency
  home/draw/away base rate  the unconditional split, ~0.44 / 0.25 / 0.31

Scored with RPS (ranked probability score), which is the standard for ordered
three-way football outcomes and penalises confident wrong calls harder than a
plain log loss. Lower is better.

Usage
-----
    python scripts/train_team_model.py
    python scripts/train_team_model.py --test-season 2024-25 --half-life 120
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from fplm.dixon_coles import DixonColes  # noqa: E402
from fplm.evaluate import calibration_table, expected_calibration_error  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"


def load_matches(con):
    """One row per match, keyed on stable team codes."""
    return con.execute(
        """
        SELECT DISTINCT
            f.season,
            f.gameweek,
            f.fixture_id,
            f.kickoff_time,
            h.team_code AS home_code,
            a.team_code AS away_code,
            h.team_name AS home_name,
            a.team_name AS away_name,
            f.home_goals,
            f.away_goals,
            f.home_difficulty,
            f.away_difficulty
        FROM stg_fixtures f
        JOIN stg_teams h ON h.season = f.season AND h.team_id = f.home_team_id
        JOIN stg_teams a ON a.season = f.season AND a.team_id = f.away_team_id
        WHERE f.finished
          AND f.home_goals IS NOT NULL
          AND h.team_code IS NOT NULL
          AND a.team_code IS NOT NULL
        ORDER BY f.kickoff_time
        """
    ).fetchdf()


def rps(probs, outcome_index):
    """Ranked probability score for ordered three-way outcomes."""
    probs = np.asarray(probs, dtype=float)
    actual = np.zeros_like(probs)
    actual[np.arange(len(probs)), outcome_index] = 1.0
    cum_p = np.cumsum(probs, axis=1)
    cum_a = np.cumsum(actual, axis=1)
    return float(np.mean(np.sum((cum_p - cum_a) ** 2, axis=1) / (probs.shape[1] - 1)))


def difficulty_baseline(train, test):
    """Map FPL's difficulty differential onto historical outcome frequencies."""
    train = train.copy()
    train["diff_delta"] = train["home_difficulty"] - train["away_difficulty"]
    train["outcome"] = np.select(
        [train.home_goals > train.away_goals, train.home_goals == train.away_goals],
        [0, 1], default=2,
    )
    lookup = (
        train.groupby("diff_delta")["outcome"]
        .value_counts(normalize=True).unstack(fill_value=0)
        .reindex(columns=[0, 1, 2], fill_value=0)
    )
    overall = train["outcome"].value_counts(normalize=True).reindex([0, 1, 2], fill_value=0).to_numpy()

    rows = []
    for delta in (test["home_difficulty"] - test["away_difficulty"]):
        if delta in lookup.index:
            row = lookup.loc[delta].to_numpy()
            rows.append(row / row.sum() if row.sum() > 0 else overall)
        else:
            rows.append(overall)
    return np.vstack(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--test-season", default="2025-26")
    ap.add_argument("--half-life", type=float, default=180.0,
                    help="time-decay half-life in days")
    ap.add_argument("--min-train-matches", type=int, default=200)
    ap.add_argument("--no-difficulty", action="store_true",
                    help="fit on match history alone, ignoring FPL difficulty ratings")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    matches = load_matches(con)
    con.close()

    test_season = matches[matches.season == args.test_season]
    if test_season.empty:
        sys.exit(f"No finished matches for {args.test_season}")

    print(f"Matches available : {len(matches):,} ({matches.season.min()}..{matches.season.max()})")
    print(f"Held-out season   : {args.test_season} ({len(test_season)} matches)")
    print(f"Decay half-life   : {args.half_life:.0f} days\n")

    preds, actuals, cs_home, cs_away, cs_home_actual, cs_away_actual = [], [], [], [], [], []
    diff_preds = []
    gameweeks = sorted(test_season.gameweek.dropna().unique())

    for gw in gameweeks:
        target = test_season[test_season.gameweek == gw]
        cutoff = target.kickoff_time.min()
        history = matches[matches.kickoff_time < cutoff]
        if len(history) < args.min_train_matches:
            continue

        model = DixonColes(half_life_days=args.half_life)
        model.fit(
            history.home_code, history.away_code,
            history.home_goals, history.away_goals,
            history.kickoff_time, as_of=cutoff,
            home_cov=None if args.no_difficulty else history.home_difficulty,
            away_cov=None if args.no_difficulty else history.away_difficulty,
        )

        lam, mu = model.rates(
            target.home_code.to_numpy(), target.away_code.to_numpy(),
            home_cov=None if args.no_difficulty else target.home_difficulty.to_numpy(),
            away_cov=None if args.no_difficulty else target.away_difficulty.to_numpy(),
        )
        home_p, draw_p, away_p = model.outcome_probabilities(lam, mu)
        preds.append(np.column_stack([home_p, draw_p, away_p]))

        h_cs, a_cs = model.clean_sheet_probabilities(lam, mu)
        cs_home.append(h_cs)
        cs_away.append(a_cs)
        cs_home_actual.append((target.away_goals == 0).to_numpy().astype(int))
        cs_away_actual.append((target.home_goals == 0).to_numpy().astype(int))

        actuals.append(np.select(
            [target.home_goals > target.away_goals, target.home_goals == target.away_goals],
            [0, 1], default=2,
        ))
        diff_preds.append(difficulty_baseline(history, target))

    probs = np.vstack(preds)
    outcomes = np.concatenate(actuals)
    diff_probs = np.vstack(diff_preds)
    base_rate = np.tile(
        np.bincount(outcomes, minlength=3) / len(outcomes), (len(outcomes), 1)
    )

    print(f"Evaluated on {len(outcomes)} matches across {len(preds)} gameweeks\n")
    print("Match outcome RPS (lower is better)")
    print(f"  Dixon-Coles              {rps(probs, outcomes):.4f}")
    print(f"  FPL difficulty baseline  {rps(diff_probs, outcomes):.4f}")
    print(f"  Base rate (no model)     {rps(base_rate, outcomes):.4f}")

    accuracy = (probs.argmax(axis=1) == outcomes).mean()
    print(f"\nOutcome accuracy           {accuracy:.3f}")
    print(f"Home advantage (last fit)  {model.home_advantage_:+.3f}")
    print(f"Rho, low-score dependence  {model.rho_:+.4f}")
    if model.use_covariate_:
        print(f"Difficulty coefficient     {model.cov_beta_:+.4f}  "
              f"(negative = harder fixture, fewer goals)")

    cs_prob = np.concatenate(cs_home + cs_away)
    cs_true = np.concatenate(cs_home_actual + cs_away_actual)
    print("\nClean sheet probability - the input every defender's scoring depends on")
    print(f"  predicted mean {cs_prob.mean():.3f}   observed {cs_true.mean():.3f}   "
          f"ECE {expected_calibration_error(cs_true, cs_prob):.4f}")
    print(calibration_table(cs_true, cs_prob, n_bins=8).to_string(index=False))

    print("\nTeam ratings, final fit (attack high = scores more, defence low = concedes less)")
    ratings = model.ratings_frame()
    names = (
        matches[matches.season == args.test_season][["home_code", "home_name"]]
        .drop_duplicates().set_index("home_code")["home_name"]
    )
    ratings["team_name"] = ratings["team"].map(names)
    ratings = ratings.dropna(subset=["team_name"])
    print(ratings.head(6)[["team_name", "attack", "defence"]].to_string(index=False))
    print("  ...")
    print(ratings.tail(4)[["team_name", "attack", "defence"]].to_string(index=False))


if __name__ == "__main__":
    main()
