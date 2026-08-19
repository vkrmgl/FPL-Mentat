"""Loading and splitting the modelling mart.

Every component model shares this, so the definition of "what counts as a
feature" and "what counts as a leak" lives in exactly one place.
"""

import pandas as pd

# Anything describing the outcome of the fixture being predicted.
LEAKY_PREFIXES = ("target_", "benchmark_")

# Raw market counts are kept in the mart for reporting but must never be trained
# on: the historical lineage reports manager counts while the live API reports
# percentages and per-gameweek deltas, so a model fitted on one scale would
# score nonsense on the other. The `_rank_pct` versions are the comparable form.
LINEAGE_INCOMPATIBLE_SUFFIXES = ("_raw",)

# Identifiers and bookkeeping. `position` is deliberately absent - it is a
# legitimate feature and per-position models also split on it.
KEY_COLUMNS = {
    "season", "player_id", "fixture_id", "gameweek", "kickoff_time",
    "team_id", "opponent_team_id", "source", "finished_fixture",
}

POSITION_NAMES = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD", 5: "AM"}


def load_fixtures(con, seasons=None, finished_only=True):
    """Load fct_player_fixture, optionally restricted to given seasons."""
    where = ["1 = 1"]
    params = []
    if finished_only:
        where.append("finished_fixture")
    if seasons:
        where.append(f"season IN ({', '.join('?' for _ in seasons)})")
        params.extend(seasons)

    return con.execute(
        f"""
        SELECT * FROM fct_player_fixture
        WHERE {' AND '.join(where)}
        ORDER BY season, kickoff_time, player_id
        """,
        params,
    ).fetchdf()


def feature_columns(df, extra_exclude=()):
    """Numeric columns safe to train on."""
    excluded = KEY_COLUMNS | set(extra_exclude)
    return [
        c for c in df.columns
        if c not in excluded
        and not c.startswith(LEAKY_PREFIXES)
        and not c.endswith(LINEAGE_INCOMPATIBLE_SUFFIXES)
        and pd.api.types.is_numeric_dtype(df[c])
    ]


def season_split(df, test_season, val_gameweek=25):
    """Train on prior seasons, validate and test within the held-out season.

    Mirrors how the model is actually used: it goes into a new season having
    only seen previous ones, and is refit as that season accumulates. Splitting
    purely by gameweek across a pooled set would let the model see, say,
    gameweek 30 of 2023-24 while being tested on gameweek 30 of 2024-25.
    """
    train = df[df["season"] < test_season]
    held = df[df["season"] == test_season]
    val = held[held["gameweek"] <= val_gameweek]
    test = held[held["gameweek"] > val_gameweek]
    return train, val, test


def minutes_class(minutes):
    """Bucket minutes onto the FPL appearance-points boundaries.

    0 -> no appearance (0 pts), 1 -> cameo under 60 (1 pt), 2 -> 60+ (2 pts).
    The classes are the scoring thresholds themselves, so the model's output
    maps straight onto points without a second calibration step.
    """
    return pd.cut(
        minutes, bins=[-0.1, 0.5, 59.5, 200], labels=[0, 1, 2]
    ).astype("int8")
