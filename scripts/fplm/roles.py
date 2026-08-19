"""Role security: who keeps their place, and who is only playing because of an injury.

The problem
-----------
A projection built from recent minutes assumes the recent past continues. It
does not, when the reason a player is starting is that someone better is injured.
Mosquera starts for Arsenal while Timber is out; he is a fine pick this week and
a poor one in a month. A model that cannot see that will keep recommending him,
and will keep being wrong in the same direction.

The approach
------------
Within a club and position, rank players by standing - price first, since FPL
prices track expected role closely, with last season's minutes breaking ties.
A player is flagged a *deputy* when someone above them is currently unavailable.
Their expected minutes are then decayed across the horizon by the probability
that the incumbent has returned, which rises with each gameweek.

This is a heuristic, and worth naming as one. It has no view of tactical
preference, a new signing settling in, or a manager who simply rates the deputy.
What it does capture is the systematic error - deputies are over-projected in
exactly one direction - and correcting a known bias imperfectly beats ignoring
it.
"""

import numpy as np
import pandas as pd

# Probability the incumbent is back by gameweek offset 0, 1, 2, ... Injuries
# published as a percentage chance are mostly short-term, so the return curve is
# steep early and flattens once a player is several weeks absent.
DEFAULT_RETURN_CURVE = [0.05, 0.35, 0.60, 0.78, 0.88, 0.94]

# A player flagged below this is treated as currently unavailable.
UNAVAILABLE_BELOW = 75.0


def _return_probability(offset, curve=None):
    curve = curve or DEFAULT_RETURN_CURVE
    return curve[min(offset, len(curve) - 1)]


def annotate_roles(players, chance_column="chance_of_playing",
                   price_column="price", minutes_column="prior_minutes"):
    """Mark deputies and the standing that makes them one.

    Returns the frame with `is_deputy`, `blocked_by` and `depth_rank` added.
    `depth_rank` is 0 for the first-choice player at that club and position.
    """
    out = players.copy()
    out["_chance"] = out.get(chance_column, pd.Series(100.0, index=out.index)).fillna(100.0)
    out["_available"] = out["_chance"] >= UNAVAILABLE_BELOW
    out["_standing"] = (
        out[price_column].fillna(0) * 1000
        + out.get(minutes_column, pd.Series(0, index=out.index)).fillna(0) / 100.0
    )

    out["depth_rank"] = (
        out.groupby(["team_id", "position"])["_standing"]
        .rank(ascending=False, method="first") - 1
    ).astype(int)

    # A deputy is anyone with an unavailable player ranked above them.
    blocked_by, is_deputy = [], []
    for (team, position), chunk in out.groupby(["team_id", "position"]):
        ordered = chunk.sort_values("_standing", ascending=False)
        for idx, row in ordered.iterrows():
            above = ordered[ordered["_standing"] > row["_standing"]]
            absent = above[~above["_available"]]
            blocked_by.append((idx, absent.index[0] if len(absent) else None))
            is_deputy.append((idx, len(absent) > 0))
    out["blocked_by"] = pd.Series(dict(blocked_by))
    out["is_deputy"] = pd.Series(dict(is_deputy))

    return out.drop(columns=["_standing", "_available"])


def minutes_multiplier(players, gameweek_offset, curve=None):
    """Factor to scale expected minutes by, for a gameweek this far ahead.

    A deputy keeps full minutes while the incumbent is out and loses them as the
    return probability rises. The floor is their own prior-season start rate,
    since a deputy who started regularly last season is not displaced to zero -
    he simply stops being guaranteed.
    """
    baseline = players.get("prior_start_rate")
    if baseline is None:
        baseline = pd.Series(0.35, index=players.index)
    baseline = baseline.fillna(0.35).clip(0.15, 0.95)

    returned = _return_probability(gameweek_offset, curve)
    deputy = players.get("is_deputy", pd.Series(False, index=players.index)).fillna(False)

    # Not a deputy -> unchanged. Deputy -> blend toward their own baseline as
    # the incumbent's return becomes likely.
    return np.where(deputy, (1 - returned) + returned * baseline, 1.0)
