"""Chip strategy as an optimal-stopping problem.

The mistake in a threshold like "play Bench Boost above 12 points" is that it
ignores what makes a chip hard: it is a one-shot option with an expiry. The
question is never "is this good?" but "is this better than the best week I am
likely to get before the window closes?" - and the answer changes every week,
because the number of remaining chances falls.

So the policy here is the classical one for stopping problems:

    play now  <=>  value_now  >=  E[ max of the k remaining opportunities ]

with k the gameweeks left in that chip's window. Early in a half the bar is
high, because there is plenty of time for something better. In the last week it
collapses to zero, because an unplayed chip scores nothing.

Everything the policy needs is estimated from data rather than asserted:

  opportunity distribution   what a chip has historically been worth in a given
                             gameweek, measured without hindsight - squads are
                             ranked on form available *before* that gameweek,
                             then scored on what actually happened.

  double-gameweek prior      chips pay most in a double, and doubles are not on
                             the fixture list yet when the decision is made.
                             Their historical frequency by gameweek number gives
                             a prior for what is still to come.

The competitive edge is not a better number. It is that the bar adapts: it
refuses a mediocre week in GW2 and accepts the same week in GW17, which is
exactly the trade a fixed threshold cannot express.
"""

import numpy as np
import pandas as pd

BENCH_SIZE = 4
SQUAD_SIZE = 15


def historical_opportunities(con, from_season="2020-21"):
    """What each chip would have been worth, per season and gameweek.

    Squads are picked on form known *before* the gameweek and then scored on
    actual points, so these are opportunities a manager could really have taken
    rather than hindsight-optimal ones.
    """
    rows = con.execute(
        """
        SELECT season, gameweek, player_id, position,
               avg_points_last5 AS prior_form,
               target_total_points AS points,
               target_minutes AS minutes
        FROM fct_player_gameweek g
        JOIN (
            SELECT season AS s, player_id AS p, gameweek AS w,
                   any_value(avg_points_last5) AS avg_points_last5,
                   any_value(position) AS pos
            FROM fct_player_fixture
            GROUP BY season, player_id, gameweek
        ) f ON f.s = g.season AND f.p = g.player_id AND f.w = g.gameweek
        WHERE g.season >= ?
        """,
        [from_season],
    ).fetchdf() if False else con.execute(
        """
        SELECT season, gameweek, player_id,
               any_value(position) AS position,
               any_value(avg_points_last5) AS prior_form,
               sum(target_total_points) AS points,
               sum(target_minutes) AS minutes
        FROM fct_player_fixture
        WHERE season >= ? AND finished_fixture
        GROUP BY season, player_id, gameweek
        """,
        [from_season],
    ).fetchdf()

    rows = rows[rows.prior_form.notna()]
    out = []
    for (season, gameweek), chunk in rows.groupby(["season", "gameweek"]):
        # The fifteen a form-following manager would plausibly have held.
        squad = chunk.nlargest(SQUAD_SIZE, "prior_form")
        if len(squad) < SQUAD_SIZE:
            continue
        # Bench is the four lowest-projected of those fifteen.
        bench = squad.nsmallest(BENCH_SIZE, "prior_form")
        captain = squad.nlargest(1, "prior_form").iloc[0]

        out.append({
            "season": season,
            "gameweek": int(gameweek),
            # Bench Boost pays exactly what the bench scores.
            "bench_boost": float(bench.points.sum()),
            # Triple Captain pays one extra multiple of the captain's return.
            "triple_captain": float(captain.points),
            # Free Hit rescues a gameweek where your own players do not play.
            # Its value is therefore the gap between the eleven you would field
            # from the whole pool - restricted to players who actually turned
            # out - and the eleven your squad could field. Both sides are ranked
            # on form known beforehand, so neither has hindsight; measuring it
            # against a hindsight-optimal eleven inflates the chip to about a
            # hundred points, which no free hit has ever been worth.
            "free_hit": float(
                chunk[chunk.minutes > 0].nlargest(11, "prior_form").points.sum()
                - squad.nlargest(11, "prior_form").points.sum()
            ),
            "players_available": int((squad.minutes > 0).sum()),
        })
    return pd.DataFrame(out)


def double_gameweek_prior(con, from_season="2018-19"):
    """P(a gameweek turns into a double) by gameweek number.

    Doubles arrive from cup rescheduling and are not on the fixture list when a
    chip decision is made, so their historical timing is the only forward-looking
    signal available. Smoothed lightly, since ten seasons is thin for 38 slots.
    """
    counts = con.execute(
        """
        SELECT gameweek,
               count(DISTINCT season || '-' || cast(team_id AS varchar))
                   FILTER (WHERE fixture_cnt >= 2) AS doubles,
               count(DISTINCT season) AS seasons
        FROM int_team_gameweek_spine
        WHERE season >= ? AND gameweek <= 38
        GROUP BY gameweek ORDER BY gameweek
        """,
        [from_season],
    ).fetchdf()
    # Share of the twenty clubs that doubled, averaged over seasons.
    counts["share"] = counts.doubles / (counts.seasons * 20)
    smoothed = counts.set_index("gameweek")["share"].rolling(
        3, center=True, min_periods=1).mean()
    return smoothed.to_dict()


class ChipPolicy:
    """Optimal-stopping thresholds for one chip, fitted to its own history."""

    def __init__(self, values, dgw_prior=None, dgw_multiplier=1.75):
        self.values = np.asarray(sorted(v for v in values if np.isfinite(v)))
        self.dgw_prior = dgw_prior or {}
        self.dgw_multiplier = dgw_multiplier

    def threshold(self, gameweeks_remaining, upcoming_gameweeks=()):
        """The bar this week: what the best of the remaining chances is worth.

        Estimated by simulating the remaining draws from the historical
        distribution, lifting any gameweek that has a real chance of becoming a
        double - a chip in a double is worth close to twice a chip in a single,
        and that possibility is most of what makes waiting rational.
        """
        if gameweeks_remaining <= 0 or len(self.values) == 0:
            return 0.0

        rng = np.random.default_rng(7)
        draws = rng.choice(self.values, size=(4000, gameweeks_remaining))

        if upcoming_gameweeks:
            lift = np.array([
                1.0 + self.dgw_prior.get(int(gw), 0.0) * (self.dgw_multiplier - 1.0)
                for gw in upcoming_gameweeks[:gameweeks_remaining]
            ])
            draws = draws * lift[None, : draws.shape[1]]

        return float(np.mean(draws.max(axis=1)))

    def percentile_of(self, value):
        if len(self.values) == 0:
            return float("nan")
        return float((self.values <= value).mean())


def build_policies(con, from_season="2020-21"):
    """One policy per chip, plus the double-gameweek prior they share."""
    opportunities = historical_opportunities(con, from_season)
    prior = double_gameweek_prior(con)
    return {
        "bboost": ChipPolicy(opportunities.bench_boost, prior),
        "3xc": ChipPolicy(opportunities.triple_captain, prior),
        "freehit": ChipPolicy(opportunities.free_hit, prior),
    }, opportunities, prior


def recommend(policies, chip_name, value_now, gameweek, window_end,
              upcoming_gameweeks=(), squad_note=None):
    """Play or hold, from the stopping rule rather than a fixed number."""
    policy = policies.get(chip_name)
    if policy is None:
        return None

    remaining = max(0, int(window_end) - int(gameweek))
    bar = policy.threshold(remaining, upcoming_gameweeks)
    percentile = policy.percentile_of(value_now)
    margin = value_now - bar

    if remaining == 0:
        verdict = "play now"
        reason = ("Last gameweek of the window - an unplayed chip is worth "
                  "nothing, so anything beats holding.")
    elif margin >= 0:
        verdict = "play now"
        reason = (f"Worth {value_now:.1f}, against {bar:.1f} for the best of the "
                  f"{remaining} weeks left. Better than what is likely to come.")
    else:
        verdict = "hold"
        reason = (f"Worth {value_now:.1f}, below the {bar:.1f} the best of the "
                  f"{remaining} remaining weeks is worth. Waiting is worth "
                  f"{-margin:.1f} points.")

    return {
        "chip": chip_name,
        "verdict": verdict,
        "value_now": float(value_now),
        "threshold": float(bar),
        "margin": float(margin),
        "percentile": percentile,
        "weeks_left": remaining,
        "reason": reason if squad_note is None else f"{reason} {squad_note}",
    }
