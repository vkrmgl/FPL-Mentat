"""Squad optimization: turning projections into a decision.

A ranked list of expected points is not a team. The binding constraints - a
100.0 budget, fifteen players, at most three from any one club, a legal
formation - interact, so the best squad is rarely the fifteen highest-projected
players. That is a mixed-integer program, and it is solved here with HiGHS via
`scipy.optimize.milp`, which ships with SciPy and needs no extra dependency.

Two objectives are supported, and they are genuinely different problems:

  expected points   maximise the sum of projected points for the starting XI,
                    with the captain counted twice. The right objective when you
                    are trying to score well in absolute terms.

  league-relative   maximise expected points *net of what your rivals already
                    own*. Chasing a leader rewards differentials, because
                    matching their template guarantees you never catch them;
                    protecting a lead rewards the opposite. This is what "win
                    your league" actually means, and it is a different objective
                    function rather than a filter applied afterwards.

All structural constraints are read from the warehouse - squad sizes and
formation limits from stg_squad_rules, budget and club limit from
raw_game_settings - so a rule change arrives through ingestion rather than
through a constant buried in this file.
"""

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp


class SquadOptimizer:
    REQUIRED_POSITIONS = (1, 2, 3, 4)

    def __init__(self, squad_rules, budget=100.0, club_limit=3, squad_size=15,
                 starting_size=11):
        self._validate_rules(squad_rules, squad_size)
        self.squad_rules = squad_rules.set_index("position")
        self.budget = budget
        self.club_limit = club_limit
        self.squad_size = squad_size
        self.starting_size = starting_size

    @classmethod
    def _validate_rules(cls, squad_rules, squad_size):
        """Refuse to solve without complete composition rules.

        Without them the program is still feasible - it just stops being FPL.
        An empty rules frame silently drops every position constraint and the
        solver cheerfully returns a squad with six midfielders, which looks
        entirely plausible in a table and would be rejected by the game. Failing
        loudly here is the difference between a caught bug and a bad transfer.
        """
        if squad_rules is None or squad_rules.empty:
            raise ValueError(
                "squad_rules is empty - position constraints would be silently "
                "dropped. stg_squad_rules is populated from raw_element_types, "
                "which only exists for seasons ingested from the live API."
            )
        missing = set(cls.REQUIRED_POSITIONS) - set(squad_rules["position"])
        if missing:
            raise ValueError(f"squad_rules missing positions {sorted(missing)}")
        total = int(squad_rules["squad_count"].sum())
        if total != squad_size:
            raise ValueError(
                f"squad_rules position counts sum to {total}, expected {squad_size}"
            )

    def solve(self, players, value_column="xp", rival_ownership=None,
              differential_weight=0.0, locked=None, banned=None,
              bench_weight=0.15, min_bench_p_play=0.55, availability_floor=0.75,
              fixture_column=None, max_defensive_per_fixture=None):
        """Pick a squad, a starting XI and a captain.

        Variable layout is [squad | start | captain], each of length n, so the
        constraint matrices below are blocks over those three segments.

        Three rules here exist because leaving them out produces squads that
        look fine in a table and are obviously wrong to anyone who plays:

        bench_weight / min_bench_p_play
            With the bench worth nothing in the objective it is chosen on price
            alone, and fills up with third-choice keepers and academy forwards
            who will not take the pitch. A bench only has value when it can
            cover a non-starter, so bench slots carry a fraction of their points
            and every squad member must clear a floor on the probability of
            actually playing.

        availability_floor
            FPL publishes a percentage chance of playing. A player flagged at
            75% with an "unspecified injury" is not a bargain, he is a warning,
            and the optimiser has no business selecting him just because he is
            cheap.

        max_defensive_per_fixture
            Clean sheets on opposite sides of the same match are mutually
            exclusive. Selecting a goalkeeper from one team and defenders from
            their opponent guarantees at most one of the two returns, which the
            expected-points sum cannot see because it ignores correlation.
        """
        players = players.reset_index(drop=True)
        n = len(players)
        value = players[value_column].to_numpy(dtype=float)

        # League-relative objective: discount players the field already owns.
        if rival_ownership is not None and differential_weight > 0:
            owned = players["player_id"].map(rival_ownership).fillna(0.0).to_numpy()
            value = value - differential_weight * owned * np.abs(value)

        price = players["price"].to_numpy(dtype=float)
        position = players["position"].to_numpy()
        team = players["team_id"].to_numpy()

        # Minimise the negative: squad members carry a fraction of their value
        # (a bench slot is worth something, just not full price), starters carry
        # the rest, and the captain scores once more.
        objective = np.concatenate([
            -value * bench_weight,
            -value * (1.0 - bench_weight),
            -value,
        ])

        constraints = []

        def block(squad_w=None, start_w=None, captain_w=None):
            return np.concatenate([
                np.zeros(n) if squad_w is None else squad_w,
                np.zeros(n) if start_w is None else start_w,
                np.zeros(n) if captain_w is None else captain_w,
            ])

        ones = np.ones(n)
        constraints.append(LinearConstraint(block(squad_w=ones), self.squad_size, self.squad_size))
        constraints.append(LinearConstraint(block(start_w=ones), self.starting_size, self.starting_size))
        constraints.append(LinearConstraint(block(captain_w=ones), 1, 1))
        constraints.append(LinearConstraint(block(squad_w=price), 0, self.budget))

        # Squad composition, and how many of each position may start.
        for pos, rule in self.squad_rules.iterrows():
            mask = (position == pos).astype(float)
            constraints.append(LinearConstraint(
                block(squad_w=mask), rule.squad_count, rule.squad_count))
            constraints.append(LinearConstraint(
                block(start_w=mask), rule.min_playing, rule.max_playing))

        # At most `club_limit` players from any one club.
        for club in np.unique(team):
            mask = (team == club).astype(float)
            constraints.append(LinearConstraint(block(squad_w=mask), 0, self.club_limit))

        # A starter must be in the squad; a captain must be starting.
        rows_start, rows_captain = [], []
        for i in range(n):
            row = np.zeros(3 * n)
            row[i] = -1.0
            row[n + i] = 1.0
            rows_start.append(row)

            row = np.zeros(3 * n)
            row[n + i] = -1.0
            row[2 * n + i] = 1.0
            rows_captain.append(row)
        constraints.append(LinearConstraint(np.array(rows_start), -np.inf, 0))
        constraints.append(LinearConstraint(np.array(rows_captain), -np.inf, 0))

        # Defensive returns from both sides of one fixture cannot both happen.
        if fixture_column is not None and max_defensive_per_fixture:
            defensive = np.isin(position, [1, 2]).astype(float)
            for fixture in players[fixture_column].dropna().unique():
                mask = (players[fixture_column] == fixture).to_numpy() * defensive
                if mask.sum() > max_defensive_per_fixture:
                    constraints.append(LinearConstraint(
                        block(squad_w=mask), 0, max_defensive_per_fixture))

        lower = np.zeros(3 * n)
        upper = np.ones(3 * n)

        # Rule out anyone carrying an injury flag, and anyone unlikely to take
        # the pitch at all - a bench slot only has value if it can cover.
        if "chance_of_playing" in players.columns:
            flagged = players["chance_of_playing"].fillna(100.0).to_numpy() < availability_floor * 100
            upper[np.flatnonzero(flagged)] = 0.0
        if "p_play" in players.columns and min_bench_p_play:
            unlikely = players["p_play"].fillna(0.0).to_numpy() < min_bench_p_play
            upper[np.flatnonzero(unlikely)] = 0.0

        if locked:
            for pid in locked:
                hits = players.index[players.player_id == pid]
                for i in hits:
                    lower[i] = 1.0
        if banned:
            for pid in banned:
                hits = players.index[players.player_id == pid]
                for i in hits:
                    upper[i] = 0.0

        result = milp(
            c=objective,
            constraints=constraints,
            integrality=np.ones(3 * n),
            bounds=Bounds(lower, upper),
        )
        if not result.success:
            raise RuntimeError(f"No feasible squad: {result.message}")

        picks = np.round(result.x).astype(int)
        out = players.copy()
        out["in_squad"] = picks[:n].astype(bool)
        out["starting"] = picks[n:2 * n].astype(bool)
        out["is_captain"] = picks[2 * n:].astype(bool)
        return out[out.in_squad].sort_values(
            ["starting", "position", value_column], ascending=[False, True, False]
        )


def rival_ownership_from_picks(picks, n_rivals):
    """Share of rival squads owning each player - the field you are racing."""
    if picks is None or picks.empty or not n_rivals:
        return {}
    counts = picks.groupby("player_id")["entry_id"].nunique()
    return (counts / n_rivals).to_dict()
