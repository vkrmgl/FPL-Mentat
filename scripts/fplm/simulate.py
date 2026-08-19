"""The simulation loop: components in, distributions out.

For each fixture the simulator draws a correlated scoreline from the Dixon-Coles
grid, decides who played and for how long, allocates the sampled goals and
assists among those players in proportion to their rates, resolves bonus by
ranking BPS within the fixture, and applies the season's scoring rules. Repeating
that a thousand times gives a distribution per player rather than a point
estimate.

Three things fall out of doing it this way that a direct points regression
cannot give you:

  P(haul)     captaincy is a right-tail decision. Ranking by expected points and
              ranking by P(>=10) are different orderings, and the second is the
              one that wins gameweeks.

  correlation a clean sheet is one sampled event shared by every defender and
              the keeper in that team, so squad-level variance is real rather
              than assumed independent.

  doubles     a double gameweek is two fixtures summed inside the same draw. No
              multiplier, no fudge factor.
"""

import numpy as np
import pandas as pd

# Bonus points awarded to the top three BPS scorers in a fixture.
BONUS_BY_RANK = {0: 3, 1: 2, 2: 1}


def _multinomial_allocate(total, weights, rng):
    """Split `total` events among players in proportion to `weights`.

    Handles the degenerate cases explicitly: no goals to allocate, or a set of
    players with no attacking rate between them.
    """
    if total <= 0 or len(weights) == 0:
        return np.zeros(len(weights), dtype=np.int16)
    total_weight = weights.sum()
    if total_weight <= 0:
        return np.zeros(len(weights), dtype=np.int16)
    return rng.multinomial(int(total), weights / total_weight).astype(np.int16)


class GameweekSimulator:
    """Simulate one gameweek from fitted components."""

    def __init__(self, scoring_rules, n_sims=1000, seed=42,
                 assist_rate=0.75, defcon_thresholds=None,
                 minutes_pool=None, bps_noise_sd=4.0, bps_weights=None):
        self.rules = scoring_rules
        self.n_sims = n_sims
        self.rng = np.random.default_rng(seed)
        # Not every goal is assisted; the rest are unassisted or own goals.
        self.assist_rate = assist_rate
        self.defcon_thresholds = defcon_thresholds or {2: 10, 3: 12, 4: 12}

        # Observed minutes within each appearance class, sampled from directly.
        # Drawing uniform(60, 90) for a starter is badly wrong: real 60+
        # appearances pile up at exactly 90, and the shape matters because every
        # per-90 rate is scaled by it.
        self.minutes_pool = minutes_pool or {}
        # Sized from the BPS model's own residuals rather than guessed.
        self.bps_noise_sd = bps_noise_sd
        # Per-position BPS weights over simulated events. When present, bonus is
        # computed from the draw rather than from a static prediction, which is
        # what keeps it correlated with the goals and clean sheets in the same
        # draw - the mechanism that actually produces hauls.
        self.bps_weights = bps_weights or {}

    # -- scoring ---------------------------------------------------------

    def _points_for(self, season, position, identifier, values):
        """Apply the season's rules for one identifier to an array of values."""
        rules = self.rules[
            (self.rules.season == season)
            & (self.rules.position == position)
            & (self.rules.identifier == identifier)
        ]
        if rules.empty:
            return np.zeros_like(values, dtype=float)

        points = np.zeros_like(values, dtype=float)
        per_unit = rules[rules.rule_type == "per_unit"]
        if not per_unit.empty:
            rule = per_unit.iloc[0]
            points += (values // rule.unit_size) * rule.points

        thresholds = rules[rules.rule_type == "threshold"].sort_values("min_value")
        if not thresholds.empty:
            tier_points = np.zeros_like(values, dtype=float)
            for _, rule in thresholds.iterrows():
                tier_points = np.where(values >= rule.min_value, rule.points, tier_points)
            points += tier_points

        passthrough = rules[rules.rule_type == "passthrough"]
        if not passthrough.empty:
            points += values
        return points

    # -- the loop --------------------------------------------------------

    @staticmethod
    def _rate(players, name, n):
        """Per-90 rate for a component, or zeros when it was not fitted.

        A component can be legitimately absent for a given season - defensive
        contribution has no training signal before 2025-26 - and the simulation
        should degrade to "this never happens" rather than fail. The scoring
        rules stay the source of truth either way: an absent component simply
        contributes nothing to the draw.
        """
        if name not in players.columns:
            return np.zeros((n, 1))
        return np.clip(players[name].to_numpy()[:, None], 0, None)

    def simulate_fixture(self, players, home_goals, away_goals, season):
        """Simulate one fixture across all draws.

        `players` must carry, per player: position, is_home, appearance class
        probabilities, and per-90 rates. `home_goals`/`away_goals` are arrays of
        length n_sims already drawn from the Dixon-Coles grid, so both sides of
        the match see the same sampled scoreline.
        """
        n = len(players)
        sims = self.n_sims
        rng = self.rng

        # --- who played, and for how long -----------------------------
        class_probs = np.stack(players["appearance_probs"].to_numpy())
        classes = np.array([
            rng.choice(3, size=sims, p=p) for p in class_probs
        ])                                            # (n_players, sims)
        cameo_pool = self.minutes_pool.get(1)
        starter_pool = self.minutes_pool.get(2)
        cameo = (rng.choice(cameo_pool, size=(n, sims)) if cameo_pool is not None
                 else rng.uniform(1, 59, size=(n, sims)))
        starter = (rng.choice(starter_pool, size=(n, sims)) if starter_pool is not None
                   else rng.uniform(60, 90, size=(n, sims)))
        minutes = np.where(classes == 0, 0.0, np.where(classes == 1, cameo, starter))
        played = minutes > 0
        exposure = minutes / 90.0

        is_home = players["is_home"].to_numpy()
        position = players["position"].to_numpy()

        team_goals = np.where(is_home[:, None], home_goals[None, :], away_goals[None, :])
        conceded = np.where(is_home[:, None], away_goals[None, :], home_goals[None, :])

        # --- allocate goals and assists within each team --------------
        goals = np.zeros((n, sims), dtype=np.int16)
        assists = np.zeros((n, sims), dtype=np.int16)
        goal_weight = self._rate(players, "goals", n) * exposure
        assist_weight = self._rate(players, "assists", n) * exposure

        for side, mask in (("h", is_home), ("a", ~is_home)):
            idx = np.flatnonzero(mask)
            if idx.size == 0:
                continue
            side_goals = home_goals if side == "h" else away_goals
            for s in range(sims):
                scored = side_goals[s]
                if scored <= 0:
                    continue
                goals[idx, s] = _multinomial_allocate(scored, goal_weight[idx, s], rng)
                # Assists are drawn separately: not every goal has one, and the
                # scorer is rarely the assister.
                n_assists = rng.binomial(scored, self.assist_rate)
                assists[idx, s] = _multinomial_allocate(n_assists, assist_weight[idx, s], rng)

        # --- independent per-player events ----------------------------
        saves = rng.poisson(self._rate(players, "saves", n) * exposure)
        defcon = rng.poisson(self._rate(players, "defensive_contribution", n) * exposure)
        yellows = np.minimum(
            rng.poisson(self._rate(players, "yellow_cards", n) * exposure), 1
        )

        clean_sheet = ((conceded == 0) & (minutes >= 60)).astype(np.int16)

        # --- bonus: rank BPS within the fixture ------------------------
        if self.bps_weights:
            bps = np.zeros((n, sims), dtype=float)
            events = {
                "target_goals_scored": goals,
                "target_assists": assists,
                "target_clean_sheet": clean_sheet,
                "target_saves": saves,
                "target_goals_conceded": conceded,
                "target_yellow_cards": yellows,
                "target_defensive_contribution": defcon,
            }
            noise_sd = 0.0
            for pos in np.unique(position):
                spec = self.bps_weights.get(int(pos))
                if spec is None:
                    continue
                sel = position == pos
                acc = np.full((sel.sum(), sims), spec["intercept"], dtype=float)
                for column, weight in spec["weights"].items():
                    if column == "played60":
                        acc += weight * (minutes[sel] >= 60)
                    elif column in events:
                        acc += weight * events[column][sel]
                bps[sel] = acc
                noise_sd = max(noise_sd, spec["residual_sd"])
            bps = np.where(played, bps + rng.normal(0, noise_sd, size=(n, sims)), -np.inf)
        else:
            bps = self._rate(players, "bps", n) * exposure
            bps = np.where(
                played, bps + rng.normal(0, self.bps_noise_sd, size=(n, sims)), -np.inf
            )
        bonus = np.zeros((n, sims), dtype=np.int16)
        order = np.argsort(-bps, axis=0)
        for rank, pts in BONUS_BY_RANK.items():
            winners = order[rank, :]
            bonus[winners, np.arange(sims)] = pts
        bonus = np.where(played, bonus, 0)

        # --- points ----------------------------------------------------
        points = np.zeros((n, sims), dtype=float)
        for pos in np.unique(position):
            sel = position == pos
            if not sel.any():
                continue
            points[sel] += self._points_for(season, pos, "minutes", minutes[sel])
            points[sel] += self._points_for(season, pos, "goals_scored", goals[sel])
            points[sel] += self._points_for(season, pos, "assists", assists[sel])
            points[sel] += self._points_for(season, pos, "clean_sheets", clean_sheet[sel])
            points[sel] += self._points_for(season, pos, "goals_conceded",
                                            np.where(played[sel], conceded[sel], 0))
            points[sel] += self._points_for(season, pos, "saves", saves[sel])
            points[sel] += self._points_for(season, pos, "yellow_cards", yellows[sel])
            points[sel] += self._points_for(season, pos, "bonus", bonus[sel])

            threshold = self.defcon_thresholds.get(pos)
            if threshold is not None:
                points[sel] += self._points_for(
                    season, pos, "defensive_contribution", defcon[sel]
                )
        return points, minutes

    def summarise(self, keys, points):
        """Collapse draws into the distribution the optimizer consumes."""
        return pd.DataFrame({
            **keys,
            "xp": points.mean(axis=1),
            "p10": np.percentile(points, 10, axis=1),
            "p50": np.percentile(points, 50, axis=1),
            "p90": np.percentile(points, 90, axis=1),
            "sd": points.std(axis=1),
            "p_haul": (points >= 10).mean(axis=1),
            "p_blank": (points <= 2).mean(axis=1),
            "p_return": (points >= 5).mean(axis=1),
        })
