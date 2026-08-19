"""Dixon-Coles bivariate Poisson model for match scorelines.

Why a parametric model here, when everything else is gradient boosting
-------------------------------------------------------------------
The simulator needs to *sample* correlated scorelines, not predict an expected
goal count. A booster can tell you Arsenal are worth 1.8 goals; it cannot hand
you a joint distribution over (home goals, away goals) from which a 2-1 can be
drawn with the right probability - and clean sheets, goals conceded and
defensive returns all depend on that joint draw, shared across every defender in
the same team.

The model
---------
Each team carries an attack and a defence rating; a global home-advantage term
lifts the home side. For a match between i and j:

    lambda = exp(attack_i + defence_j + home)     expected home goals
    mu     = exp(attack_j + defence_i)            expected away goals

Independent Poissons under-predict low-scoring draws, so Dixon and Coles (1997)
apply a correction `tau` to the four scorelines at or below 1-1, governed by a
single dependence parameter rho.

Recent matches carry more information about a team's current strength than old
ones, so the likelihood is weighted by exponential time decay with half-life
`half_life_days`.

Team identity uses FPL's stable `code`, never `team_id`: ids are reassigned every
August, so id 3 has been Bournemouth, Burnley, Brentford and Brighton.
"""

import numpy as np
from scipy.optimize import minimize
from scipy.stats import poisson

MAX_GOALS = 10


def tau(home_goals, away_goals, lam, mu, rho):
    """Dixon-Coles low-score correction.

    Adjusts only the 0-0, 0-1, 1-0 and 1-1 cells, where independent Poissons are
    measurably wrong. Everything else passes through unchanged.
    """
    h = np.asarray(home_goals)
    a = np.asarray(away_goals)
    out = np.ones(np.broadcast(h, a, lam, mu).shape, dtype=float)

    out = np.where((h == 0) & (a == 0), 1.0 - lam * mu * rho, out)
    out = np.where((h == 0) & (a == 1), 1.0 + lam * rho, out)
    out = np.where((h == 1) & (a == 0), 1.0 + mu * rho, out)
    out = np.where((h == 1) & (a == 1), 1.0 - rho, out)
    return out


class DixonColes:
    def __init__(self, half_life_days=180.0, max_goals=MAX_GOALS):
        self.half_life_days = half_life_days
        self.max_goals = max_goals
        self.teams_ = None
        self.attack_ = None
        self.defence_ = None
        self.home_advantage_ = None
        self.rho_ = None
        self.cov_beta_ = 0.0
        self.use_covariate_ = False
        self.cov_mean_ = 0.0

    # -- fitting ---------------------------------------------------------

    def _decay_weights(self, kickoffs, as_of):
        age_days = (as_of - kickoffs) / np.timedelta64(1, "D")
        age_days = np.maximum(age_days, 0.0)
        return 0.5 ** (age_days / self.half_life_days)

    def fit(self, home_team, away_team, home_goals, away_goals, kickoff,
            as_of=None, home_cov=None, away_cov=None):
        """Fit ratings from completed matches.

        `as_of` anchors the time decay. Passing the moment being predicted from
        keeps a walk-forward evaluation honest: matches after it get zero weight
        rather than quietly informing their own prediction.

        `home_cov`/`away_cov` are optional per-match covariates shifting each
        side's log rate - in practice FPL's own fixture difficulty rating.
        Measured walk-forward over 2025-26, a difficulty-only baseline scores
        RPS 0.1995 against 0.2112 for a goals-only fit at its best half-life, so
        the rating carries squad and transfer information that match history
        alone does not recover. Absorbing it as a covariate keeps that signal
        while retaining the joint scoreline distribution, which the rating
        cannot provide and which every clean-sheet and goals-conceded return
        depends on.
        """
        home_team = np.asarray(home_team)
        away_team = np.asarray(away_team)
        home_goals = np.asarray(home_goals, dtype=float)
        away_goals = np.asarray(away_goals, dtype=float)
        kickoff = np.asarray(kickoff, dtype="datetime64[ns]")
        as_of = np.datetime64(as_of) if as_of is not None else kickoff.max()

        self.use_covariate_ = home_cov is not None and away_cov is not None
        if self.use_covariate_:
            # Centre so the covariate shifts rates relative to an average
            # fixture rather than competing with the intercept.
            hc = np.asarray(home_cov, dtype=float)
            ac = np.asarray(away_cov, dtype=float)
            self.cov_mean_ = float(np.nanmean(np.concatenate([hc, ac])))
            hc = np.nan_to_num(hc - self.cov_mean_)
            ac = np.nan_to_num(ac - self.cov_mean_)
        else:
            self.cov_mean_ = 0.0
            hc = ac = np.zeros(len(home_goals))

        self.teams_ = np.unique(np.concatenate([home_team, away_team]))
        index = {team: i for i, team in enumerate(self.teams_)}
        n = len(self.teams_)

        hi = np.array([index[t] for t in home_team])
        ai = np.array([index[t] for t in away_team])
        weights = self._decay_weights(kickoff, as_of)

        def negative_log_likelihood(params):
            attack = params[:n]
            defence = params[n:2 * n]
            home_adv, rho, cov_beta = params[2 * n], params[2 * n + 1], params[2 * n + 2]

            # Identifiability: ratings are only defined up to a constant, so
            # centre the attack vector rather than leaving the optimiser to
            # wander along a flat direction.
            attack = attack - attack.mean()

            lam = np.exp(attack[hi] + defence[ai] + home_adv + cov_beta * hc)
            mu = np.exp(attack[ai] + defence[hi] + cov_beta * ac)
            lam = np.clip(lam, 1e-6, 25.0)
            mu = np.clip(mu, 1e-6, 25.0)

            correction = tau(home_goals, away_goals, lam, mu, rho)
            correction = np.clip(correction, 1e-9, None)

            ll = (
                np.log(correction)
                + poisson.logpmf(home_goals, lam)
                + poisson.logpmf(away_goals, mu)
            )
            return -np.sum(weights * ll)

        x0 = np.concatenate([
            np.zeros(n),           # attack
            np.zeros(n),           # defence
            [0.25],                # home advantage
            [-0.05],               # rho
            [0.0],                 # covariate coefficient
        ])
        bounds = (
            [(-3, 3)] * n + [(-3, 3)] * n
            + [(-1, 1), (-0.2, 0.2), (-1, 1)]
        )

        result = minimize(
            negative_log_likelihood, x0, method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 500},
        )

        params = result.x
        self.attack_ = params[:n] - params[:n].mean()
        self.defence_ = params[n:2 * n]
        self.home_advantage_ = params[2 * n]
        self.rho_ = params[2 * n + 1]
        self.cov_beta_ = params[2 * n + 2] if self.use_covariate_ else 0.0
        self.converged_ = bool(result.success)
        self.n_matches_ = len(home_goals)
        return self

    # -- prediction ------------------------------------------------------

    def rates(self, home_team, away_team, home_cov=None, away_cov=None):
        """Expected goals for both sides. Unknown teams fall back to average."""
        index = {team: i for i, team in enumerate(self.teams_)}

        def attack_of(t):
            return self.attack_[index[t]] if t in index else 0.0

        def defence_of(t):
            return self.defence_[index[t]] if t in index else float(self.defence_.mean())

        if self.use_covariate_ and home_cov is not None:
            hc = np.nan_to_num(np.asarray(home_cov, dtype=float) - self.cov_mean_)
            ac = np.nan_to_num(np.asarray(away_cov, dtype=float) - self.cov_mean_)
        else:
            hc = ac = np.zeros(len(home_team))

        lam = np.exp([
            attack_of(h) + defence_of(a) + self.home_advantage_ + self.cov_beta_ * c
            for h, a, c in zip(home_team, away_team, hc)
        ])
        mu = np.exp([
            attack_of(a) + defence_of(h) + self.cov_beta_ * c
            for h, a, c in zip(home_team, away_team, ac)
        ])
        return lam, mu

    def scoreline_grid(self, lam, mu):
        """Full corrected probability grid over scorelines, renormalised.

        Sampling draws directly from this rather than from a plain Poisson with
        a patch applied afterwards, so the low-score correction is actually
        reflected in the draws.
        """
        goals = np.arange(self.max_goals + 1)
        home_pmf = poisson.pmf(goals[:, None], lam)
        away_pmf = poisson.pmf(goals[:, None], mu)

        grid = home_pmf[:, None, :] * away_pmf[None, :, :]
        hh, aa = np.meshgrid(goals, goals, indexing="ij")
        correction = tau(hh[:, :, None], aa[:, :, None], lam, mu, self.rho_)

        grid = np.clip(grid * correction, 0.0, None)
        return grid / grid.sum(axis=(0, 1), keepdims=True)

    def sample_scorelines(self, lam, mu, n_sims, rng):
        """Draw (n_sims, n_matches) home and away goals from the corrected grid."""
        grid = self.scoreline_grid(lam, mu)
        n_cells = (self.max_goals + 1) ** 2
        flat = grid.reshape(n_cells, -1)

        home = np.empty((n_sims, flat.shape[1]), dtype=np.int16)
        away = np.empty_like(home)
        for m in range(flat.shape[1]):
            cells = rng.choice(n_cells, size=n_sims, p=flat[:, m])
            home[:, m] = cells // (self.max_goals + 1)
            away[:, m] = cells % (self.max_goals + 1)
        return home, away

    def outcome_probabilities(self, lam, mu):
        """P(home win), P(draw), P(away win) from the corrected grid."""
        grid = self.scoreline_grid(lam, mu)
        goals = np.arange(self.max_goals + 1)
        hh, aa = np.meshgrid(goals, goals, indexing="ij")
        return (
            grid[hh > aa].reshape(-1, grid.shape[2]).sum(axis=0),
            grid[hh == aa].reshape(-1, grid.shape[2]).sum(axis=0),
            grid[hh < aa].reshape(-1, grid.shape[2]).sum(axis=0),
        )

    def clean_sheet_probabilities(self, lam, mu):
        """P(home clean sheet), P(away clean sheet).

        A team keeps a clean sheet when the *opponent* fails to score, so these
        read off the opposite margin of the grid.
        """
        grid = self.scoreline_grid(lam, mu)
        return grid[:, 0, :].sum(axis=0), grid[0, :, :].sum(axis=0)

    def ratings_frame(self):
        import pandas as pd
        return pd.DataFrame({
            "team": self.teams_,
            "attack": self.attack_,
            "defence": self.defence_,
        }).sort_values("attack", ascending=False)
