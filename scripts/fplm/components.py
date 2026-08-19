"""Per-player component models: rates per 90 minutes, not per match.

Everything here predicts a *rate*, with minutes carried as a Poisson exposure
offset rather than left as a feature to be inferred. A substitute who scores in
twenty minutes and a starter who scores in ninety are the same rate and very
different match outcomes; keeping them separate is what lets the appearance
model own the minutes question and these models own the football.

Nothing here predicts points. Rates are turned into events by the simulator,
and events are turned into points by the scoring-rules table, which is versioned
by season - so a model fitted across 2020-21..2025-26 is never asked to have
learned that defensive contribution started scoring in 2025-26.
"""

import numpy as np
import pandas as pd

import lightgbm as lgb

# name -> (target column, LightGBM objective, positions it applies to)
RATE_COMPONENTS = {
    "goals": ("target_goals_scored", "poisson", (1, 2, 3, 4)),
    "assists": ("target_assists", "poisson", (1, 2, 3, 4)),
    "saves": ("target_saves", "poisson", (1,)),
    "defensive_contribution": ("target_defensive_contribution", "poisson", (2, 3, 4)),
    "bps": ("target_bps", "regression", (1, 2, 3, 4)),
    "yellow_cards": ("target_yellow_cards", "poisson", (1, 2, 3, 4)),
}

MIN_MINUTES = 1


def _exposure_offset(minutes):
    """log(minutes / 90) - the Poisson offset that turns counts into per-90 rates."""
    return np.log(np.clip(minutes, MIN_MINUTES, None) / 90.0)


class RateModel:
    """A single per-90 component model."""

    def __init__(self, name, target, objective, positions):
        self.name = name
        self.target = target
        self.objective = objective
        self.positions = positions
        self.model = None
        self.features = None
        self.skip_reason = None

    def _subset(self, df):
        return df[df["position"].isin(self.positions) & (df["target_minutes"] >= MIN_MINUTES)]

    def fit(self, train, val, features):
        self.features = features
        tr, va = self._subset(train), self._subset(val)

        if len(tr) < 500:
            self.skip_reason = f"only {len(tr)} training rows"
            return None

        # A component can be structurally absent from the training window rather
        # than merely rare. `defensive_contribution` is the live example: FPL
        # published the underlying tackle and interception counts until 2018-19,
        # dropped them for six seasons, then reintroduced them in 2025-26 when
        # they started scoring. A Poisson objective on an all-zero target aborts
        # outright, so this is caught and reported rather than left to surface as
        # a crash mid-run.
        if float(tr[self.target].sum()) <= 0:
            self.skip_reason = (
                f"target is all-zero across {train['season'].min()}.."
                f"{train['season'].max()} - not yet observable in this window"
            )
            return None

        params = dict(
            objective=self.objective,
            n_estimators=1200,
            learning_rate=0.05,
            num_leaves=63,
            min_child_samples=40,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            random_state=42,
            verbosity=-1,
        )
        self.model = lgb.LGBMRegressor(**params)

        fit_kwargs = {}
        if self.objective == "poisson":
            # Exposure enters as an offset on the linear predictor, so the model
            # learns a rate rather than having to discover minutes from features.
            fit_kwargs["init_score"] = _exposure_offset(tr["target_minutes"])
            fit_kwargs["eval_init_score"] = [_exposure_offset(va["target_minutes"])]

        self.model.fit(
            tr[features], tr[self.target],
            eval_set=[(va[features], va[self.target])],
            callbacks=[lgb.early_stopping(80, verbose=False)],
            **fit_kwargs,
        )
        return self

    def predict_per90(self, df):
        """Expected events per 90 minutes."""
        if self.model is None:
            return np.zeros(len(df))
        raw = self.model.predict(df[self.features])
        if self.objective == "poisson":
            # raw already excludes the offset, so it is the per-90 rate directly.
            return np.clip(raw, 0.0, None)
        return raw

    def predict_for_minutes(self, df, minutes):
        """Expected events given a specific minutes figure."""
        return self.predict_per90(df) * np.clip(minutes, 0, None) / 90.0


class ComponentSuite:
    """All per-90 component models, fitted and applied together."""

    def __init__(self):
        self.models = {}
        self.skipped = {}
        self.fallback_used = {}

    def fit(self, train, val, features, fallback_train=None):
        """Fit every component, falling back to in-season data where needed.

        A component can be absent from the historical window but present in the
        season being predicted - defensive contribution is the live case, since
        FPL only began publishing the underlying counts again in 2025-26 when
        they started scoring. Rather than drop the component entirely, it is
        refitted on `fallback_train`, which must be strictly earlier than the
        window being predicted. That is also what production does: early in a
        rule change you train on the gameweeks played so far.
        """
        for name, (target, objective, positions) in RATE_COMPONENTS.items():
            model = RateModel(name, target, objective, positions)
            if model.fit(train, val, features) is not None:
                self.models[name] = model
                continue

            if fallback_train is not None:
                reason = model.skip_reason
                retry = RateModel(name, target, objective, positions)
                # Split the fallback window in two so early stopping still has
                # data it was not fitted on.
                cut = int(len(fallback_train) * 0.8)
                if retry.fit(fallback_train.iloc[:cut], fallback_train.iloc[cut:],
                             features) is not None:
                    self.models[name] = retry
                    self.fallback_used[name] = reason
                    continue

            self.skipped[name] = model.skip_reason
        return self

    def residual_sd(self, df, name):
        """Std-dev of a component's residuals, for sizing simulation noise.

        The bonus draw ranks predicted BPS within a fixture, so how noisy that
        ranking should be is an empirical question - a hard-coded jitter either
        washes out real separation or invents it.
        """
        model = self.models.get(name)
        if model is None:
            return 0.0
        subset = model._subset(df)
        if subset.empty:
            return 0.0
        expected = model.predict_for_minutes(subset, subset["target_minutes"])
        return float(np.std(subset[model.target].to_numpy(dtype=float) - expected))

    def rates(self, df):
        """Per-90 rate for every component, as a frame aligned to `df`."""
        out = pd.DataFrame(index=df.index)
        for name, model in self.models.items():
            rate = model.predict_per90(df)
            # Zero out components that do not apply to the position, so a
            # goalkeeper never contributes to a defensive-contribution draw.
            mask = df["position"].isin(model.positions).to_numpy()
            out[name] = np.where(mask, rate, 0.0)
        return out

    def evaluate(self, test):
        """Per-component fit quality on players who actually appeared."""
        rows = []
        for name, model in self.models.items():
            subset = model._subset(test)
            if subset.empty:
                continue
            expected = model.predict_for_minutes(subset, subset["target_minutes"])
            actual = subset[model.target].to_numpy(dtype=float)
            rows.append({
                "component": name,
                "n": len(subset),
                "actual_total": actual.sum(),
                "predicted_total": expected.sum(),
                "bias_pct": 100 * (expected.sum() - actual.sum()) / max(actual.sum(), 1),
                "rmse": float(np.sqrt(np.mean((expected - actual) ** 2))),
            })
        return pd.DataFrame(rows)


# BPS event weights, fitted rather than transcribed --------------------------
BPS_EVENT_COLUMNS = [
    "target_goals_scored", "target_assists", "target_clean_sheet",
    "target_saves", "target_goals_conceded", "target_yellow_cards",
    "target_red_cards", "target_own_goals", "target_defensive_contribution",
]


def fit_bps_weights(df, min_rows=200):
    """Per-position BPS weights, regressed on the events that produce them.

    BPS is very nearly a deterministic function of match events - fitting it
    here recovers FPL's published system (clean sheet 12, yellow -3, goals
    scaling 12/18/24 by position) at R-squared 0.84 to 0.94.

    This matters because bonus is the single largest driver of hauls: 62% of
    10-point returns include three bonus points, against only 18% that include a
    second goal. Predicting BPS from features and then drawing bonus from it
    independently severs the link - bonus lands on whoever the static prediction
    liked, not on whoever scored in *that* draw. Recomputing BPS from the
    simulated events restores the correlation that produces hauls in the first
    place.

    Returns {position: {"weights": {...}, "intercept": float, "residual_sd": float}}.
    """
    from sklearn.linear_model import LinearRegression

    out = {}
    # Restrict to rows where every constituent event is actually recorded.
    # Defensive contribution is null before 2025-26, and imputing it as zero
    # would drag its fitted weight toward nothing on seasons that simply were
    # not measuring it.
    played = df[(df["target_minutes"] > 0) & df["target_bps"].notna()]
    played = played.dropna(subset=BPS_EVENT_COLUMNS)
    for position, chunk in played.groupby("position"):
        if len(chunk) < min_rows:
            continue
        features = chunk[BPS_EVENT_COLUMNS].to_numpy(dtype=float)
        features = np.column_stack([features, (chunk["target_minutes"] >= 60).astype(float)])
        target = chunk["target_bps"].to_numpy(dtype=float)

        model = LinearRegression().fit(features, target)
        residuals = target - model.predict(features)
        out[int(position)] = {
            "weights": dict(zip(BPS_EVENT_COLUMNS + ["played60"], model.coef_)),
            "intercept": float(model.intercept_),
            "residual_sd": float(np.std(residuals)),
            "r2": float(model.score(features, target)),
        }
    return out
