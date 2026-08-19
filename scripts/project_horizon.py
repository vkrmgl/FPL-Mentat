#!/usr/bin/env python
"""Project every player across a horizon and write the result to the warehouse.

Why a horizon rather than a gameweek
------------------------------------
Optimising one gameweek picks players with a good fixture this week and ignores
what follows, which is how you end up transferring in a defender the week before
his club plays the top three in a row. Every projection here is produced for
each of the next H gameweeks, so a decision can weigh the whole run.

Two corrections are applied that a single-gameweek view cannot make:

  role decay      a player only starting because someone better is injured is
                  projected down as the horizon extends - see fplm/roles.py.

  fixture swing   the team model is re-evaluated per fixture, so an easy opener
                  followed by a hard run reads as exactly that.

Output lands in `fct_player_horizon`, one row per player per gameweek, which the
app and the optimiser both read. Nothing downstream calls a model directly.

Usage
-----
    python scripts/project_horizon.py --horizon 5 --sims 3000
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from fplm.components import ComponentSuite, fit_bps_weights  # noqa: E402
from fplm.dixon_coles import DixonColes  # noqa: E402
from fplm.features import feature_columns, load_fixtures, minutes_class  # noqa: E402
from fplm.roles import annotate_roles, minutes_multiplier  # noqa: E402
from fplm.simulate import GameweekSimulator  # noqa: E402
from predict_gameweek import COLD_START_MAP, build_frame, target_gameweek  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--season", default="2026-27")
    ap.add_argument("--start-gameweek", type=int)
    ap.add_argument("--horizon", type=int, default=5)
    ap.add_argument("--sims", type=int, default=3000)
    ap.add_argument("--from-season", default="2020-21")
    ap.add_argument("--half-life", type=float, default=730.0)
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    first_gw = target_gameweek(con, args.season, args.start_gameweek)
    gameweeks = list(range(first_gw, first_gw + args.horizon))

    history = load_fixtures(con)
    history = history[(history.season >= args.from_season)
                      & history.position.isin([1, 2, 3, 4])
                      & history.target_minutes.notna()].copy()
    history["minutes_class"] = minutes_class(history["target_minutes"])
    features = feature_columns(history, extra_exclude=["minutes_class"])

    seasons = sorted(history.season.unique())
    train = history[history.season < seasons[-1]]
    val = history[history.season == seasons[-1]]

    print(f"Season {args.season}, gameweeks {gameweeks[0]}-{gameweeks[-1]}, "
          f"{args.sims} draws")
    print(f"Fitted on {seasons[0]}..{seasons[-1]}")

    appearance = lgb.LGBMClassifier(
        objective="multiclass", num_class=3, n_estimators=1200, learning_rate=0.05,
        num_leaves=63, min_child_samples=40, subsample=0.8, subsample_freq=1,
        colsample_bytree=0.8, random_state=42, verbosity=-1)
    appearance.fit(train[features], train["minutes_class"],
                   eval_set=[(val[features], val["minutes_class"])],
                   callbacks=[lgb.early_stopping(80, verbose=False)])
    suite = ComponentSuite().fit(train, val, features, fallback_train=val)
    bps_weights = fit_bps_weights(history)

    matches = con.execute(
        """
        SELECT DISTINCT f.kickoff_time, h.team_code AS home_code, a.team_code AS away_code,
               f.home_goals, f.away_goals, f.home_difficulty, f.away_difficulty
        FROM stg_fixtures f
        JOIN stg_teams h ON h.season=f.season AND h.team_id=f.home_team_id
        JOIN stg_teams a ON a.season=f.season AND a.team_id=f.away_team_id
        WHERE f.finished AND f.home_goals IS NOT NULL AND h.team_code IS NOT NULL
        """).fetchdf()
    rules = con.execute("SELECT * FROM stg_scoring_rules").fetchdf()

    minutes_pool = {
        1: train.loc[(train.target_minutes > 0) & (train.target_minutes < 60), "target_minutes"].to_numpy(),
        2: train.loc[train.target_minutes >= 60, "target_minutes"].to_numpy(),
    }

    all_rows = []
    for offset, gameweek in enumerate(gameweeks):
        frame = build_frame(con, args.season, gameweek, features)
        if frame.empty:
            print(f"  GW{gameweek}: no fixtures")
            continue

        frame["chance_of_playing"] = frame["chance_of_playing_next_round"]
        frame = annotate_roles(frame)
        role_factor = minutes_multiplier(frame, offset)

        team_model = DixonColes(half_life_days=args.half_life).fit(
            matches.home_code, matches.away_code, matches.home_goals,
            matches.away_goals, matches.kickoff_time,
            as_of=frame.kickoff_time.min(),
            home_cov=matches.home_difficulty, away_cov=matches.away_difficulty)

        simulator = GameweekSimulator(rules, n_sims=args.sims,
                                      minutes_pool=minutes_pool,
                                      bps_weights=bps_weights, seed=42 + offset)
        probs = pd.DataFrame(appearance.predict_proba(frame[features]), index=frame.index)

        # Role decay moves probability mass out of "played 60+" and into "did not
        # play", which is what losing your place actually looks like.
        adjusted = probs.to_numpy().copy()
        shift = (1.0 - role_factor)[:, None] * adjusted[:, [1, 2]]
        adjusted[:, 0] += shift.sum(axis=1)
        adjusted[:, 1] -= shift[:, 0]
        adjusted[:, 2] -= shift[:, 1]
        adjusted = np.clip(adjusted, 0, None)
        adjusted /= adjusted.sum(axis=1, keepdims=True)

        rates = suite.rates(frame)
        results = []
        for fixture_id, chunk in frame.groupby("fixture_id"):
            lam, mu = team_model.rates(
                [chunk.home_code.iloc[0]], [chunk.away_code.iloc[0]],
                home_cov=[chunk.fixture_difficulty.iloc[0]],
                away_cov=[chunk.opponent_difficulty.iloc[0]])
            hg, ag = team_model.sample_scorelines(lam, mu, args.sims, simulator.rng)

            players = pd.DataFrame({
                "position": chunk["position"].to_numpy(),
                "is_home": chunk["is_home"].to_numpy(),
                "appearance_probs": list(adjusted[frame.index.get_indexer(chunk.index)]),
            })
            for component in rates.columns:
                players[component] = rates.loc[chunk.index, component].to_numpy()

            points, _ = simulator.simulate_fixture(players, hg[:, 0], ag[:, 0], args.season)
            summary = simulator.summarise({"player_id": chunk["player_id"].to_numpy()}, points)
            summary["fixture_difficulty"] = chunk["fixture_difficulty"].to_numpy()
            results.append(summary)

        gw_rows = pd.concat(results, ignore_index=True).groupby("player_id").agg(
            xp=("xp", "sum"), sd=("sd", "sum"), p_haul=("p_haul", "max"),
            p_return=("p_return", "max"), p90=("p90", "sum"),
            fixture_difficulty=("fixture_difficulty", "mean"),
            fixture_cnt=("xp", "size"),
        ).reset_index()

        meta = frame.drop_duplicates("player_id").set_index("player_id")
        gw_rows = gw_rows.join(
            meta[["web_name", "position", "team_id", "price", "ownership_percent",
                  "chance_of_playing_next_round", "news", "has_prior",
                  "is_deputy", "depth_rank"]], on="player_id")
        gw_rows["role_factor"] = pd.Series(role_factor, index=frame.index).groupby(
            frame["player_id"]).first().reindex(gw_rows.player_id).to_numpy()

        # Availability is applied here, not learned - no point-in-time history of
        # the flag exists, so it cannot be a training feature.
        availability = gw_rows["chance_of_playing_next_round"].fillna(100.0) / 100.0
        gw_rows["xp"] = gw_rows["xp"] * availability
        gw_rows["p_haul"] = gw_rows["p_haul"] * availability

        gw_rows["season"] = args.season
        gw_rows["gameweek"] = gameweek
        gw_rows["horizon_offset"] = offset
        gw_rows["se"] = gw_rows["sd"] / np.sqrt(args.sims)
        all_rows.append(gw_rows)
        print(f"  GW{gameweek}: {len(gw_rows)} players, "
              f"{int(gw_rows.is_deputy.sum())} deputies discounted, "
              f"mean xP {gw_rows.xp.mean():.2f}")

    out = pd.concat(all_rows, ignore_index=True)
    con.execute("DROP TABLE IF EXISTS fct_player_horizon")
    con.register("_horizon", out)
    con.execute("CREATE TABLE fct_player_horizon AS SELECT * FROM _horizon")
    con.unregister("_horizon")
    con.close()

    print(f"\nWrote fct_player_horizon: {len(out):,} rows "
          f"({out.player_id.nunique()} players x {out.gameweek.nunique()} gameweeks)")


if __name__ == "__main__":
    main()
