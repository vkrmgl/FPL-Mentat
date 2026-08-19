#!/usr/bin/env python
"""Produce projections and a recommended squad for an upcoming gameweek.

This is the thing the rest of the repo exists to run. It fits the component
models on every completed season, builds a feature frame for fixtures that have
not been played, simulates them, and reports both a projection table and a legal
squad.

The cold start
--------------
In gameweek 1 nothing has been played, so every rolling form feature is null.
The fallback is the player's own previous season, joined on FPL's career-stable
`player_code` - `player_id` is reassigned every August, so it cannot be used for
this. Roughly 78% of a new season's squad list has a prior season; the rest are
genuine unknowns (promoted-club players, new signings from abroad) and are left
with null form for the model to treat as such.

Prior-season rates are mapped onto the feature names the models were trained on.
That is an approximation, and a deliberate one: a player's last-season scoring
rate is a far better prior than nothing, but it does not know about a transfer,
a new manager, or a change of role. Confidence should be lower in gameweek 1
than in gameweek 10, and the `has_prior` column marks which players have any
basis at all.

Usage
-----
    python scripts/predict_gameweek.py                     # next unplayed gameweek
    python scripts/predict_gameweek.py --gameweek 1 --sims 1000
    python scripts/predict_gameweek.py --entry 5902157     # ... and your squad
"""

import argparse
import pathlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from fplm.components import ComponentSuite, fit_bps_weights  # noqa: E402
from fplm.dixon_coles import DixonColes  # noqa: E402
from fplm.features import POSITION_NAMES, feature_columns, load_fixtures, minutes_class  # noqa: E402
from fplm.optimize import SquadOptimizer  # noqa: E402
from fplm.simulate import GameweekSimulator  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"

# Prior-season rate -> the trained feature it stands in for during a cold start.
COLD_START_MAP = {
    "prior_appearance_rate": ["appearance_rate_last3", "appearance_rate_last5",
                              "appearance_rate_last10"],
    "prior_start_rate": ["start_rate_last3", "start_rate_last5", "start_rate_last10",
                         "played_60_rate_last3", "played_60_rate_last5",
                         "played_60_rate_last10"],
    "prior_minutes_per_appearance": ["avg_minutes_incl_absent_last3",
                                     "avg_minutes_incl_absent_last5",
                                     "avg_minutes_incl_absent_last10"],
    "prior_goals_p90": ["goals_scored_p90_last3", "goals_scored_p90_last5",
                        "goals_scored_p90_last10", "expected_goals_p90_last3",
                        "expected_goals_p90_last5", "expected_goals_p90_last10"],
    "prior_assists_p90": ["assists_p90_last3", "assists_p90_last5", "assists_p90_last10",
                          "expected_assists_p90_last3", "expected_assists_p90_last5",
                          "expected_assists_p90_last10"],
    "prior_bonus_p90": ["bonus_p90_last5"],
    "prior_bps_p90": ["bps_p90_last3", "bps_p90_last5", "bps_p90_last10"],
    "prior_saves_p90": ["saves_p90_last3", "saves_p90_last5"],
    "prior_clean_sheet_rate": ["player_clean_sheet_rate_last5"],
    "prior_points_per_appearance": ["avg_points_last3", "avg_points_last5",
                                    "avg_points_last10"],
}


def target_gameweek(con, season, requested):
    if requested:
        return requested
    row = con.execute(
        "SELECT min(gameweek) FROM stg_fixtures WHERE season = ? AND NOT finished",
        [season],
    ).fetchone()
    if not row or row[0] is None:
        sys.exit(f"No unplayed fixtures for {season}")
    return int(row[0])


def build_frame(con, season, gameweek, features):
    """One row per player per fixture in the target gameweek."""
    fixtures = con.execute(
        """
        SELECT f.fixture_id, f.gameweek, f.kickoff_time,
               f.home_team_id, f.away_team_id, f.home_difficulty, f.away_difficulty,
               h.team_code AS home_code, a.team_code AS away_code
        FROM stg_fixtures f
        JOIN stg_teams h ON h.season=f.season AND h.team_id=f.home_team_id
        JOIN stg_teams a ON a.season=f.season AND a.team_id=f.away_team_id
        WHERE f.season = ? AND f.gameweek = ?
        """,
        [season, gameweek],
    ).fetchdf()

    players = con.execute(
        """
        WITH latest AS (
            SELECT *, row_number() OVER (PARTITION BY player_id
                                         ORDER BY valid_from DESC) AS rn
            FROM stg_player_snapshot WHERE season = ?
        )
        SELECT l.player_id, l.player_code, l.web_name, l.team_id, l.position,
               l.price, l.ownership_percent, l.status,
               l.chance_of_playing_next_round, l.news,
               l.penalties_order, l.fk_order, l.corners_order,
               l.transfers_in_event, l.transfers_out_event,
               p.* EXCLUDE (season, player_code)
        FROM latest l
        LEFT JOIN int_player_prior_season p
          ON p.season = ? AND p.player_code = l.player_code
        WHERE l.rn = 1 AND l.position IN (1,2,3,4)
        """,
        [season, season],
    ).fetchdf()

    rows = []
    for _, fx in fixtures.iterrows():
        for side, team, opp, is_home, diff, opp_diff in (
            ("h", fx.home_team_id, fx.away_team_id, True, fx.home_difficulty, fx.away_difficulty),
            ("a", fx.away_team_id, fx.home_team_id, False, fx.away_difficulty, fx.home_difficulty),
        ):
            chunk = players[players.team_id == team].copy()
            chunk["fixture_id"] = fx.fixture_id
            chunk["gameweek"] = fx.gameweek
            chunk["kickoff_time"] = fx.kickoff_time
            chunk["opponent_team_id"] = opp
            chunk["is_home"] = is_home
            chunk["fixture_difficulty"] = diff
            chunk["opponent_difficulty"] = opp_diff
            chunk["home_code"] = fx.home_code
            chunk["away_code"] = fx.away_code
            rows.append(chunk)

    frame = pd.concat(rows, ignore_index=True)
    frame["season"] = season
    frame["has_prior"] = frame["prior_minutes"].notna()

    # Market signals, ranked within the gameweek exactly as the mart does.
    for source, name in (("ownership_percent", "ownership_rank_pct"),
                         ("transfers_in_event", "transfers_in_rank_pct"),
                         ("transfers_out_event", "transfers_out_rank_pct")):
        frame[name] = frame[source].rank(pct=True)

    # Cold start: stand prior-season rates in for the rolling features.
    for prior_col, targets in COLD_START_MAP.items():
        if prior_col not in frame.columns:
            continue
        for target in targets:
            frame[target] = frame[prior_col]

    frame["gameweek_fixture_cnt"] = frame.groupby("player_id")["fixture_id"].transform("count")
    frame["is_double_gameweek"] = frame["gameweek_fixture_cnt"] >= 2
    frame["is_blank_gameweek"] = False

    for column in features:
        if column not in frame.columns:
            frame[column] = np.nan
    return frame


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--season", default="2026-27")
    ap.add_argument("--gameweek", type=int)
    ap.add_argument("--sims", type=int, default=1000)
    ap.add_argument("--from-season", default="2020-21")
    ap.add_argument("--half-life", type=float, default=730.0)
    ap.add_argument("--entry", type=int, help="also report this manager's squad")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--json-out", help="write the full projection set to this path")
    ap.add_argument("--seed", type=int, default=42, help="simulation seed")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    gameweek = target_gameweek(con, args.season, args.gameweek)

    history = load_fixtures(con)
    history = history[(history.season >= args.from_season)
                      & history.position.isin([1, 2, 3, 4])
                      & history.target_minutes.notna()].copy()
    history["minutes_class"] = minutes_class(history["target_minutes"])
    features = feature_columns(history, extra_exclude=["minutes_class"])

    # Fit on everything completed; validate on the most recent season.
    seasons = sorted(history.season.unique())
    train = history[history.season < seasons[-1]]
    val = history[history.season == seasons[-1]]

    print(f"Season {args.season}, gameweek {gameweek}")
    print(f"Fitted on {seasons[0]}..{seasons[-1]}  "
          f"({len(train):,} train / {len(val):,} validation rows)")

    appearance = lgb.LGBMClassifier(
        objective="multiclass", num_class=3, n_estimators=1200, learning_rate=0.05,
        num_leaves=63, min_child_samples=40, subsample=0.8, subsample_freq=1,
        colsample_bytree=0.8, random_state=42, verbosity=-1,
    )
    appearance.fit(train[features], train["minutes_class"],
                   eval_set=[(val[features], val["minutes_class"])],
                   callbacks=[lgb.early_stopping(80, verbose=False)])
    suite = ComponentSuite().fit(train, val, features, fallback_train=val)
    bps_weights = fit_bps_weights(history)

    frame = build_frame(con, args.season, gameweek, features)
    print(f"Candidates: {len(frame)} player-fixtures, "
          f"{frame.has_prior.mean():.0%} with prior-season history")

    # Say plainly when the projections are running on last season's form. The
    # level stays roughly right - a projected starter lands near the 3.8 points
    # that the average 60-minute appearance is worth - but the *spread* is
    # compressed, because a prior-season rate separates players far less sharply
    # than in-season form does. The practical effect is that steady clean-sheet
    # returns from defenders outrank volatile attacking returns more than they
    # should, and the ordering is less trustworthy than it will be by GW5.
    if gameweek <= 3:
        print("\n  NOTE: early-season cold start. Form features come from last")
        print("  season via player_code, so rankings are less separated than")
        print("  mid-season and defenders are systematically favoured. Treat the")
        print("  ordering as indicative; captaincy in particular carries more")
        print("  uncertainty than the P(haul) figures alone suggest.")

    matches = con.execute(
        """
        SELECT DISTINCT f.kickoff_time, h.team_code AS home_code, a.team_code AS away_code,
               f.home_goals, f.away_goals, f.home_difficulty, f.away_difficulty
        FROM stg_fixtures f
        JOIN stg_teams h ON h.season=f.season AND h.team_id=f.home_team_id
        JOIN stg_teams a ON a.season=f.season AND a.team_id=f.away_team_id
        WHERE f.finished AND f.home_goals IS NOT NULL AND h.team_code IS NOT NULL
        """
    ).fetchdf()
    squad_rules = con.execute(
        "SELECT * FROM stg_squad_rules WHERE season = ?", [args.season]).fetchdf()
    rules = con.execute("SELECT * FROM stg_scoring_rules").fetchdf()
    settings = con.execute(
        "SELECT squad_total_spend, squad_team_limit, squad_squadsize, squad_squadplay "
        "FROM raw_game_settings LIMIT 1").fetchdf().iloc[0]
    chips = con.execute(
        "SELECT chip_name, first_gameweek, last_gameweek FROM stg_chips "
        "WHERE season = ? ORDER BY first_gameweek, chip_name", [args.season]).fetchdf()
    entry_squad = None
    if args.entry:
        entry_squad = con.execute(
            "SELECT DISTINCT element FROM raw_entry_pick WHERE entry_id = ? "
            "AND season = ? AND gameweek = ?", [args.entry, args.season, gameweek - 1]
        ).fetchdf()
    con.close()

    team_model = DixonColes(half_life_days=args.half_life).fit(
        matches.home_code, matches.away_code, matches.home_goals, matches.away_goals,
        matches.kickoff_time, as_of=frame.kickoff_time.min(),
        home_cov=matches.home_difficulty, away_cov=matches.away_difficulty,
    )

    minutes_pool = {
        1: train.loc[(train.target_minutes > 0) & (train.target_minutes < 60), "target_minutes"].to_numpy(),
        2: train.loc[train.target_minutes >= 60, "target_minutes"].to_numpy(),
    }
    simulator = GameweekSimulator(rules, n_sims=args.sims, minutes_pool=minutes_pool,
                                  bps_weights=bps_weights, seed=args.seed)

    probs = pd.DataFrame(appearance.predict_proba(frame[features]), index=frame.index)
    rates = suite.rates(frame)

    results = []
    for fixture_id, chunk in frame.groupby("fixture_id"):
        lam, mu = team_model.rates(
            [chunk.home_code.iloc[0]], [chunk.away_code.iloc[0]],
            home_cov=[chunk.fixture_difficulty.iloc[0]],
            away_cov=[chunk.opponent_difficulty.iloc[0]],
        )
        hg, ag = team_model.sample_scorelines(lam, mu, args.sims, simulator.rng)

        players = pd.DataFrame({
            "position": chunk["position"].to_numpy(),
            "is_home": chunk["is_home"].to_numpy(),
            "appearance_probs": list(probs.loc[chunk.index].to_numpy()),
        })
        for component in rates.columns:
            players[component] = rates.loc[chunk.index, component].to_numpy()

        points, _ = simulator.simulate_fixture(players, hg[:, 0], ag[:, 0], args.season)
        summary = simulator.summarise(
            {"player_id": chunk["player_id"].to_numpy()}, points)
        summary["fixture_id"] = fixture_id
        results.append(summary)

    sims = pd.concat(results, ignore_index=True)
    # Sum across fixtures so a double gameweek is two fixtures added, not averaged.
    # P(appearing at all), straight from the appearance model - the optimizer
    # needs it to avoid a bench of players who will not take the pitch.
    frame["p_play"] = 1.0 - probs[0].to_numpy()

    per_player = sims.groupby("player_id").agg(
        xp=("xp", "sum"), p90=("p90", "sum"), sd=("sd", "sum"),
        p_haul=("p_haul", "max"), p_return=("p_return", "max"),
    ).reset_index()

    # Monte Carlo standard error on the mean. Without it two players a tenth of
    # a point apart look ranked when they are indistinguishable - the ordering
    # of the top two genuinely flips with the random seed at low draw counts.
    per_player["se"] = per_player["sd"] / np.sqrt(args.sims)

    meta = frame.drop_duplicates("player_id").set_index("player_id")
    out = per_player.join(
        meta[["web_name", "position", "team_id", "price", "ownership_percent",
              "chance_of_playing_next_round", "has_prior", "news", "p_play",
              "fixture_id"]],
        on="player_id")
    out["chance_of_playing"] = out["chance_of_playing_next_round"]

    # Availability is applied here rather than learned: no point-in-time history
    # of the flag exists, so it cannot be a training feature.
    availability = out["chance_of_playing_next_round"].fillna(100.0) / 100.0
    out["xp"] = out["xp"] * availability
    out["p_haul"] = out["p_haul"] * availability

    print(f"\nTOP {args.top} BY EXPECTED POINTS   (+/- is Monte Carlo standard error)")
    cols = ["web_name", "position", "price", "xp", "se", "p_haul", "p_return", "has_prior"]
    top = out.nlargest(args.top, "xp").copy()
    top["position"] = top["position"].map(POSITION_NAMES)
    print(top[cols].to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    print(f"\nTOP 8 CAPTAIN PICKS BY P(HAUL)  - the right-tail ranking, not the mean")
    cap = out.nlargest(8, "p_haul").copy()
    cap["position"] = cap["position"].map(POSITION_NAMES)
    print(cap[["web_name", "position", "price", "xp", "se", "p_haul"]].to_string(
        index=False, float_format=lambda v: f"{v:.3f}"))

    # Say plainly when the top of the ranking is inside the noise.
    lead = out.nlargest(2, "xp")
    if len(lead) == 2:
        gap = lead.xp.iloc[0] - lead.xp.iloc[1]
        noise = float(np.hypot(lead.se.iloc[0], lead.se.iloc[1]))
        if gap < 2 * noise:
            print(f"\n  {lead.web_name.iloc[0]} and {lead.web_name.iloc[1]} are separated by "
                  f"{gap:.2f} against a combined standard error of {noise:.2f}.")
            print("  They are not distinguishable at this number of draws - pick on")
            print("  team news and your own read, not on the decimal.")

    print("\nDIFFERENTIALS (under 10% owned)")
    diff = out[out.ownership_percent < 10].nlargest(8, "xp").copy()
    diff["position"] = diff["position"].map(POSITION_NAMES)
    print(diff[["web_name", "position", "price", "ownership_percent", "xp", "p_haul"]]
          .to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    pool = out[out.xp.notna() & out.price.notna()].copy()
    optimizer = SquadOptimizer(
        squad_rules, budget=settings.squad_total_spend / 10,
        club_limit=int(settings.squad_team_limit),
        squad_size=int(settings.squad_squadsize),
        starting_size=int(settings.squad_squadplay))
    squad = optimizer.solve(
        pool,
        # A bench that cannot play is worthless, and a 75%-flagged player is a
        # warning rather than a bargain. Both are enforced, not hoped for.
        bench_weight=0.15,
        min_bench_p_play=0.55,
        availability_floor=0.75,
        # Clean sheets on opposite sides of one match are mutually exclusive.
        fixture_column="fixture_id",
        max_defensive_per_fixture=3,
    )
    xi = squad[squad.starting]
    print(f"\nOPTIMAL SQUAD  cost {squad.price.sum():.1f} / "
          f"{settings.squad_total_spend/10:.0f}   XI xP {xi.xp.sum():.1f}")
    show = squad.copy()
    show["position"] = show["position"].map(POSITION_NAMES)
    show["role"] = np.where(show.is_captain, "(C)",
                            np.where(show.starting, "XI", "bench"))
    print(show[["web_name", "position", "team_id", "price", "xp", "p_haul", "role"]]
          .to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    print("\nCHIP WINDOWS")
    for _, c in chips.iterrows():
        live = "available now" if c.first_gameweek <= gameweek <= c.last_gameweek else ""
        print(f"  {c.chip_name:<10} GW{int(c.first_gameweek):>2}-{int(c.last_gameweek):<2} {live}")

    if args.json_out:
        import json
        payload = {
            "season": args.season,
            "gameweek": int(gameweek),
            "n_sims": args.sims,
            "generated_at": pd.Timestamp.utcnow().isoformat(),
            "prior_coverage": float(frame.has_prior.mean()),
            "budget": float(settings.squad_total_spend / 10),
            "projections": out.sort_values("xp", ascending=False).to_dict("records"),
            "squad": squad.to_dict("records"),
            "chips": chips.to_dict("records"),
            "fixtures": frame.drop_duplicates("fixture_id")[
                ["fixture_id", "kickoff_time", "home_code", "away_code",
                 "fixture_difficulty", "opponent_difficulty"]
            ].to_dict("records"),
        }
        pathlib.Path(args.json_out).write_text(
            json.dumps(payload, indent=1, default=str))
        print(f"\nWrote {args.json_out}")

    if entry_squad is not None and not entry_squad.empty:
        held = set(entry_squad.element)
        print(f"\nYOUR SQUAD ({len(held)} players)")
        mine = out[out.player_id.isin(held)].nlargest(15, "xp").copy()
        mine["position"] = mine["position"].map(POSITION_NAMES)
        print(mine[["web_name", "position", "price", "xp", "p_haul"]].to_string(
            index=False, float_format=lambda v: f"{v:.3f}"))
    elif args.entry:
        print(f"\nNo picks stored for entry {args.entry} at GW{gameweek - 1} - "
              "picks only exist after a gameweek's deadline has passed.")


if __name__ == "__main__":
    main()
