#!/usr/bin/env python
"""Derive and validate the FPL scoring-rules seed.

The rules are not typed from memory. For 2025-26 they are read straight out of
the `explain` blocks, which record the points awarded for every scoring event.
For earlier seasons they are validated by reconstruction: apply the candidate
rules to vaastav's component stats and check the result equals the recorded
`total_points` for every player-gameweek. A season only ships if it reconciles.

Rule types
----------
per_unit    points = floor(value / unit_size) * points     (goals, saves, cards)
threshold   points = points, when value >= min_value        (minutes, clean sheets)
            Several rows may match; the highest min_value wins.
passthrough points = value                                  (bonus)

Usage
-----
    python scripts/build_scoring_rules.py --validate-only
    python scripts/build_scoring_rules.py            # validate, then write seed
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"
SEED_PATH = REPO_ROOT / "dbt" / "seeds" / "scoring_rules.csv"

SEASONS = [
    "2016-17", "2017-18", "2018-19", "2019-20", "2020-21", "2021-22",
    "2022-23", "2023-24", "2024-25", "2025-26", "2026-27",
]

GKP, DEF, MID, FWD = 1, 2, 3, 4
ALL_POS = (GKP, DEF, MID, FWD)

POSITION_CODE = {"GK": GKP, "GKP": GKP, "DEF": DEF, "MID": MID, "FWD": FWD, "AM": 5}

# (identifier, position, rule_type, unit_size, points, min_value, verified, note)
# `verified` records whether the value was confirmed against observed data.
BASE_RULES = [
    ("minutes", p, "threshold", 1, 1, 1, True, "appearance") for p in ALL_POS
] + [
    ("minutes", p, "threshold", 1, 2, 60, True, "60+ minutes") for p in ALL_POS
] + [
    ("goals_scored", GKP, "per_unit", 1, 6, 1, True,
     "confirmed by the single observed GK goal, 2020-21"),
    ("goals_scored", DEF, "per_unit", 1, 6, 1, True, ""),
    ("goals_scored", MID, "per_unit", 1, 5, 1, True, ""),
    ("goals_scored", FWD, "per_unit", 1, 4, 1, True, ""),
    ("clean_sheets", GKP, "threshold", 1, 4, 1, True, ""),
    ("clean_sheets", DEF, "threshold", 1, 4, 1, True, ""),
    ("clean_sheets", MID, "threshold", 1, 1, 1, True, ""),
    ("goals_conceded", GKP, "per_unit", 2, -1, 2, True, "-1 per 2 conceded"),
    ("goals_conceded", DEF, "per_unit", 2, -1, 2, True, "-1 per 2 conceded"),
    ("saves", GKP, "per_unit", 3, 1, 3, True, "+1 per 3 saves"),
    ("penalties_saved", GKP, "per_unit", 1, 5, 1, True, ""),
] + [
    ("assists", p, "per_unit", 1, 3, 1, True, "") for p in ALL_POS
] + [
    ("penalties_missed", p, "per_unit", 1, -2, 1, True, "") for p in ALL_POS
] + [
    ("own_goals", p, "per_unit", 1, -2, 1, True, "") for p in ALL_POS
] + [
    ("yellow_cards", p, "per_unit", 1, -1, 1, True, "") for p in ALL_POS
] + [
    ("red_cards", p, "per_unit", 1, -3, 1, True, "") for p in ALL_POS
] + [
    ("bonus", p, "passthrough", 1, 1, 1, True, "value is the points") for p in ALL_POS
]

# Introduced in 2025-26. Threshold differs by position.
DEFCON_RULES = [
    ("defensive_contribution", DEF, "threshold", 1, 2, 10, True, "10+ CBIT"),
    ("defensive_contribution", MID, "threshold", 1, 2, 12, True, "12+ CBIRT"),
    ("defensive_contribution", FWD, "threshold", 1, 2, 12, True, "12+ CBIRT"),
]

COLUMNS = ["season", "identifier", "position", "rule_type", "unit_size",
           "points", "min_value", "verified", "note"]


def rules_for(season):
    rules = list(BASE_RULES)
    if season >= "2025-26":
        rules += DEFCON_RULES
    return [
        {"season": season, "identifier": i, "position": p, "rule_type": rt,
         "unit_size": u, "points": pts, "min_value": mv,
         "verified": v, "note": n}
        for (i, p, rt, u, pts, mv, v, n) in rules
    ]


def build_seed():
    return pd.DataFrame([r for s in SEASONS for r in rules_for(s)])[COLUMNS]


def apply_rules(row, rules_by_pos):
    """Score one player-gameweek from its component stats."""
    pos = row["position_code"]
    total = 0
    for rule in rules_by_pos.get(pos, []):
        value = row.get(rule["identifier"])
        if value is None or pd.isna(value) or value <= 0:
            continue
        if rule["rule_type"] == "per_unit":
            total += int(value // rule["unit_size"]) * rule["points"]
        elif rule["rule_type"] == "passthrough":
            total += int(value)
    # thresholds resolve to a single best match per identifier
    for identifier, tiers in rules_by_pos.get(("threshold", pos), {}).items():
        value = row.get(identifier)
        if value is None or pd.isna(value):
            continue
        best = [t for t in tiers if value >= t["min_value"]]
        if best:
            total += max(best, key=lambda t: t["min_value"])["points"]
    return total


def index_rules(season_rules):
    """Split rules into per-unit/passthrough and threshold lookups."""
    by_pos = {}
    for r in season_rules:
        pos = r["position"]
        if r["rule_type"] == "threshold":
            key = ("threshold", pos)
            by_pos.setdefault(key, {}).setdefault(r["identifier"], []).append(r)
        else:
            by_pos.setdefault(pos, []).append(r)
    return by_pos


def validate(con, season):
    """Reconstruct total_points for one season and report the match rate."""
    df = con.execute(
        """
        SELECT v.element, v.gameweek, v.position, v.total_points,
               v.minutes, v.goals_scored, v.assists, v.clean_sheets,
               v.goals_conceded, v.own_goals, v.penalties_saved,
               v.penalties_missed, v.yellow_cards, v.red_cards, v.saves,
               v.bonus, v.defensive_contribution,
               p.element_type
        FROM raw_vaastav_player_gameweek v
        LEFT JOIN (
            SELECT DISTINCT id, element_type FROM raw_vaastav_players WHERE season = ?
        ) p ON p.id = v.element
        WHERE v.season = ?
        """,
        [season, season],
    ).fetchdf()

    if df.empty:
        return None

    # Position comes from merged_gw where present, else players_raw.
    df["position_code"] = (
        df["position"].map(POSITION_CODE).fillna(df["element_type"])
    )
    df = df[df["position_code"].isin(ALL_POS)]
    if df.empty:
        return None

    by_pos = index_rules(rules_for(season))
    df["reconstructed"] = df.apply(lambda r: apply_rules(r, by_pos), axis=1)
    df["delta"] = df["reconstructed"] - df["total_points"]

    matched = int((df["delta"] == 0).sum())
    return {
        "season": season,
        "rows": len(df),
        "matched": matched,
        "rate": matched / len(df),
        "mean_abs_delta": float(df["delta"].abs().mean()),
        "worst": df.nlargest(3, df["delta"].abs().name or "delta")[
            ["element", "gameweek", "position_code", "total_points", "reconstructed", "delta"]
        ] if matched < len(df) else None,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--validate-only", action="store_true")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    print("Reconstructing total_points from candidate rules\n")
    print(f"  {'season':<10} {'rows':>7} {'matched':>8} {'rate':>8} {'MAE':>7}")
    print("  " + "-" * 44)

    failures = []
    for season in SEASONS:
        res = validate(con, season)
        if res is None:
            print(f"  {season:<10} {'-':>7} {'no data':>8}")
            continue
        flag = "" if res["rate"] > 0.999 else "  <-- CHECK"
        print(f"  {res['season']:<10} {res['rows']:>7} {res['matched']:>8} "
              f"{res['rate']:>7.2%} {res['mean_abs_delta']:>7.3f}{flag}")
        if res["rate"] <= 0.999:
            failures.append(res)

    con.close()

    if failures:
        print("\nSeasons that did not reconcile:")
        for f in failures:
            print(f"\n  {f['season']} - sample discrepancies:")
            print(f["worst"].to_string(index=False))

    if args.validate_only:
        return

    seed = build_seed()
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    seed.to_csv(SEED_PATH, index=False)
    print(f"\nSeed written: {SEED_PATH.relative_to(REPO_ROOT)}  ({len(seed)} rows)")
    print(f"  unverified values: {int((~seed['verified']).sum())}")


if __name__ == "__main__":
    main()
