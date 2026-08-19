#!/usr/bin/env python
"""Backfill historical seasons from vaastav/Fantasy-Premier-League.

Why a separate source
---------------------
The FPL API only serves the current season, so anything before 2026-27 has to
come from an archive. This repository has per-gameweek player rows from 2016-17
onward, already merged with Understat xG.

It is complementary to - not a replacement for - the `raw_log` recovery in
`recover_history.py`:

  * only `raw_log` has the `explain` block, i.e. per-component points
    attribution, which is the target for the decomposition models;
  * only vaastav has per-gameweek `value` (price), `selected` (ownership),
    transfer flow, and FPL's own `xP`, which is a free public benchmark.

Landed into `raw_vaastav_*` tables rather than merged into the API-sourced
tables, so provenance stays explicit and staging can union the two on purpose.

Usage
-----
    python scripts/backfill_vaastav.py --seasons 2025-26 --dry-run
    python scripts/backfill_vaastav.py --from-season 2019-20
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

from fplm.warehouse import Warehouse  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"
BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"

ALL_SEASONS = [
    "2016-17", "2017-18", "2018-19", "2019-20", "2020-21",
    "2021-22", "2022-23", "2023-24", "2024-25", "2025-26",
]

# file within the season directory -> destination table
SEASON_FILES = {
    "gws/merged_gw.csv": "raw_vaastav_player_gameweek",
    "players_raw.csv": "raw_vaastav_players",
    "teams.csv": "raw_vaastav_teams",
    "fixtures.csv": "raw_vaastav_fixtures",
}


def fetch(season, filename):
    url = f"{BASE}/{season}/{filename}"
    try:
        return pd.read_csv(url, encoding_errors="replace", low_memory=False)
    except Exception as exc:  # noqa: BLE001 - older seasons are missing files
        print(f"      {filename:<22} unavailable ({type(exc).__name__})")
        return None


def backfill_season(wh, season, dry_run):
    print(f"  [{season}]")
    landed = {}
    for filename, table in SEASON_FILES.items():
        df = fetch(season, filename)
        if df is None or df.empty:
            continue

        # `round` is reserved-ish and inconsistent across seasons; normalise the
        # gameweek key so downstream models do not have to special-case it.
        if "round" in df.columns and "gameweek" not in df.columns:
            df = df.rename(columns={"round": "gameweek"})
        df["source"] = "vaastav"

        print(f"      {filename:<22} {len(df):>6} rows")
        landed[table] = len(df)
        if not dry_run:
            wh.land(df, table, season)
            wh.log(table, f"vaastav:{season}/{filename}", "success", season,
                   200, row_count=len(df))
    return landed


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--seasons", help="comma-separated, e.g. 2023-24,2024-25")
    ap.add_argument("--from-season", help="backfill this season and everything after it")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.seasons:
        seasons = [s.strip() for s in args.seasons.split(",")]
    elif args.from_season:
        if args.from_season not in ALL_SEASONS:
            ap.error(f"unknown season {args.from_season}; expected one of {ALL_SEASONS}")
        seasons = ALL_SEASONS[ALL_SEASONS.index(args.from_season):]
    else:
        seasons = ALL_SEASONS

    wh = Warehouse(args.db)
    wh.start_run("backfill", notes=f"vaastav backfill: {','.join(seasons)}")
    mode = "DRY RUN - nothing will be written" if args.dry_run else "writing"
    print(f"[db]   {args.db}\n[mode] {mode}\n[seasons] {len(seasons)}: {seasons[0]}..{seasons[-1]}\n")

    totals = {}
    try:
        for season in seasons:
            for table, n in backfill_season(wh, season, args.dry_run).items():
                totals[table] = totals.get(table, 0) + n
        wh.finish_run("dry-run" if args.dry_run else "success")
    except Exception:
        wh.finish_run("failed")
        raise
    finally:
        wh.close()

    print("\n-----------------------------")
    for table, n in sorted(totals.items()):
        print(f"{table:<32} {n:>7} rows")
    print("Backfill complete")


if __name__ == "__main__":
    main()
