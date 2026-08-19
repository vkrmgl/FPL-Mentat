#!/usr/bin/env python
"""Raw-layer ingestion for FPL-Mentat.

Usage
-----
    python scripts/ingest.py                       # bootstrap + fixtures + live
    python scripts/ingest.py --gameweeks 1-5       # a specific range
    python scripts/ingest.py --entry 1234567       # add manager data
    python scripts/ingest.py --league 314          # add league standings
    python scripts/ingest.py --element-summaries   # slow: one call per player

Safe to re-run. Landing is append-only and deduplicated by payload hash, so a
second run on the same day is a no-op while a genuine change lands a new
version. Nothing is ever dropped or overwritten.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fplm.api import FPLClient, derive_season  # noqa: E402
from fplm.ingest import (  # noqa: E402
    ingest_bootstrap,
    ingest_element_summaries,
    ingest_entry,
    ingest_fixtures,
    ingest_league,
    ingest_live,
)
from fplm.warehouse import Warehouse  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"


def parse_gameweeks(spec):
    """Parse '1-5', '3', or '1,4,9-11' into a sorted list of ints."""
    out = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-")
            out.update(range(int(lo), int(hi) + 1))
        elif part:
            out.add(int(part))
    return sorted(out)


def report(table, n):
    """Format one landing result. Zero rows can mean 'no change' or 'no data',
    so say neither - the audit log records which it was."""
    suffix = "  (no new rows)" if n == 0 else ""
    return f"      {table:<26} {n:>6} rows{suffix}"


def playable_gameweeks(bootstrap):
    """Gameweeks worth pulling live data for: finished, or currently running."""
    return [
        e["id"]
        for e in bootstrap["events"]
        if e.get("finished") or e.get("is_current") or e.get("data_checked")
    ]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB), help="DuckDB path")
    ap.add_argument("--season", help="Override the derived season label, e.g. 2026-27")
    ap.add_argument("--gameweeks", help="Gameweeks for live data, e.g. '1-38' or '5'")
    ap.add_argument("--entry", type=int, action="append", default=[],
                    help="Manager entry id (repeatable)")
    ap.add_argument("--league", type=int, action="append", default=[],
                    help="Classic league id (repeatable)")
    ap.add_argument("--element-summaries", action="store_true",
                    help="Pull per-player histories (one request per player)")
    ap.add_argument("--skip-live", action="store_true", help="Skip /event/{gw}/live/")
    args = ap.parse_args()

    client = FPLClient()
    wh = Warehouse(args.db)

    print(f"[db] {args.db}")
    print("[1/5] bootstrap-static ...")
    bootstrap = client.bootstrap()
    season = args.season or derive_season(bootstrap)
    run_id = wh.start_run(season, notes=" ".join(sys.argv[1:]) or "default run")
    print(f"      season={season}  run_id={run_id}")

    try:
        _, results, bootstrap = ingest_bootstrap(client, wh, season)
        for table, n in results.items():
            print(report(table, n))

        print("[2/5] fixtures ...")
        for table, n in ingest_fixtures(client, wh, season).items():
            print(report(table, n))

        if args.skip_live:
            print("[3/5] live data ... skipped")
        else:
            gws = parse_gameweeks(args.gameweeks) if args.gameweeks else playable_gameweeks(bootstrap)
            if not gws:
                print("[3/5] live data ... no finished or current gameweeks yet")
            else:
                print(f"[3/5] live data for gameweeks {gws[0]}-{gws[-1]} ({len(gws)} calls) ...")
                res = ingest_live(client, wh, season, gws)
                covered = res.pop("gameweeks_covered", [])
                for table, n in res.items():
                    print(f"      {table:<26} {n:>6} rows")
                print(f"      gameweeks with data: {len(covered)}")

        if args.element_summaries:
            ids = [e["id"] for e in bootstrap["elements"]]
            print(f"[4/5] element summaries for {len(ids)} players (slow) ...")
            for table, n in ingest_element_summaries(client, wh, season, ids).items():
                print(f"      {table:<26} {n:>6} rows")
        else:
            print("[4/5] element summaries ... skipped (use --element-summaries)")

        if args.entry or args.league:
            print("[5/5] manager and league data ...")
            gws = parse_gameweeks(args.gameweeks) if args.gameweeks else playable_gameweeks(bootstrap)
            for entry_id in args.entry:
                for table, n in ingest_entry(client, wh, season, entry_id, gws).items():
                    print(f"      entry {entry_id} {table:<20} {n:>6} rows")
            for league_id in args.league:
                for table, n in ingest_league(client, wh, season, league_id).items():
                    print(f"      league {league_id} {table:<19} {n:>6} rows")
        else:
            print("[5/5] manager and league data ... skipped (use --entry / --league)")

        wh.finish_run("success")
        print("-----------------------------")
        print(f"Raw layer ingestion complete  [{season}]")
    except Exception:
        wh.finish_run("failed")
        raise
    finally:
        wh.close()


if __name__ == "__main__":
    main()
