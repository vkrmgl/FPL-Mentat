#!/usr/bin/env python
"""Recover the 2025-26 season into the current raw schema from `raw_log`.

Why this exists
---------------
The FPL API only ever serves the current season - `/event/{gw}/live/` returns
an empty `elements` list for every gameweek once the season rolls over. The
2025-26 data can therefore never be refetched.

It is recoverable anyway because the original ingest script logged the *entire*
response body to `raw_log`, not just the columns it parsed. That archive still
holds the `explain` block, which the old `raw_gw_data` table dropped. Replaying
it gives us something the old pipeline never had: per-fixture, per-component
rows, so double gameweeks are no longer collapsed into a single row.

Usage
-----
    # Read the archived warehouse, write into a fresh season-aware one
    python scripts/recover_history.py \
        --source-db data/backups/fpl-mentat_2025-26_20260818.duckdb \
        --db data/fpl-mentat.duckdb --dry-run

Source and destination are separate so the old warehouse is never mutated: it
is the only copy of the archive and is read strictly read-only.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from fplm.ingest import parse_live_payload  # noqa: E402
from fplm.warehouse import Warehouse  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"
SEASON = "2025-26"

# The original script logged under these names; map them onto current tables.
LEGACY_EVENT_TABLES = {
    "events.raw_teams": "raw_teams",
    "events.raw_players": "raw_players",
    "events.raw_events": "raw_events",
}


def carry_over_log(src_con, wh, dry_run):
    """Copy the archived audit log into the new warehouse.

    The payload bodies are the reason this recovery was possible at all, so
    they travel with the working warehouse rather than living only in a backup.
    """
    n = src_con.execute("SELECT count(*) FROM raw_log").fetchone()[0]
    print(f"  raw_log (archive)          {n:>6} rows")
    if dry_run:
        return
    df = src_con.execute(
        """
        SELECT payload, response_code, table_name, timestamp, status
        FROM raw_log ORDER BY id
        """
    ).fetchdf()
    df["ingest_run_id"] = wh.run_id
    df["season"] = SEASON
    df["endpoint"] = "archive:2025-26"
    wh.con.register("_log_df", df)
    try:
        wh.con.execute(
            """
            INSERT INTO raw_log(payload, response_code, table_name, timestamp, status,
                                ingest_run_id, season, endpoint)
            SELECT payload, response_code, table_name, timestamp, status,
                   ingest_run_id, season, endpoint FROM _log_df
            """
        )
    finally:
        wh.con.unregister("_log_df")


def latest_payloads(con, pattern):
    """Latest successful payload per logged table_name matching `pattern`.

    The original ingest ran seven times over the season, so every gameweek has
    seven archived bodies. The last one is the settled version - bonus points
    and provisional stats have finished changing by then.
    """
    return con.execute(
        """
        WITH ranked AS (
            SELECT table_name, payload, timestamp,
                   row_number() OVER (PARTITION BY table_name ORDER BY timestamp DESC) AS rn
            FROM raw_log
            WHERE status = 'success' AND payload IS NOT NULL AND table_name LIKE ?
        )
        SELECT table_name, payload FROM ranked WHERE rn = 1 ORDER BY table_name
        """,
        [pattern],
    ).fetchall()


def recover_live(src_con, wh, dry_run):
    """Rebuild player-gameweek stats and per-fixture points from archived bodies."""
    rows = latest_payloads(src_con, "gw_data_gw%")
    stat_frames, explain_rows, covered = [], [], []

    for table_name, payload in rows:
        gw = int(table_name.replace("gw_data_gw", ""))
        stats, explains = parse_live_payload(json.loads(payload), gw)
        if stats is None:
            continue
        covered.append(gw)
        stat_frames.append(stats)
        explain_rows.extend(explains)

    if not stat_frames:
        print("  no archived live payloads found")
        return

    stats_df = pd.concat(stat_frames, ignore_index=True)
    explain_df = pd.DataFrame(explain_rows)

    dgw = (
        explain_df.groupby(["player_id", "gameweek"])["fixture_id"].nunique().gt(1).sum()
        if len(explain_df) else 0
    )
    print(f"  gameweeks recovered      : {len(covered)} ({min(covered)}-{max(covered)})")
    print(f"  raw_player_gameweek      : {len(stats_df)} rows")
    print(f"  raw_player_fixture_points: {len(explain_df)} rows")
    print(f"  player-fixture pairs     : {explain_df[['player_id','gameweek','fixture_id']].drop_duplicates().shape[0]}")
    print(f"  double-gameweek instances: {dgw}")

    if not dry_run:
        wh.land(stats_df, "raw_player_gameweek", SEASON)
        wh.land(explain_df, "raw_player_fixture_points", SEASON)


def recover_bootstrap(src_con, wh, dry_run):
    """Rebuild teams / players / events, preserving every archived snapshot.

    Unlike the live data these are landed once per archived run rather than
    deduplicated to the latest, because the run-to-run differences *are* the
    data: they are the only record of in-season price, availability and
    set-piece-order changes.
    """
    for legacy, table in LEGACY_EVENT_TABLES.items():
        rows = src_con.execute(
            """
            SELECT payload, timestamp FROM raw_log
            WHERE status = 'success' AND payload IS NOT NULL AND table_name = ?
            ORDER BY timestamp
            """,
            [legacy],
        ).fetchall()
        if not rows:
            print(f"  {table:<26} no archived payloads")
            continue

        total = 0
        for payload, ts in rows:
            df = pd.json_normalize(json.loads(payload))
            df["snapshot_at"] = ts
            total += len(df)
            if not dry_run:
                wh.land(df, table, SEASON)
        print(f"  {table:<26} {total:>6} rows across {len(rows)} snapshots")


def recover_fixtures(src_con, wh, dry_run):
    """Rebuild fixtures from the per-gameweek archived payloads."""
    rows = latest_payloads(src_con, "fixtures_gw%")
    frames = []
    for _, payload in rows:
        data = json.loads(payload)
        if data:
            frames.append(pd.json_normalize(data))
    if not frames:
        print("  raw_fixtures               no archived payloads")
        return

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"])
    print(f"  raw_fixtures               {len(df):>6} rows")
    if not dry_run:
        wh.land(df, "raw_fixtures", SEASON)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB), help="destination warehouse")
    ap.add_argument("--source-db", required=True,
                    help="archived warehouse holding raw_log (opened read-only)")
    ap.add_argument("--dry-run", action="store_true", help="report without writing")
    args = ap.parse_args()

    if Path(args.source_db).resolve() == Path(args.db).resolve():
        ap.error("--source-db and --db must differ; the archive is never written to")

    src_con = duckdb.connect(args.source_db, read_only=True)
    wh = Warehouse(args.db)
    wh.start_run(SEASON, notes=f"historical recovery from {args.source_db}")
    mode = "DRY RUN - nothing will be written" if args.dry_run else "writing"
    print(f"[source] {args.source_db} (read-only)\n[db]     {args.db}\n[mode]   {mode}\n")

    try:
        print("[1/4] live gameweek data (stats + explain)")
        recover_live(src_con, wh, args.dry_run)
        print("\n[2/4] bootstrap snapshots (teams / players / events)")
        recover_bootstrap(src_con, wh, args.dry_run)
        print("\n[3/4] fixtures")
        recover_fixtures(src_con, wh, args.dry_run)
        print("\n[4/4] audit log carry-over")
        carry_over_log(src_con, wh, args.dry_run)
        wh.finish_run("dry-run" if args.dry_run else "success")
        print("\n-----------------------------")
        print(f"Recovery complete [{SEASON}]")
    except Exception:
        wh.finish_run("failed")
        raise
    finally:
        src_con.close()
        wh.close()


if __name__ == "__main__":
    main()
