"""Endpoint-by-endpoint ingestion into the raw layer.

Table naming is deliberate: every table is `raw_<entity>` at its natural grain.
Two of them are new and matter a lot for the decomposition model:

  raw_player_fixture_points
      One row per player / fixture / scoring identifier, unnested from the
      `explain` block of /event/{gw}/live/. This is the component-level target
      (minutes, goals_scored, assists, clean_sheets, saves, bonus, ...) *and*
      it is per-fixture, so double gameweeks are no longer collapsed.

  raw_fixture_stats
      One row per fixture / identifier / side / player, unnested from the
      `stats` block of /fixtures/. Independent attribution of the same events.
"""

import pandas as pd

from .api import derive_season

# bootstrap-static sub-payloads that are plain lists of records
BOOTSTRAP_TABLES = {
    "teams": "raw_teams",
    "elements": "raw_players",
    "events": "raw_events",
    "element_types": "raw_element_types",
    "chips": "raw_chips",
    "phases": "raw_phases",
    "element_stats": "raw_element_stats",
}


def ingest_bootstrap(client, wh, season=None):
    """Land every list in /bootstrap-static/, plus game_settings as one row."""
    payload = client.bootstrap()
    season = season or derive_season(payload)
    results = {}

    for key, table in BOOTSTRAP_TABLES.items():
        records = payload.get(key) or []
        if not records:
            wh.log(table, "/bootstrap-static/", "empty", season, 200, row_count=0)
            results[table] = 0
            continue
        df = pd.json_normalize(records)
        results[table] = wh.land_if_changed(
            df, table, season, records, "/bootstrap-static/"
        )

    # game_settings is a flat dict, not a list - keep it as a single wide row
    settings = payload.get("game_settings")
    if settings:
        df = pd.json_normalize([settings])
        results["raw_game_settings"] = wh.land_if_changed(
            df, "raw_game_settings", season, settings, "/bootstrap-static/"
        )

    return season, results, payload


def ingest_fixtures(client, wh, season):
    """Land all fixtures in one call, plus the per-fixture stats attribution."""
    fixtures = client.fixtures()
    if not fixtures:
        wh.log("raw_fixtures", "/fixtures/", "empty", season, 200, row_count=0)
        return {"raw_fixtures": 0, "raw_fixture_stats": 0}

    flat = pd.json_normalize(fixtures)
    n_fix = wh.land_if_changed(flat, "raw_fixtures", season, fixtures, "/fixtures/")

    # Unnest stats -> one row per fixture / identifier / side / player.
    stat_rows = []
    for fx in fixtures:
        for stat in fx.get("stats") or []:
            for side in ("h", "a"):
                for entry in stat.get(side) or []:
                    stat_rows.append(
                        {
                            "fixture_id": fx["id"],
                            "gameweek": fx.get("event"),
                            "identifier": stat.get("identifier"),
                            "side": side,
                            "player_id": entry.get("element"),
                            "value": entry.get("value"),
                        }
                    )

    n_stats = 0
    if stat_rows:
        n_stats = wh.land_if_changed(
            pd.DataFrame(stat_rows), "raw_fixture_stats", season,
            stat_rows, "/fixtures/ (stats)",
        )
    else:
        wh.log("raw_fixture_stats", "/fixtures/ (stats)", "empty", season, 200, row_count=0)

    return {"raw_fixtures": n_fix, "raw_fixture_stats": n_stats}


def parse_live_payload(payload, gameweek):
    """Split one /event/{gw}/live/ body into (player stats df, explain rows).

    Kept separate from fetching so the historical recovery in
    `scripts/recover_history.py` can replay archived payloads through exactly
    this code and land a schema identical to the live path.
    """
    elements = payload.get("elements") or []
    if not elements:
        return None, []

    stats = pd.json_normalize([{"player_id": e["id"], **e["stats"]} for e in elements])
    stats["gameweek"] = gameweek

    explain_rows = [
        {
            "player_id": e["id"],
            "gameweek": gameweek,
            "fixture_id": block.get("fixture"),
            "identifier": s.get("identifier"),
            "points": s.get("points"),
            "value": s.get("value"),
            "points_modification": s.get("points_modification"),
        }
        for e in elements
        for block in (e.get("explain") or [])
        for s in (block.get("stats") or [])
    ]
    return stats, explain_rows


def ingest_live(client, wh, season, gameweeks):
    """Land per-gameweek player stats and the per-fixture points breakdown."""
    stat_frames, explain_rows = [], []
    covered = []

    for gw in gameweeks:
        payload = client.live(gw)
        stats, explains = parse_live_payload(payload, gw)
        if stats is None:
            wh.log(f"raw_player_gameweek_gw{gw}", f"/event/{gw}/live/", "empty",
                   season, 200, row_count=0)
            continue

        covered.append(gw)
        stat_frames.append(stats)
        explain_rows.extend(explains)

        # Log the full body per gameweek. This is what let us recover 2025-26
        # per-fixture data after the API rolled over - worth the disk.
        wh.log(f"raw_player_gameweek_gw{gw}", f"/event/{gw}/live/", "success",
               season, 200, payload=payload, row_count=len(payload["elements"]))

    results = {"raw_player_gameweek": 0, "raw_player_fixture_points": 0,
               "gameweeks_covered": covered}

    if stat_frames:
        combined = pd.concat(stat_frames, ignore_index=True)
        results["raw_player_gameweek"] = wh.land(combined, "raw_player_gameweek", season)
    if explain_rows:
        results["raw_player_fixture_points"] = wh.land(
            pd.DataFrame(explain_rows), "raw_player_fixture_points", season
        )

    return results


def ingest_element_summaries(client, wh, season, player_ids):
    """Per-player fixture history and prior-season totals.

    One request per player, so this is the slow part of a run - call it when
    you need `history_past`, not on every weekly refresh.
    """
    history, past, upcoming = [], [], []

    for pid in player_ids:
        payload = client.element_summary(pid)
        for row in payload.get("history") or []:
            history.append({"player_id": pid, **row})
        for row in payload.get("history_past") or []:
            past.append({"player_id": pid, **row})
        for row in payload.get("fixtures") or []:
            upcoming.append({"player_id": pid, **row})

    results = {}
    for rows, table in [
        (history, "raw_player_fixture_history"),
        (past, "raw_player_season_history"),
        (upcoming, "raw_player_upcoming_fixtures"),
    ]:
        if rows:
            results[table] = wh.land(pd.json_normalize(rows), table, season)
        else:
            wh.log(table, "/element-summary/", "empty", season, 200, row_count=0)
            results[table] = 0
    return results


def ingest_entry(client, wh, season, entry_id, gameweeks=()):
    """Manager-level data: profile, history, chips, transfers and squad picks."""
    results = {}

    profile = client.entry(entry_id)
    if profile:
        flat = pd.json_normalize({k: v for k, v in profile.items() if k != "leagues"})
        results["raw_entry"] = wh.land(flat, "raw_entry", season)

        leagues = profile.get("leagues") or {}
        league_rows = [
            {"entry_id": entry_id, "league_kind": kind, **lg}
            for kind, items in leagues.items()
            if isinstance(items, list)
            for lg in items
        ]
        if league_rows:
            results["raw_entry_league"] = wh.land(
                pd.json_normalize(league_rows), "raw_entry_league", season
            )

    history = client.entry_history(entry_id)
    if history:
        for key, table in [
            ("current", "raw_entry_gameweek"),
            ("past", "raw_entry_past_season"),
            ("chips", "raw_entry_chip"),
        ]:
            rows = [{"entry_id": entry_id, **r} for r in (history.get(key) or [])]
            if rows:
                results[table] = wh.land(pd.json_normalize(rows), table, season)

    transfers = client.entry_transfers(entry_id)
    if transfers:
        rows = [{"entry_id": entry_id, **r} for r in transfers]
        results["raw_entry_transfer"] = wh.land(
            pd.json_normalize(rows), "raw_entry_transfer", season
        )

    pick_rows = []
    for gw in gameweeks:
        payload = client.entry_picks(entry_id, gw)
        if not payload:
            continue  # 404 before the deadline has passed
        for p in payload.get("picks") or []:
            pick_rows.append(
                {
                    "entry_id": entry_id,
                    "gameweek": gw,
                    "active_chip": payload.get("active_chip"),
                    **{f"entry_{k}": v for k, v in (payload.get("entry_history") or {}).items()},
                    **p,
                }
            )
    if pick_rows:
        results["raw_entry_pick"] = wh.land(
            pd.json_normalize(pick_rows), "raw_entry_pick", season
        )

    return results


def ingest_league(client, wh, season, league_id, max_pages=5):
    """Classic league membership - the rival entry_ids behind 'win your league'.

    Members appear in one of two places depending on the time of year. Before a
    league has scored anything, everyone sits in `new_entries` and `standings`
    is empty; once play starts they move across. Both are read, because the
    pre-season window is the only chance to record a roster before gameweek 1.
    """
    rows, meta = [], None

    for page in range(1, max_pages + 1):
        payload = client.league_standings(league_id, page=page)
        if not payload:
            break
        meta = meta or payload.get("league")

        standings = (payload.get("standings") or {}).get("results") or []
        rows.extend({"league_id": league_id, "membership": "standing", **r}
                    for r in standings)

        if page == 1:
            joiners = (payload.get("new_entries") or {}).get("results") or []
            rows.extend({"league_id": league_id, "membership": "new_entry", **r}
                        for r in joiners)

        if not (payload.get("standings") or {}).get("has_next"):
            break

    results = {}
    if meta:
        results["raw_league"] = wh.land(pd.json_normalize([meta]), "raw_league", season)

    if not rows:
        wh.log("raw_league_standing", f"/leagues-classic/{league_id}/standings/",
               "empty", season, 200, row_count=0)
        results["raw_league_standing"] = 0
        return results

    results["raw_league_standing"] = wh.land(
        pd.json_normalize(rows), "raw_league_standing", season
    )
    return results
