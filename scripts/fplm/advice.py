"""Turn horizon projections into transfer, captain and chip advice for one squad.

Everything here is scored over the whole horizon rather than the next gameweek.
That is the point: a player with a gentle opener and a brutal run after it should
not be recommended, and the only way to see that is to add up what he is worth
across the run.

A transfer is only advice if it clears a bar. Three separate bars apply:

  free transfer   a banked transfer has option value - team news lands, prices
                  move, a better move appears next week. Spending one on a
                  marginal gain is a small loss, so a free move needs to beat a
                  threshold rather than merely be positive.

  hit             a -4 has to be paid back inside the horizon, so the gain must
                  clear four points plus the same threshold.

  hold            when nothing clears its bar, holding is the recommendation,
                  not a failure to find one.
"""

import numpy as np
import pandas as pd

# Points a free transfer must gain across the horizon before it is worth using.
FREE_TRANSFER_THRESHOLD = 2.0
TRANSFER_HIT = 4.0


def horizon_value(projections, gameweeks=None, decay=0.9):
    """Total projected points per player across the horizon.

    Later gameweeks are discounted slightly - not because they matter less, but
    because a projection five weeks out is less certain than one for Saturday,
    and the plan will be re-run before then anyway.
    """
    frame = projections
    if gameweeks is not None:
        frame = frame[frame.gameweek.isin(gameweeks)]
    frame = frame.copy()
    frame["weight"] = decay ** frame["horizon_offset"]
    frame["weighted_xp"] = frame["xp"] * frame["weight"]

    agg = frame.groupby("player_id").agg(
        web_name=("web_name", "first"),
        position=("position", "first"),
        team_id=("team_id", "first"),
        price=("price", "first"),
        ownership_percent=("ownership_percent", "first"),
        horizon_xp=("xp", "sum"),
        weighted_xp=("weighted_xp", "sum"),
        next_xp=("xp", "first"),
        mean_haul=("p_haul", "mean"),
        best_haul=("p_haul", "max"),
        mean_difficulty=("fixture_difficulty", "mean"),
        blanks=("fixture_cnt", lambda s: int((s == 0).sum())),
        doubles=("fixture_cnt", lambda s: int((s >= 2).sum())),
        is_deputy=("is_deputy", "first"),
        min_role_factor=("role_factor", "min"),
        chance=("chance_of_playing_next_round", "first"),
        news=("news", "first"),
    ).reset_index()
    return agg.sort_values("weighted_xp", ascending=False)


def squad_report(values, squad_ids):
    """Per-player horizon value for a held squad, weakest first."""
    held = values[values.player_id.isin(squad_ids)].copy()
    held = held.sort_values("weighted_xp")
    return held


def transfer_candidates(values, squad_ids, bank=0.0, max_suggestions=6,
                        free_transfers=1):
    """Rank sell/buy pairs by what they gain across the horizon.

    Only same-position swaps are considered, since anything else changes the
    squad's shape and has to be solved as a squad rather than a pair.
    """
    held = values[values.player_id.isin(squad_ids)]
    available = values[~values.player_id.isin(squad_ids)]
    if held.empty:
        return pd.DataFrame()

    rows = []
    for _, out_player in held.iterrows():
        budget = out_player.price + bank
        pool = available[
            (available.position == out_player.position)
            & (available.price <= budget)
        ]
        if pool.empty:
            continue
        best = pool.nlargest(3, "weighted_xp")
        for _, in_player in best.iterrows():
            gain = in_player.weighted_xp - out_player.weighted_xp
            if gain <= 0:
                continue
            rows.append({
                "out_id": out_player.player_id,
                "out": out_player.web_name,
                "out_price": out_player.price,
                "out_xp": out_player.weighted_xp,
                "in_id": in_player.player_id,
                "in": in_player.web_name,
                "in_price": in_player.price,
                "in_xp": in_player.weighted_xp,
                "cost": in_player.price - out_player.price,
                "gain": gain,
                "gain_after_hit": gain - TRANSFER_HIT,
                "in_haul": in_player.best_haul,
                "in_deputy": bool(in_player.is_deputy),
                "out_deputy": bool(out_player.is_deputy),
            })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values("gain", ascending=False)
    # One suggestion per player being sold - the alternatives are noise.
    return out.drop_duplicates("out_id").head(max_suggestions)


def transfer_verdict(candidates, free_transfers=1,
                     threshold=FREE_TRANSFER_THRESHOLD):
    """Should the manager transfer, take a hit, or hold?"""
    if candidates.empty:
        return {"action": "hold", "reason":
                "No same-position upgrade improves the horizon at all."}

    best = candidates.iloc[0]
    if best.gain >= threshold:
        return {
            "action": "transfer",
            "move": f"{best['out']} -> {best['in']}",
            "gain": float(best.gain),
            "reason": (f"Gains {best.gain:.1f} points across the horizon, above the "
                       f"{threshold:.0f}-point bar for spending a free transfer."),
        }
    if best.gain_after_hit >= threshold:
        return {
            "action": "transfer_with_hit",
            "move": f"{best['out']} -> {best['in']}",
            "gain": float(best.gain_after_hit),
            "reason": (f"Gains {best.gain:.1f} points, still {best.gain_after_hit:.1f} "
                       "after a -4."),
        }
    return {
        "action": "hold",
        "move": f"{best['out']} -> {best['in']}",
        "gain": float(best.gain),
        "reason": (f"The best available move gains only {best.gain:.1f} points across "
                   f"the horizon, below the {threshold:.0f}-point bar. Bank the transfer - "
                   "team news and price changes will surface something better, and you "
                   "can hold up to five."),
    }


def captain_advice(projections, squad_ids, gameweek):
    """Captain from within the squad, for the next gameweek only."""
    gw = projections[(projections.gameweek == gameweek)
                     & projections.player_id.isin(squad_ids)]
    if gw.empty:
        return None
    by_points = gw.nlargest(3, "xp")[["web_name", "xp", "se", "p_haul"]]
    by_tail = gw.nlargest(3, "p_haul")[["web_name", "xp", "se", "p_haul"]]
    top = by_points.iloc[0]
    second = by_points.iloc[1] if len(by_points) > 1 else None
    tied = False
    if second is not None:
        noise = float(np.hypot(top.se, second.se))
        tied = (top.xp - second.xp) < 2 * noise
    return {"by_points": by_points, "by_tail": by_tail, "tied": tied}


CHIP_NAMES = {
    "wildcard": "Wildcard",
    "freehit": "Free Hit",
    "bboost": "Bench Boost",
    "3xc": "Triple Captain",
}


def chip_advice(projections, chips, squad_ids, gameweek):
    """Whether any chip is worth playing now, judged over the horizon.

    Each chip has two windows a season, one per half. Only the window covering
    the gameweek in hand is worth reporting - the other is months away and
    listing it twice reads as a bug.
    """
    notes = []
    squad = projections[projections.player_id.isin(squad_ids)]

    relevant = chips[chips.last_gameweek >= gameweek].sort_values("first_gameweek")
    relevant = relevant.drop_duplicates("chip_name")

    for _, chip in relevant.iterrows():
        label = CHIP_NAMES.get(chip.chip_name, chip.chip_name)
        live = chip.first_gameweek <= gameweek <= chip.last_gameweek
        if not live:
            notes.append({"chip": label, "verdict": "not yet",
                          "detail": f"Opens in GW{int(chip.first_gameweek)}."})
            continue

        if chip.chip_name == "bboost":
            per_gw = squad.groupby("gameweek")["xp"].apply(
                lambda s: s.nsmallest(4).sum())
            best_gw = per_gw.idxmax() if len(per_gw) else None
            value = per_gw.max() if len(per_gw) else 0.0
            notes.append({
                "chip": label,
                "verdict": "hold" if value < 12 else f"consider GW{best_gw}",
                "detail": (f"Best bench in the horizon is worth {value:.1f} points "
                           f"(GW{best_gw}). Under about 12 it is better saved for a "
                           "double gameweek."),
            })
        elif chip.chip_name == "3xc":
            best = squad.loc[squad.groupby("gameweek")["xp"].idxmax()] if len(squad) else None
            top = best.nlargest(1, "xp").iloc[0] if best is not None and len(best) else None
            notes.append({
                "chip": label,
                "verdict": "hold" if top is None or top.p_haul < 0.25
                           else f"consider GW{int(top.gameweek)}",
                "detail": (f"Best captain option peaks at {top.p_haul:.1%} haul "
                           f"probability ({top.web_name}, GW{int(top.gameweek)}). "
                           "Below roughly 25% a double gameweek is the better use."
                           if top is not None else "No squad projection available."),
            })
        elif chip.chip_name in ("wildcard", "freehit"):
            weak = squad.groupby("player_id")["xp"].sum().nsmallest(5).sum()
            notes.append({
                "chip": label,
                "verdict": "hold" if weak > 8 else "consider",
                "detail": (f"Your five weakest players are worth {weak:.1f} points "
                           "across the horizon. A wildcard earns its place when that "
                           "number is low and several moves are needed at once."),
            })
    return pd.DataFrame(notes)
