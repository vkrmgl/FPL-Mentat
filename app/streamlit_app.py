"""FPL-Mentat: projections and squad advice.

Two things, both reading `fct_player_horizon` rather than calling a model:
projections for every player across the next few gameweeks, and advice for one
manager's squad. Everything is scored over the whole horizon, so a player with a
kind opener and a brutal run afterwards ranks where he belongs.
"""

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from formation import formation_svg, kit_strip_svg  # noqa: E402
from fplm.advice import (  # noqa: E402
    captain_advice, chip_advice, horizon_value, squad_report,
    transfer_candidates, transfer_verdict,
)
from fplm.chips import build_policies, recommend  # noqa: E402
from fplm.optimize import SquadOptimizer  # noqa: E402

DB = REPO_ROOT / "data" / "fpl-mentat.duckdb"


def _best_fifteen(values, squad_rules):
    """The highest-projecting legal fifteen, from the same MILP the reports use.

    Before the first deadline the FPL API publishes picks for nobody, so there is
    no squad to advise on. Rather than leave the whole section inert, the squad
    is filled with the optimiser's own answer - which is what a manager without a
    team actually wants to see.
    """
    pool = values[values.horizon_xp.notna() & values.price.notna()].copy()
    pool["xp"] = pool["weighted_xp"]
    optimizer = SquadOptimizer(squad_rules)
    return optimizer.solve(
        pool,
        bench_weight=0.15,
        min_bench_p_play=0.0,   # p_play is per-gameweek and absent from horizon totals
        availability_floor=0.75,
    )


POSITIONS = {1: "Goalkeepers", 2: "Defenders", 3: "Midfielders", 4: "Forwards"}
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15) FPL-Mentat"}

st.set_page_config(page_title="FPL-Mentat", page_icon="⚽", layout="wide")


@st.cache_data(ttl=900)
def load():
    con = duckdb.connect(str(DB), read_only=True)
    horizon = con.execute("SELECT * FROM fct_player_horizon").fetchdf()
    teams = con.execute(
        "SELECT team_id, team_code, team_name, team_short_name FROM stg_teams "
        "WHERE season = ?", [horizon.season.iloc[0]]).fetchdf()
    squad_rules = con.execute(
        "SELECT * FROM stg_squad_rules WHERE season = ?",
        [horizon.season.iloc[0]]).fetchdf()
    chips = con.execute(
        "SELECT chip_name, first_gameweek, last_gameweek FROM stg_chips WHERE season = ?",
        [horizon.season.iloc[0]]).fetchdf()
    policies, opportunities, dgw_prior = build_policies(con)
    con.close()
    horizon = horizon.merge(teams, on="team_id", how="left")
    return horizon, teams, chips, squad_rules, policies, opportunities, dgw_prior


@st.cache_data(ttl=600)
def fetch_squad(entry_id, gameweek):
    """A manager's squad, live from the FPL API.

    Picks only exist once a gameweek's deadline has passed, so before the season
    opens this legitimately returns nothing.
    """
    for gw in range(gameweek - 1, 0, -1):
        r = requests.get(
            f"https://fantasy.premierleague.com/api/entry/{entry_id}/event/{gw}/picks/",
            headers=UA, timeout=20)
        if r.status_code == 200:
            data = r.json()
            return ([p["element"] for p in data.get("picks", [])],
                    gw, data.get("entry_history", {}))
    return [], None, {}


@st.cache_data(ttl=600)
def fetch_entry(entry_id):
    r = requests.get(f"https://fantasy.premierleague.com/api/entry/{entry_id}/",
                     headers=UA, timeout=20)
    return r.json() if r.status_code == 200 else None


horizon, teams, chips, squad_rules, policies, opportunities, dgw_prior = load()
gameweeks = sorted(horizon.gameweek.unique())


st.title("FPL-Mentat")

# Controls sit above the tables rather than alongside them. Beside a tall table
# they end up in the path of a page scroll, and a Streamlit slider under the
# cursor swallows the wheel - scrolling the list silently moved the horizon and
# changed every number on screen without saying so.
# The control row is pinned to the foot of the window so the sliders stay
# reachable while reading the tables. `fixed` rather than `sticky`: the controls
# sit at the top of the document, and a sticky element only stays visible while
# its own parent is on screen, so a sticky-bottom rule would scroll away
# immediately. :has() is what lets the rule find the block, since Streamlit gives
# its containers no stable class of their own.
st.markdown("""
<style>
div[data-testid="stVerticalBlock"] > div:has(div.pinned-controls) {
    position: fixed;
    left: 0;
    right: 0;
    bottom: 0;
    z-index: 999;
    padding: 0.55rem clamp(1rem, 6vw, 5.5rem) 0.85rem;
    background: rgba(14, 20, 18, 0.97);
    backdrop-filter: blur(8px);
    border-top: 1px solid rgba(255, 255, 255, 0.12);
}
/* Labels are part of the widget, so they travel with it - this only stops the
   bar clipping them. */
div[data-testid="stVerticalBlock"] > div:has(div.pinned-controls) label {
    font-size: 0.78rem;
    opacity: 0.85;
    margin-bottom: 0.1rem;
}
/* Fixed positioning takes the bar out of flow, so the page needs room at the
   foot or the last rows hide underneath it. */
div[data-testid="stMainBlockContainer"] { padding-bottom: 9rem; }
</style>
""", unsafe_allow_html=True)

with st.container():
    st.markdown('<div class="pinned-controls"></div>', unsafe_allow_html=True)
    control_left, control_right = st.columns(2)
    with control_left:
        window = st.slider("Gameweeks ahead", 1, len(gameweeks), 1)
    with control_right:
        max_price = st.slider("Max price", 3.5, 16.0, 16.0, 0.5)

selected = gameweeks[:window]
# `values` spans the selected window and is what both halves of the page use,
# so the squad numbers and the projections above can never describe different
# ranges. `view` is the same thing with the price filter applied.
values = horizon_value(horizon, gameweeks=selected).merge(
    teams[["team_id", "team_code", "team_short_name"]], on="team_id", how="left")
view = values[values.price <= max_price]

# ---------------------------------------------------------------- projections
st.header("Projections")

columns = st.columns(4)
for column, (position, label) in zip(columns, POSITIONS.items()):
    chunk = view[view.position == position].sort_values(
        "horizon_xp", ascending=False)
    with column:
        st.markdown(f"**{label}**")
        if chunk.empty:
            st.caption("Nothing matches the filters.")
            continue
        st.dataframe(
            pd.DataFrame({
                "#": range(1, len(chunk) + 1),
                "Player": chunk.web_name.to_numpy(),
            }),
            width="stretch", height=430, hide_index=True,
        )

st.divider()

# ---------------------------------------------------------------------- squad
st.header("My squad")
with st.container():
    entry_id = st.text_input(
        "FPL team ID",
        help="The number in your team's URL: fantasy.premierleague.com/entry/<ID>/event/1",
    )

    squad_ids, bank = [], 0.0
    entry = None

    if entry_id.strip() and not entry_id.strip().isdigit():
        st.error("A team ID is all digits.")
    elif entry_id.strip():
        entry = fetch_entry(int(entry_id.strip()))
        if entry is None:
            st.error(f"No FPL team found with ID {entry_id.strip()}.")
        else:
            st.success(
                f"**{entry.get('name')}** — {entry.get('player_first_name')} "
                f"{entry.get('player_last_name')}"
            )
            squad_ids, from_gw, entry_history = fetch_squad(
                int(entry_id.strip()), gameweeks[0])
            if squad_ids:
                bank = entry_history.get("bank", 0) / 10.0
                st.caption(f"Squad as at GW{from_gw} · bank £{bank:.1f}m")

    # Before the first deadline the FPL API publishes no picks for anyone, so
    # the advice would be unreachable exactly when it is most wanted. Filling
    # with the optimiser's own fifteen keeps the section useful, and it is
    # replaced by the real squad the moment one exists.
    auto_filled = False
    if not squad_ids:
        picked = _best_fifteen(values, squad_rules)
        squad_ids = picked.player_id.tolist()
        auto_filled = True
        st.info(
            "No squad is published for this gameweek yet — the FPL API only releases "
            "picks once a deadline has passed. Showing the highest-projecting legal "
            "fifteen instead. Enter a team ID above once the season is under way."
            if entry is None else
            "Your picks are not published yet, so this is the highest-projecting legal "
            "fifteen. It switches to your own squad once a deadline has passed."
        )

    if squad_ids:
        held = squad_report(values, squad_ids)
        candidates = transfer_candidates(values, squad_ids, bank=bank)
        verdict = transfer_verdict(candidates)

        icon = {"hold": "⏸", "transfer": "✅", "transfer_with_hit": "⚠️"}
        st.header(f"{icon.get(verdict['action'], '')} "
                  f"{verdict['action'].replace('_', ' ').title()}")
        if verdict.get("move"):
            st.markdown(f"**{verdict['move']}**")
        st.write(verdict["reason"])

        st.subheader("Squad")
        # `held` already carries team_code, inherited from `values` - merging
        # teams in again suffixes both copies and loses the column.
        pitch = held.sort_values("weighted_xp", ascending=False)

        # Pick a legal eleven greedily from the best available, then bench the
        # rest. Formation limits come from the same rules table the optimiser
        # uses, so the shape on screen is always one FPL would accept.
        limits = squad_rules.set_index("position")
        starters, counts = [], {1: 0, 2: 0, 3: 0, 4: 0}
        for row in pitch.itertuples():
            position = int(row.position)
            if len(starters) >= 11:
                break
            if counts[position] >= int(limits.loc[position, "max_playing"]):
                continue
            remaining = 11 - len(starters) - 1
            shortfall = sum(
                max(0, int(limits.loc[other, "min_playing"]) - counts[other])
                for other in (1, 2, 3, 4) if other != position
            )
            if shortfall > remaining:
                continue
            starters.append(row.player_id)
            counts[position] += 1

        def _slots(frame):
            return [{
                "player_id": int(r.player_id), "web_name": r.web_name,
                "position": int(r.position),
                "team_code": None if pd.isna(r.team_code) else int(r.team_code),
                "xp": float(r.horizon_xp),
            } for r in frame.itertuples()]

        on_pitch = pitch[pitch.player_id.isin(starters)]
        benched = pitch[~pitch.player_id.isin(starters)]
        captain = on_pitch.nlargest(1, "weighted_xp").player_id.iloc[0]
        vice = (on_pitch.nlargest(2, "weighted_xp").player_id.iloc[1]
                if len(on_pitch) > 1 else None)

        # components.v1.html rather than st.html: the latter sanitises markup and
        # strips the SVG entirely, leaving a silent blank where the pitch should be.
        components.html(
            f'<div style="background:#0E1412">'
            f'{formation_svg(_slots(on_pitch), _slots(benched), captain_id=int(captain), vice_id=None if vice is None else int(vice))}'
            f'</div>',
            height=700,
        )
        shape = "-".join(str(counts[p]) for p in (2, 3, 4))
        st.caption(
            f"{shape} · £{pitch.price.sum():.1f} · projected "
            f"{on_pitch.horizon_xp.sum():.1f} across GW{selected[0]}–{selected[-1]}. "
            "The number under each shirt is that player's horizon total; C is the "
            "captain, V the vice."
        )

        if not candidates.empty:
            st.subheader("Transfers worth considering")
            st.caption(
                "Ranked by points gained across the whole horizon, so a player with one "
                "kind fixture and a hard run afterwards will not appear here."
            )
            st.dataframe(pd.DataFrame({
                "Out": candidates.out + candidates.out_deputy.map(
                    {True: " (deputy)", False: ""}),
                "In": candidates["in"],
                "Cost": candidates.cost.round(1),
                "Gain": candidates.gain.round(2),
                "After -4": candidates.gain_after_hit.round(2),
                "In haul": candidates.in_haul.round(3),
            }), width="stretch", hide_index=True)

        next_gw = horizon[(horizon.gameweek == gameweeks[0])
                          & horizon.player_id.isin(squad_ids)]
        if not next_gw.empty:
            next_gw = next_gw.merge(teams[["team_id", "team_code"]], on="team_id",
                                    how="left", suffixes=("", "_t"))
            code_column = "team_code" if "team_code" in next_gw else "team_code_t"

            def _strip(frame, show="points"):
                return [{
                    "player_id": int(r.player_id), "web_name": r.web_name,
                    "position": int(r.position),
                    "team_code": None if pd.isna(getattr(r, code_column))
                                 else int(getattr(r, code_column)),
                    "xp": float(r.xp),
                    "value_text": (f"{r.p_haul:.0%}" if show == "haul"
                                   else f"{r.xp:.1f}"),
                } for r in frame.itertuples()]

            st.subheader(f"Captain options for GW{gameweeks[0]}")
            st.markdown("**Ranked by expected points**")
            components.html(
                f'<div style="background:#0E1412">'
                f'{kit_strip_svg(_strip(next_gw.nlargest(7, "xp")))}</div>',
                height=130)
            st.markdown("**Ranked by chance of a haul** — the figure is P(10+ points)")
            components.html(
                f'<div style="background:#0E1412">'
                f'{kit_strip_svg(_strip(next_gw.nlargest(7, "p_haul"), show="haul"))}</div>',
                height=130)

            lead = next_gw.nlargest(2, "xp")
            if len(lead) == 2:
                noise = float(np.hypot(lead.se.iloc[0], lead.se.iloc[1]))
                if (lead.xp.iloc[0] - lead.xp.iloc[1]) < 2 * noise:
                    st.info(
                        "The top two are inside the simulation's own error, so expected "
                        "points cannot separate them. Take the better haul chance if you "
                        "are chasing, either if you are not."
                    )

        st.subheader("Chips")

        # Chip values for the coming gameweek, from this squad's own projections.
        gw_now = gameweeks[0]
        squad_gw = horizon[(horizon.gameweek == gw_now)
                           & horizon.player_id.isin(squad_ids)].sort_values(
                               "xp", ascending=False)
        pool_gw = horizon[horizon.gameweek == gw_now]

        bench_value = float(squad_gw.xp.tail(4).sum())
        captain_value = float(squad_gw.xp.head(1).sum())
        free_hit_value = float(
            pool_gw.nlargest(11, "xp").xp.sum() - squad_gw.head(11).xp.sum())

        current = {"bboost": bench_value, "3xc": captain_value,
                   "freehit": free_hit_value}
        windows = chips.set_index("chip_name")

        cards = []
        for chip_name, value in current.items():
            if chip_name not in windows.index:
                continue
            row = windows.loc[chip_name]
            row = row.iloc[0] if isinstance(row, pd.DataFrame) else row
            if gw_now < row.first_gameweek:
                cards.append((chip_name, "not yet",
                              f"Opens in GW{int(row.first_gameweek)}.", None))
                continue
            upcoming = list(range(gw_now + 1, int(row.last_gameweek) + 1))
            call = recommend(policies, chip_name, value, gw_now,
                             int(row.last_gameweek), upcoming)
            cards.append((chip_name, call["verdict"], call["reason"], call))

        pretty = {"bboost": "Bench Boost", "3xc": "Triple Captain",
                  "freehit": "Free Hit", "wildcard": "Wildcard"}
        for chip_name, verdict, reason, call in cards:
            icon = "✅" if verdict == "play now" else "⏸"
            with st.container(border=True):
                left, right = st.columns([1, 3])
                with left:
                    st.markdown(f"**{icon} {pretty.get(chip_name, chip_name)}**")
                    if call:
                        st.metric("This week", f"{call['value_now']:.1f}",
                                  delta=f"{call['margin']:+.1f} vs bar",
                                  delta_color="normal")
                with right:
                    st.write(reason)
                    if call:
                        st.caption(
                            f"Bar is the expected best of the {call['weeks_left']} "
                            f"gameweeks left in the window, drawn from ten seasons of "
                            f"history and lifted where a double gameweek is likely. "
                            f"This week sits at the {call['percentile']:.0%} percentile "
                            "of what this chip has historically been worth."
                        )

        with st.expander("How these calls are made"):
            st.markdown(
                "A chip is a one-shot option with an expiry, so the question is not "
                "*is this good* but *is this better than the best week still to come*. "
                "The bar is the expected maximum of the remaining opportunities, "
                "estimated from what each chip was actually worth in every gameweek "
                "since 2020-21 — squads ranked on form known beforehand, then scored "
                "on what happened, so no hindsight. Gameweeks with a history of "
                "becoming doubles are weighted up, since doubles are where chips pay "
                "and they are not on the fixture list when the decision is made.\n\n"
                "The bar falls as the window closes. Bench Boost needs about 29 points "
                "in GW1 and nothing at all in GW19, because an unplayed chip scores "
                "zero. That adaptation is the whole point — a fixed threshold cannot "
                "refuse a mediocre week early and accept the same week late."
            )

    st.divider()
    st.caption(
        "Projections come from simulating each fixture: appearance and minutes "
        "models, a Dixon-Coles scoreline draw, per-90 component rates, and the "
        "season's own scoring rules. Players only starting because of an injury "
        "elsewhere are projected down over the horizon. Pre-season friendlies are "
        "not modelled — the FPL API does not publish them."
    )
