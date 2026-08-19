"""Render a squad as a pitch, the way an FPL manager expects to see it.

A fifteen-row table is accurate and unreadable. A formation shows shape,
captaincy and who is benched at a glance, which is how the decision is actually
made. Shirts are SVG in club colours - see kits.py for why not the official
images.
"""

from kits import kit_for, shirt_svg

PITCH_W, ROW_Y = 940, {1: 46, 2: 168, 3: 292, 4: 416}
BENCH_Y = 596


def _slot(x, y, player, scale=1.0, badge=None):
    body, trim, text = kit_for(player.get("team_code"), player["position"])
    w = 44 * scale
    label_y = y + 40 * scale + 15
    name = str(player.get("web_name", ""))[:14]
    if player.get("value_text") is not None:
        points_text = str(player["value_text"])
    else:
        points = player.get("xp")
        points_text = f"{points:.1f}" if points is not None else ""

    badge_svg = ""
    if badge:
        badge_svg = f"""
      <circle cx="{x + w - 2}" cy="{y + 2}" r="9" fill="#FFFFFF" stroke="#141B17"
              stroke-width="1.2"/>
      <text x="{x + w - 2}" y="{y + 5.5}" text-anchor="middle" font-size="10"
            font-weight="700" fill="#141B17"
            font-family="ui-monospace,Menlo,monospace">{badge}</text>"""

    return f"""{shirt_svg(x, y, player.get("team_code"), player["position"], scale)}
      <rect x="{x - 12}" y="{label_y - 11}" width="{w + 24}" height="15" rx="3"
            fill="#0E1412" fill-opacity=".82"/>
      <text x="{x + w / 2}" y="{label_y}" text-anchor="middle" font-size="10.5"
            fill="#E6EDE7" font-family="ui-sans-serif,system-ui,sans-serif">{name}</text>
      <rect x="{x + w / 2 - 17}" y="{label_y + 4}" width="34" height="14" rx="3"
            fill="#1D6E56"/>
      <text x="{x + w / 2}" y="{label_y + 14.5}" text-anchor="middle" font-size="9.5"
            font-weight="600" fill="#FFFFFF"
            font-family="ui-monospace,Menlo,monospace">{points_text}</text>{badge_svg}"""


def _row(players, y, scale=1.0, captain_id=None, vice_id=None):
    if not players:
        return ""
    spacing = min(150, PITCH_W / (len(players) + 0.6))
    start = (PITCH_W - spacing * (len(players) - 1)) / 2 - 22 * scale
    out = []
    for i, player in enumerate(players):
        badge = ("C" if player["player_id"] == captain_id
                 else "V" if player["player_id"] == vice_id else None)
        out.append(_slot(start + i * spacing, y, player, scale, badge))
    return "".join(out)


def formation_svg(starters, bench, captain_id=None, vice_id=None):
    """`starters` and `bench` are lists of dicts with player_id, web_name,
    position, team_code and xp."""
    height = 680
    rows = "".join(
        _row([p for p in starters if p["position"] == position],
             ROW_Y[position], 1.0, captain_id, vice_id)
        for position in (1, 2, 3, 4)
    )
    bench_row = _row(bench, BENCH_Y, 0.82, captain_id, vice_id)
    shape = "-".join(
        str(sum(1 for p in starters if p["position"] == position))
        for position in (2, 3, 4)
    )

    return f"""
<svg viewBox="0 0 {PITCH_W} {height}" width="100%" role="img"
     aria-label="Squad in a {shape} formation with four substitutes"
     xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto">
  <defs>
    <linearGradient id="turf" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#16352A"/>
      <stop offset="100%" stop-color="#0F2620"/>
    </linearGradient>
  </defs>
  <rect x="0" y="0" width="{PITCH_W}" height="560" rx="10" fill="url(#turf)"/>
  {"".join(f'<rect x="0" y="{n * 70}" width="{PITCH_W}" height="35" fill="#FFFFFF" fill-opacity="0.018"/>' for n in range(8))}
  <rect x="14" y="14" width="{PITCH_W - 28}" height="532" rx="6" fill="none"
        stroke="#FFFFFF" stroke-opacity=".16"/>
  <line x1="14" y1="280" x2="{PITCH_W - 14}" y2="280" stroke="#FFFFFF" stroke-opacity=".12"/>
  <circle cx="{PITCH_W / 2}" cy="280" r="56" fill="none" stroke="#FFFFFF" stroke-opacity=".12"/>
  <rect x="{PITCH_W / 2 - 150}" y="14" width="300" height="82" fill="none"
        stroke="#FFFFFF" stroke-opacity=".12"/>
  <rect x="{PITCH_W / 2 - 150}" y="464" width="300" height="82" fill="none"
        stroke="#FFFFFF" stroke-opacity=".12"/>
  {rows}
  <rect x="0" y="566" width="{PITCH_W}" height="{height - 566}" rx="8"
        fill="#141B17" stroke="#253029"/>
  <text x="16" y="586" font-size="10" fill="#82918A" letter-spacing="1.4"
        font-family="ui-monospace,Menlo,monospace">SUBSTITUTES</text>
  {bench_row}
</svg>"""


STRIP_SLOT_W = 118


def kit_strip_svg(players, scale=0.9):
    """A horizontal row of shirts with a name and a number under each.

    Used for captaincy shortlists, where the decision is a comparison across a
    handful of names and a table makes you read rather than look.
    """
    if not players:
        return ""
    width = STRIP_SLOT_W * len(players)
    height = 108
    shirt_w = 44 * scale

    slots = []
    for i, player in enumerate(players):
        x = i * STRIP_SLOT_W + (STRIP_SLOT_W - shirt_w) / 2
        slots.append(_slot(x, 12, player, scale))

    return f"""
<svg viewBox="0 0 {width} {height}" width="100%" role="img"
     aria-label="{len(players)} captaincy options with projected points"
     xmlns="http://www.w3.org/2000/svg"
     style="max-width:100%;height:auto">
  <rect x="0" y="0" width="{width}" height="{height}" rx="8" fill="#12261F"/>
  {"".join(slots)}
</svg>"""
