"""Kit colours for rendering a squad as a formation.

FPL publishes shirt images, but they sit behind a CDN the app cannot rely on
offline, so shirts are drawn as SVG with each club's primary, secondary and trim
colours. Keyed on FPL's `team_code`, which follows a club across seasons -
`team_id` is reassigned every August and would repoint at a different club.

Goalkeepers get their own palette, since almost every club's keeper wears
something unrelated to the outfield kit.
"""

# team_code -> (shirt, sleeves/trim, text)
KITS = {
    3:  ("#EF0107", "#FFFFFF", "#FFFFFF"),  # Arsenal
    7:  ("#95BFE5", "#670E36", "#FFFFFF"),  # Aston Villa
    91: ("#DA291C", "#000000", "#FFFFFF"),  # Bournemouth
    94: ("#E30613", "#FFFFFF", "#FFFFFF"),  # Brentford
    36: ("#0057B8", "#FFFFFF", "#FFFFFF"),  # Brighton
    8:  ("#034694", "#FFFFFF", "#FFFFFF"),  # Chelsea
    9:  ("#78D0F3", "#FFFFFF", "#0A2240"),  # Coventry
    31: ("#1B458F", "#C4122E", "#FFFFFF"),  # Crystal Palace
    11: ("#003399", "#FFFFFF", "#FFFFFF"),  # Everton
    54: ("#FFFFFF", "#000000", "#111111"),  # Fulham
    88: ("#F5A11B", "#000000", "#111111"),  # Hull City
    40: ("#3A64A3", "#FFFFFF", "#FFFFFF"),  # Ipswich
    2:  ("#FFFFFF", "#1D428A", "#111111"),  # Leeds
    14: ("#C8102E", "#00B2A9", "#FFFFFF"),  # Liverpool
    43: ("#6CABDD", "#1C2C5B", "#FFFFFF"),  # Man City
    1:  ("#DA291C", "#000000", "#FFFFFF"),  # Man Utd
    4:  ("#241F20", "#FFFFFF", "#FFFFFF"),  # Newcastle
    17: ("#DD0000", "#FFFFFF", "#FFFFFF"),  # Nott'm Forest
    6:  ("#FFFFFF", "#132257", "#111111"),  # Spurs
    56: ("#EB172B", "#FFFFFF", "#FFFFFF"),  # Sunderland
}

GOALKEEPER_KIT = ("#39B54A", "#1E6B2A", "#FFFFFF")
FALLBACK_KIT = ("#6B7A70", "#3A453E", "#FFFFFF")


def kit_for(team_code, position):
    if position == 1:
        return GOALKEEPER_KIT
    return KITS.get(int(team_code) if team_code is not None else -1, FALLBACK_KIT)


def shirt_svg(x, y, team_code, position, scale=1.0):
    """One shirt, drawn as a path. `x`,`y` is the top-left of the shirt."""
    body, trim, _ = kit_for(team_code, position)
    w, h = 44 * scale, 40 * scale
    return f"""
    <g transform="translate({x},{y})">
      <path d="M {0.22*w} 0 L {0.36*w} {0.10*h} Q {0.5*w} {0.20*h} {0.64*w} {0.10*h}
               L {0.78*w} 0 L {w} {0.20*h} L {0.86*w} {0.38*h} L {0.82*w} {0.30*h}
               L {0.82*w} {h} L {0.18*w} {h} L {0.18*w} {0.30*h} L {0.14*w} {0.38*h}
               L 0 {0.20*h} Z"
            fill="{body}" stroke="{trim}" stroke-width="{1.6*scale}"
            stroke-linejoin="round"/>
      <path d="M {0.36*w} {0.10*h} Q {0.5*w} {0.24*h} {0.64*w} {0.10*h}"
            fill="none" stroke="{trim}" stroke-width="{1.6*scale}"/>
    </g>"""
