select
    pg.player_id,
    pg.gameweek,
    coalesce(c.correct_team_id, p.team_id) as team_id,
    p.position,
    p.corners_and_indirect_freekicks_order as corners_order,
    p.direct_freekicks_order as fk_order,
    p.penalties_order as penalty_order
from {{ ref('stg_player_gameweek') }} pg
left join {{ ref('stg_players') }} p
    on pg.player_id = p.player_id
left join {{ ref('player_team_corrections') }} c
    on pg.player_id = c.player_id
    and pg.gameweek between c.gameweek_start and c.gameweek_end
