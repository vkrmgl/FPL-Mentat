-- Component points must sum to the recorded gameweek total.
--
-- This is the load-bearing check for the decomposition architecture: if the
-- `explain` extraction is wrong, every component model trains on a corrupted
-- target and nothing downstream would notice. Currently passes for 27,228 of
-- 27,228 player-gameweeks in 2025-26.
--
-- Returns offending rows, so an empty result is a pass.

with component_totals as (
    select
        season,
        player_id,
        gameweek,
        sum(points) as component_points
    from {{ ref('stg_player_fixture_points') }}
    group by season, player_id, gameweek
),

recorded as (
    select
        season,
        player_id,
        gameweek,
        cast(total_points as double) as total_points
    from {{ source('raw_layer', 'raw_player_gameweek') }}
    qualify row_number() over (
        partition by season, player_id, gameweek order by ingested_at desc
    ) = 1
)

select
    r.season,
    r.player_id,
    r.gameweek,
    r.total_points,
    coalesce(c.component_points, 0) as component_points,
    coalesce(c.component_points, 0) - r.total_points as delta
from recorded r
left join component_totals c
    on  c.season = r.season
    and c.player_id = r.player_id
    and c.gameweek = r.gameweek
where coalesce(c.component_points, 0) <> r.total_points
