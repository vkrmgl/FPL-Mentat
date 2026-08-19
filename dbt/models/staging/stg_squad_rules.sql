-- Grain: season x position
--
-- Squad composition constraints for the optimizer.
--
-- Sourced from the API where available, and from a seed everywhere else.
-- `raw_element_types` only exists for seasons ingested live - it was never part
-- of the original ingest script, so it could not be recovered for 2025-26 from
-- the archived payloads the way teams and fixtures were.
--
-- The gap is not cosmetic. An empty rules frame drops every position constraint
-- from the optimizer without erroring, and it will happily return a squad with
-- six midfielders - a team that looks entirely plausible in a table and would be
-- rejected by the game. The optimizer now refuses to build without complete
-- rules, and this model makes sure it never has to.
--
-- Note: the filter and the dedupe are split across two CTEs rather than combined
-- as WHERE + QUALIFY. DuckDB 1.5.1 raises a spurious internal binder error
-- ("inequal types VARCHAR != BIGINT" on a column that is VARCHAR everywhere)
-- when an IN-list filter sits alongside QUALIFY in one block.

with ranked as (
    select
        season,
        id as position,
        singular_name_short as position_short,
        singular_name as position_name,
        squad_select as squad_count,
        squad_min_play as min_playing,
        squad_max_play as max_playing,
        element_count as available_players,
        'api' as source,
        row_number() over (
            partition by season, id order by ingested_at desc
        ) as rn
    from {{ source('raw_layer', 'raw_element_types') }}
),

api as (
    select
        season, position, position_short, position_name,
        squad_count, min_playing, max_playing, available_players, source
    from ranked
    where rn = 1
      and position in (1, 2, 3, 4)  -- exclude Assistant Manager, not a squad slot
),

historical as (
    select
        season,
        position,
        position_short,
        position_name,
        squad_count,
        min_playing,
        max_playing,
        cast(null as bigint) as available_players,
        'seed' as source
    from {{ ref('squad_rules_history') }}
    where season not in (select distinct season from api)
)

select * from api
union all
select * from historical
