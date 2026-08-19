-- The mart must not create, lose or double-count anything.
--
-- Aggregating fct_player_fixture up to gameweek grain has to reproduce the
-- staging totals exactly. This is the end-to-end guard on the grain fix: if a
-- join anywhere between staging and the mart ever fans out again, points and
-- minutes inflate here and the test fails on that run rather than surfacing
-- later as a model that mysteriously scores well in backtest.
--
-- Returns offending rows, so an empty result is a pass.

with source_totals as (
    select
        season,
        count(*) as n_rows,
        sum(minutes) as minutes,
        sum(total_points) as total_points,
        sum(goals_scored) as goals_scored,
        sum(assists) as assists
    from {{ ref('stg_player_fixture') }}
    group by season
),

mart_totals as (
    select
        season,
        count(*) as n_rows,
        sum(target_minutes) as minutes,
        sum(target_total_points) as total_points,
        sum(target_goals_scored) as goals_scored,
        sum(target_assists) as assists
    from {{ ref('fct_player_fixture') }}
    group by season
),

gameweek_totals as (
    select
        season,
        sum(minutes) as minutes,
        sum(total_points) as total_points,
        sum(goals_scored) as goals_scored,
        sum(assists) as assists
    from {{ ref('fct_player_gameweek') }}
    group by season
)

select
    s.season,
    s.n_rows as source_rows,
    m.n_rows as mart_rows,
    s.total_points as source_points,
    m.total_points as mart_points,
    g.total_points as gameweek_points
from source_totals s
join mart_totals m using (season)
join gameweek_totals g using (season)
where s.n_rows <> m.n_rows
   or s.minutes is distinct from m.minutes
   or s.total_points is distinct from m.total_points
   or s.goals_scored is distinct from m.goals_scored
   or s.assists is distinct from m.assists
   or s.minutes is distinct from g.minutes
   or s.total_points is distinct from g.total_points
   or s.goals_scored is distinct from g.goals_scored
   or s.assists is distinct from g.assists
