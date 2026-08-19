-- Grain: season x fixture_id
--
-- Conformed fixture dimension. Team names are resolved on both sides so
-- downstream consumers do not each re-join dim_team twice.

select
    f.season,
    f.fixture_id,
    f.gameweek,
    f.kickoff_time,
    f.home_team_id,
    f.away_team_id,
    h.team_name as home_team_name,
    a.team_name as away_team_name,
    f.home_goals,
    f.away_goals,
    f.home_difficulty,
    f.away_difficulty,
    f.finished
from {{ ref('stg_fixtures') }} f
left join {{ ref('dim_team') }} h
    on  h.season = f.season
    and h.team_id = f.home_team_id
left join {{ ref('dim_team') }} a
    on  a.season = f.season
    and a.team_id = f.away_team_id
