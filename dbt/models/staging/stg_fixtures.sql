-- Grain: season x fixture_id
--
-- Fixture ids, like team ids, are only unique within a season.
--
-- Coverage note: the API lineage covers 2025-26 (recovered) and 2026-27 onward;
-- the vaastav lineage covers 2018-19..2025-26. 2016-17 and 2017-18 have player
-- rows but no fixture reference, so team-level models start at 2018-19.

with api as (
    select
        season,
        id as fixture_id,
        event as gameweek,
        cast(kickoff_time as timestamp) as kickoff_time,
        team_h as home_team_id,
        team_a as away_team_id,
        team_h_score as home_goals,
        team_a_score as away_goals,
        team_h_difficulty as home_difficulty,
        team_a_difficulty as away_difficulty,
        finished,
        'api' as source,
        ingested_at as effective_at
    from {{ source('raw_layer', 'raw_fixtures') }}
),

historical as (
    select
        season,
        id as fixture_id,
        event as gameweek,
        cast(kickoff_time as timestamp) as kickoff_time,
        team_h as home_team_id,
        team_a as away_team_id,
        team_h_score as home_goals,
        team_a_score as away_goals,
        team_h_difficulty as home_difficulty,
        team_a_difficulty as away_difficulty,
        finished,
        'vaastav' as source,
        ingested_at as effective_at
    from {{ source('raw_layer', 'raw_vaastav_fixtures') }}
),

unioned as (
    select * from api
    union all
    select * from historical
)

select
    season,
    fixture_id,
    gameweek,
    kickoff_time,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    home_difficulty,
    away_difficulty,
    finished,
    source
from unioned
where gameweek is not null  -- fixtures not yet assigned to a gameweek
qualify row_number() over (
    partition by season, fixture_id
    order by case when source = 'api' then 0 else 1 end, effective_at desc
) = 1
