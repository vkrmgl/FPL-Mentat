-- Grain: season x team_id
--
-- Team ids are reassigned alphabetically each August, so a promoted club
-- inherits a relegated club's id. Every join onto this must carry the season.
--
-- Sourced from the fixture list rather than the team reference, because the
-- reference is incomplete - vaastav ships no teams.csv for 2018-19 - and any
-- club that played a match belongs in the dimension whether or not a reference
-- row exists.

with played as (
    select distinct season, team_id
    from {{ ref('int_team_fixture') }}
)

select
    p.season,
    p.team_id,
    t.team_code,
    t.team_name,
    t.team_short_name,
    t.strength,
    t.strength_overall_home,
    t.strength_overall_away,
    t.strength_attack_home,
    t.strength_attack_away,
    t.strength_defence_home,
    t.strength_defence_away,
    t.team_name is null as reference_missing
from played p
left join {{ ref('stg_teams') }} t
    on  t.season = p.season
    and t.team_id = p.team_id
