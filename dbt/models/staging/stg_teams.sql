-- Grain: season x team_id
--
-- Team ids are only unique *within* a season - they are assigned alphabetically
-- each August, so promoted clubs reuse the ids of relegated ones. Every join
-- onto a team must therefore carry the season.

with api as (
    select
        season,
        id as team_id,
        code as team_code,
        name as team_name,
        short_name as team_short_name,
        strength,
        strength_overall_home,
        strength_overall_away,
        strength_attack_home,
        strength_attack_away,
        strength_defence_home,
        strength_defence_away,
        'api' as source,
        coalesce(snapshot_at, ingested_at) as effective_at
    from {{ source('raw_layer', 'raw_teams') }}
),

historical as (
    select
        season,
        id as team_id,
        code as team_code,
        name as team_name,
        short_name as team_short_name,
        strength,
        strength_overall_home,
        strength_overall_away,
        strength_attack_home,
        strength_attack_away,
        strength_defence_home,
        strength_defence_away,
        'vaastav' as source,
        ingested_at as effective_at
    from {{ source('raw_layer', 'raw_vaastav_teams') }}
),

unioned as (
    select * from api
    union all
    select * from historical
)

select
    season,
    team_id,
    team_code,
    team_name,
    team_short_name,
    strength,
    strength_overall_home,
    strength_overall_away,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    source
from unioned
-- Prefer the API lineage where both cover a season, then the latest snapshot.
qualify row_number() over (
    partition by season, team_id
    order by case when source = 'api' then 0 else 1 end, effective_at desc
) = 1
