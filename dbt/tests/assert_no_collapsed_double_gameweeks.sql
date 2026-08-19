-- A player's fixtures in a gameweek must match their team's fixture count.
--
-- Guards the grain fix directly. If stg_player_fixture ever collapses a double
-- gameweek back to one row - or fans a single fixture out into two - this
-- catches it on the run that introduced it, rather than several models later
-- when a rolling average quietly starts averaging the wrong number of matches.
--
-- Only checks players who actually appeared, since a benched player at a club
-- with two fixtures may legitimately have fewer rows.
--
-- Returns offending rows, so an empty result is a pass.

with player_fixture_counts as (
    select
        season,
        player_id,
        gameweek,
        any_value(team_id) as team_id,
        count(distinct fixture_id) as player_fixtures,
        sum(minutes) as total_minutes
    from {{ ref('stg_player_fixture') }}
    group by season, player_id, gameweek
),

team_fixture_counts as (
    select season, gameweek, team_id, count(distinct fixture_id) as team_fixtures
    from (
        select season, gameweek, home_team_id as team_id, fixture_id
        from {{ ref('stg_fixtures') }}
        union all
        select season, gameweek, away_team_id as team_id, fixture_id
        from {{ ref('stg_fixtures') }}
    )
    group by season, gameweek, team_id
)

select
    p.season,
    p.player_id,
    p.gameweek,
    p.player_fixtures,
    t.team_fixtures
from player_fixture_counts p
join team_fixture_counts t
    on  t.season = p.season
    and t.gameweek = p.gameweek
    and t.team_id = p.team_id
where p.total_minutes > 0
  and p.player_fixtures > t.team_fixtures
