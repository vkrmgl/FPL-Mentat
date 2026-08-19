-- Grain: season x team_id x gameweek
--
-- A complete spine: every team, every gameweek, whether or not they play. This
-- is the model that makes absence explicit.
--
-- The old `int_fixture_ticker` was built by aggregating fixtures, so a blank
-- gameweek produced no row at all. Two bugs followed from that:
--
--   * `had_blank_gw` tested `fixture_cnt = 0`, but blanks arrived as NULL from
--     the left join, so the flag was 0 for all 27,555 rows and the feature was
--     dead weight.
--   * `next_gw_fixture_cnt` used `lead()` over a table with missing rows, so it
--     skipped straight past a blank to the following gameweek - meaning the
--     feature keyed on to decide double-gameweek handling was wrong precisely
--     when it mattered.
--
-- Cross-joining teams against gameweeks fixes both: blanks are real rows with
-- fixture_cnt = 0, and lead() walks a dense sequence.

-- Built on int_gameweek_spine rather than stg_deadlines: deadlines only exist
-- for seasons ingested from the live API, and the historical seasons need the
-- same blank/double/congestion features to be usable as training data.
--
-- Teams come from the fixture list rather than stg_teams, because the team
-- reference is itself incomplete - vaastav ships no teams.csv for 2018-19. Any
-- club that played a match that season belongs in the spine whether or not a
-- reference row exists for it.
with season_teams as (
    select distinct season, team_id
    from {{ ref('int_team_fixture') }}
),

team_gameweeks as (
    select
        t.season,
        t.team_id,
        d.gameweek,
        d.gameweek_start as deadline_time,
        d.days_since_prev_gameweek
    from season_teams t
    join {{ ref('int_gameweek_spine') }} d
        on d.season = t.season
),

fixtures_played as (
    select
        season,
        team_id,
        gameweek,
        count(*) as fixture_cnt,
        count(*) filter (where is_home) as home_fixture_cnt,
        avg(difficulty) as avg_difficulty,
        min(difficulty) as min_difficulty,
        max(difficulty) as max_difficulty,
        min(kickoff_time) as first_kickoff,
        max(kickoff_time) as last_kickoff,
        min(days_since_last_match) as min_days_rest
    from {{ ref('int_team_fixture') }}
    group by season, team_id, gameweek
),

joined as (
    select
        tg.season,
        tg.team_id,
        tg.gameweek,
        tg.deadline_time,
        tg.days_since_prev_gameweek,
        coalesce(f.fixture_cnt, 0) as fixture_cnt,
        coalesce(f.home_fixture_cnt, 0) as home_fixture_cnt,
        f.avg_difficulty,
        f.min_difficulty,
        f.max_difficulty,
        f.first_kickoff,
        f.last_kickoff,
        f.min_days_rest
    from team_gameweeks tg
    left join fixtures_played f
        on  f.season = tg.season
        and f.team_id = tg.team_id
        and f.gameweek = tg.gameweek
)

select
    *,
    fixture_cnt = 0 as is_blank_gameweek,
    fixture_cnt >= 2 as is_double_gameweek,

    -- Correct now that blanks occupy a row: a team blanking next week reports 0
    -- rather than the count of whenever they next happen to play.
    lead(fixture_cnt) over (
        partition by season, team_id order by gameweek
    ) as next_gw_fixture_cnt,
    lag(fixture_cnt) over (
        partition by season, team_id order by gameweek
    ) as prev_gw_fixture_cnt,

    -- Trailing match load, for rotation and fatigue features.
    sum(fixture_cnt) over (
        partition by season, team_id order by gameweek
        rows between 2 preceding and current row
    ) as fixtures_last_3_gameweeks,
    sum(fixture_cnt) over (
        partition by season, team_id order by gameweek
        rows between 4 preceding and current row
    ) as fixtures_last_5_gameweeks,

    -- A gap of twelve days or more between deadlines is an international break.
    -- Derived from the calendar alone, so it needs no external source.
    coalesce(days_since_prev_gameweek >= 12, false) as follows_international_break

from joined
