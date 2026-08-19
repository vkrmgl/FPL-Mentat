-- Grain: season x team_id x fixture_id
--
-- Every fixture from both teams' points of view: one row per team per match.
-- This is the long form the Dixon-Coles fit and the simulator both consume, and
-- it is what lets opponent context be joined without the double self-join that
-- fanned out in the old pipeline.
--
-- Team-level xG is summed from player rows rather than taken from a team feed,
-- since that is the only source available across all seasons.

with sides as (
    select
        season,
        fixture_id,
        gameweek,
        kickoff_time,
        home_team_id as team_id,
        away_team_id as opponent_id,
        true as is_home,
        home_goals as goals_for,
        away_goals as goals_against,
        home_difficulty as difficulty,
        away_difficulty as opponent_difficulty,
        finished
    from {{ ref('stg_fixtures') }}

    union all

    select
        season,
        fixture_id,
        gameweek,
        kickoff_time,
        away_team_id as team_id,
        home_team_id as opponent_id,
        false as is_home,
        away_goals as goals_for,
        home_goals as goals_against,
        away_difficulty as difficulty,
        home_difficulty as opponent_difficulty,
        finished
    from {{ ref('stg_fixtures') }}
),

-- Aggregate the players who appeared for each team in each fixture.
player_side as (
    select
        season,
        fixture_id,
        team_id,
        sum(expected_goals) as team_xg,
        sum(expected_assists) as team_xa,
        sum(bps) as team_bps,
        sum(minutes) as team_minutes,
        count(*) filter (where minutes > 0) as players_used
    from {{ ref('stg_player_fixture') }}
    group by season, fixture_id, team_id
)

select
    s.season,
    s.team_id,
    -- FPL's team `code` is stable across seasons; `team_id` is reassigned every
    -- August, so id 3 has been Bournemouth, Burnley, Brentford and Brighton.
    -- Any model fitted across seasons must key on the code or it blends
    -- unrelated clubs into one set of attack and defence ratings.
    tc.team_code,
    oc.team_code as opponent_code,
    s.fixture_id,
    s.gameweek,
    s.kickoff_time,
    s.opponent_id,
    s.is_home,
    s.goals_for,
    s.goals_against,
    s.difficulty,
    s.opponent_difficulty,
    s.finished,

    case when s.goals_against = 0 then 1 else 0 end as clean_sheet,
    case
        when s.goals_for > s.goals_against then 'W'
        when s.goals_for = s.goals_against then 'D'
        when s.goals_for < s.goals_against then 'L'
    end as result,

    p.team_xg,
    p.team_xa,
    p.team_bps,
    p.players_used,

    -- Rest since this team's previous match. Captures fixture congestion
    -- directly, and is the free half of the answer to midweek European games -
    -- the other half needs a source the FPL API does not expose.
    date_diff(
        'day',
        lag(s.kickoff_time) over (partition by s.season, s.team_id order by s.kickoff_time),
        s.kickoff_time
    ) as days_since_last_match

from sides s
left join {{ ref('stg_teams') }} tc
    on  tc.season = s.season
    and tc.team_id = s.team_id
left join {{ ref('stg_teams') }} oc
    on  oc.season = s.season
    and oc.team_id = s.opponent_id
left join player_side p
    on  p.season = s.season
    and p.fixture_id = s.fixture_id
    and p.team_id = s.team_id
