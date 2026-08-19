-- Grain: season x team_id x fixture_id
--
-- Rolling team form, attached to every fixture including ones not yet played.
--
-- Two deliberate departures from the old `int_team_stats_rolling`:
--
--   Rolled over matches, not gameweeks. A double gameweek is two observations,
--   which is what it is. The old model rolled over gameweek rows that had
--   themselves been duplicated by a fan-out, so a double consumed two slots of a
--   three-week window and the averages disagreed with each other.
--
--   Leak-free by construction, not by convention. Form is computed only over
--   *finished* matches, then attached with an ASOF join on a strict inequality.
--   A fixture can only ever see matches that kicked off before it, so there is
--   no frame offset to get wrong and no way for an unplayed fixture to dilute a
--   window with nulls.

{% set windows = [3, 5, 10] %}

with finished as (
    select *
    from {{ ref('int_team_fixture') }}
    where finished
      and goals_for is not null
),

-- Form as at the end of each completed match.
rolled as (
    select
        season,
        team_id,
        kickoff_time
        {% for n in windows %}
        , avg(goals_for) over w{{ n }} as avg_goals_for_last{{ n }}
        , avg(goals_against) over w{{ n }} as avg_goals_against_last{{ n }}
        , avg(clean_sheet) over w{{ n }} as clean_sheet_rate_last{{ n }}
        , avg(team_xg) over w{{ n }} as avg_xg_last{{ n }}
        , avg(team_xa) over w{{ n }} as avg_xa_last{{ n }}
        , avg(case when result = 'W' then 1.0 else 0.0 end) over w{{ n }} as win_rate_last{{ n }}
        , count(*) over w{{ n }} as matches_in_window{{ n }}
        {% endfor %}
    from finished
    window
        {% for n in windows %}
        w{{ n }} as (
            partition by season, team_id order by kickoff_time
            rows between {{ n - 1 }} preceding and current row
        ){{ "," if not loop.last }}
        {% endfor %}
)

select
    f.season,
    f.team_id,
    f.fixture_id,
    f.gameweek,
    f.kickoff_time,
    f.opponent_id,
    f.is_home
    {% for n in windows %}
    , r.avg_goals_for_last{{ n }}
    , r.avg_goals_against_last{{ n }}
    , r.clean_sheet_rate_last{{ n }}
    , r.avg_xg_last{{ n }}
    , r.avg_xa_last{{ n }}
    , r.win_rate_last{{ n }}
    , r.matches_in_window{{ n }}
    {% endfor %}
from {{ ref('int_team_fixture') }} f
asof left join rolled r
    on  f.season = r.season
    and f.team_id = r.team_id
    and f.kickoff_time > r.kickoff_time
