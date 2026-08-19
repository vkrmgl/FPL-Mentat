-- Grain: season x player_id x fixture_id
--
-- Rolling player form, expressed as per-90 rates over the last N *appearances*.
--
-- Two changes from the old `int_player_rolling`, both of which matter:
--
--   Windows count appearances, not gameweeks. Under the old definition a player
--   returning from a five-week injury carried an average diluted by five zero
--   rows, so his form looked terrible exactly when he was about to start again.
--   Absence is a separate signal and belongs in int_player_availability, not
--   smuggled into a scoring rate.
--
--   Rates are per 90 minutes rather than per match. A substitute who scores in
--   20 minutes and a starter who scores in 90 are not the same observation, and
--   the minutes model is what converts a rate back into an expected total.
--
-- Leak-free by construction: form is computed over appearances only, then
-- attached with an ASOF join on a strict inequality, so a fixture can only see
-- matches that kicked off before it.

{% set windows = [3, 5, 10] %}
{% set rate_stats = [
    'goals_scored', 'assists', 'expected_goals', 'expected_assists',
    'expected_goal_involvements', 'bps', 'saves', 'defensive_contribution',
    'clearances_blocks_interceptions', 'recoveries', 'tackles',
    'influence', 'creativity', 'threat', 'bonus'
] %}

with appearances as (
    select
        season,
        player_id,
        fixture_id,
        kickoff_time,
        minutes,
        total_points,
        clean_sheets,
        goals_conceded
        {% for stat in rate_stats %}
        , {{ stat }}
        {% endfor %}
    from {{ ref('stg_player_fixture') }}
    where minutes > 0
),

rolled as (
    select
        season,
        player_id,
        kickoff_time
        {% for n in windows %}
        -- Per-90 rates: total events over total minutes in the window, so a
        -- window mixing cameos and full games weights them correctly.
        {% for stat in rate_stats %}
        , sum({{ stat }}) over w{{ n }}
            / nullif(sum(minutes) over w{{ n }} / 90.0, 0) as {{ stat }}_p90_last{{ n }}
        {% endfor %}
        , avg(minutes) over w{{ n }} as avg_minutes_last{{ n }}
        , avg(total_points) over w{{ n }} as avg_points_last{{ n }}
        , avg(clean_sheets) over w{{ n }} as clean_sheet_rate_last{{ n }}
        , avg(goals_conceded) over w{{ n }} as avg_goals_conceded_last{{ n }}
        , count(*) over w{{ n }} as appearances_in_window{{ n }}
        {% endfor %}
        , stddev_samp(minutes) over w5 as minutes_volatility_last5
    from appearances
    window
        {% for n in windows %}
        w{{ n }} as (
            partition by season, player_id order by kickoff_time
            rows between {{ n - 1 }} preceding and current row
        ),
        {% endfor %}
        w5v as (
            partition by season, player_id order by kickoff_time
            rows between 4 preceding and current row
        )
)

select
    f.season,
    f.player_id,
    f.fixture_id,
    f.gameweek,
    f.kickoff_time,
    f.team_id,
    f.opponent_team_id,
    f.is_home
    {% for n in windows %}
    {% for stat in rate_stats %}
    , r.{{ stat }}_p90_last{{ n }}
    {% endfor %}
    , r.avg_minutes_last{{ n }}
    , r.avg_points_last{{ n }}
    , r.clean_sheet_rate_last{{ n }}
    , r.avg_goals_conceded_last{{ n }}
    , r.appearances_in_window{{ n }}
    {% endfor %}
    , r.minutes_volatility_last5
    , r.kickoff_time as form_as_of

from {{ ref('stg_player_fixture') }} f
asof left join rolled r
    on  f.season = r.season
    and f.player_id = r.player_id
    and f.kickoff_time > r.kickoff_time
