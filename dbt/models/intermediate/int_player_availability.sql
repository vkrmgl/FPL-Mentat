-- Grain: season x player_id x fixture_id
--
-- Everything the appearance and minutes models need. Kept separate from
-- int_player_form on purpose: form answers "how well does he play", this
-- answers "will he play at all", and conflating them is what made the old
-- pipeline need a hand-maintained `invalid_players` list in the notebook.
--
-- Note the denominator. Rates here are over every fixture the player's squad
-- played, including ones he sat out, because a benched appearance is exactly
-- the signal being measured. That is the opposite of int_player_form, which
-- counts appearances only.
--
-- This is also where non-Premier-League football is accounted for. Cup and
-- European minutes are not exposed by the FPL API at all, so they are captured
-- indirectly through their effects: `days_since_last_match` and the trailing
-- fixture counts pick up congestion, and `follows_international_break` picks up
-- the travel-and-fatigue window. Actual midweek minutes would need an external
-- source and are deliberately out of scope until the appearance model exists
-- and the residual can be measured.

{% set windows = [3, 5, 10] %}

with squad_fixtures as (
    select
        season,
        player_id,
        fixture_id,
        gameweek,
        team_id,
        kickoff_time,
        minutes,
        coalesce(starts, case when minutes > 0 then 1 else 0 end) as started,
        case when minutes > 0 then 1 else 0 end as appeared,
        case when minutes >= 60 then 1 else 0 end as played_60
    from {{ ref('stg_player_fixture') }}
),

rolled as (
    select
        season,
        player_id,
        kickoff_time
        {% for n in windows %}
        , avg(appeared) over w{{ n }} as appearance_rate_last{{ n }}
        , avg(started) over w{{ n }} as start_rate_last{{ n }}
        , avg(played_60) over w{{ n }} as played_60_rate_last{{ n }}
        , avg(minutes) over w{{ n }} as avg_minutes_incl_absent_last{{ n }}
        , sum(minutes) over w{{ n }} as total_minutes_last{{ n }}
        {% endfor %}
        , stddev_samp(minutes) over w5 as minutes_volatility_last5
        , max(case when minutes > 0 then kickoff_time end) over w_all
            as last_appearance_at
    from squad_fixtures
    window
        {% for n in windows %}
        w{{ n }} as (
            partition by season, player_id order by kickoff_time
            rows between {{ n - 1 }} preceding and current row
        ),
        {% endfor %}
        w_all as (
            partition by season, player_id order by kickoff_time
            rows between unbounded preceding and current row
        )
)

select
    f.season,
    f.player_id,
    f.fixture_id,
    f.gameweek,
    f.team_id,
    f.kickoff_time
    {% for n in windows %}
    , r.appearance_rate_last{{ n }}
    , r.start_rate_last{{ n }}
    , r.played_60_rate_last{{ n }}
    , r.avg_minutes_incl_absent_last{{ n }}
    , r.total_minutes_last{{ n }}
    {% endfor %}
    , r.minutes_volatility_last5

    -- Days since the player last got on the pitch. Distinguishes a rested
    -- starter from someone working back from a long absence, which the
    -- appearance rates alone cannot.
    , date_diff('day', r.last_appearance_at, f.kickoff_time) as days_since_last_appearance

    -- Team-level schedule pressure, from the spine.
    , tf.days_since_last_match as team_days_rest
    , sp.fixtures_last_3_gameweeks
    , sp.fixtures_last_5_gameweeks
    , sp.fixture_cnt as gameweek_fixture_cnt
    , sp.is_double_gameweek
    , sp.is_blank_gameweek
    , sp.next_gw_fixture_cnt
    , sp.follows_international_break
    , sp.days_since_prev_gameweek

from {{ ref('stg_player_fixture') }} f
asof left join rolled r
    on  f.season = r.season
    and f.player_id = r.player_id
    and f.kickoff_time > r.kickoff_time
left join {{ ref('int_team_fixture') }} tf
    on  tf.season = f.season
    and tf.team_id = f.team_id
    and tf.fixture_id = f.fixture_id
left join {{ ref('int_team_gameweek_spine') }} sp
    on  sp.season = f.season
    and sp.team_id = f.team_id
    and sp.gameweek = f.gameweek
