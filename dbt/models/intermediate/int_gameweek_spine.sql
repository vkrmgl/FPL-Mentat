-- Grain: season x gameweek
--
-- Every gameweek in every season the warehouse covers.
--
-- Deadlines only exist for seasons ingested from the live API (2025-26 onward),
-- but the historical backfill still needs schedule shape - congestion and
-- international-break features are as relevant to 2018-19 training rows as to
-- next week's prediction. Gameweeks are therefore taken from deadlines where
-- available and derived from fixture kickoffs everywhere else.
--
-- `gameweek_start` is the deadline when known and the first kickoff otherwise.
-- The two differ by roughly ninety minutes, which is immaterial to the day-level
-- gap arithmetic built on top of it.

with from_deadlines as (
    select
        season,
        gameweek,
        deadline_time
    from {{ ref('stg_deadlines') }}
),

from_fixtures as (
    select
        season,
        gameweek,
        min(kickoff_time) as first_kickoff,
        max(kickoff_time) as last_kickoff,
        count(*) as fixture_cnt
    from {{ ref('stg_fixtures') }}
    group by season, gameweek
),

-- Generate a dense 1..max range per season rather than taking the gameweeks
-- that happen to have fixtures. Whole gameweeks can be empty: 2022-23 gameweek
-- 7 was postponed in its entirety after the death of Queen Elizabeth II, so
-- every team blanked at once. Deriving the spine from observed fixtures would
-- silently drop that gameweek, and `lead()` would then step over it exactly the
-- way the old fixture ticker stepped over single-team blanks.
bounds as (
    select season, max(gameweek) as max_gameweek
    from (
        select season, gameweek from from_fixtures
        union all
        select season, gameweek from from_deadlines
    )
    group by season
),

dense as (
    select season, unnest(generate_series(1, max_gameweek)) as gameweek
    from bounds
),

combined as (
    select
        g.season,
        g.gameweek,
        d.deadline_time,
        f.first_kickoff,
        f.last_kickoff,
        coalesce(f.fixture_cnt, 0) as fixture_cnt,
        d.deadline_time is not null as has_deadline
    from dense g
    left join from_deadlines d
        on  d.season = g.season
        and d.gameweek = g.gameweek
    left join from_fixtures f
        on  f.season = g.season
        and f.gameweek = g.gameweek
)

select
    season,
    gameweek,
    deadline_time,
    first_kickoff,
    last_kickoff,
    fixture_cnt,
    has_deadline,
    coalesce(deadline_time, first_kickoff) as gameweek_start,

    date_diff(
        'day',
        lag(coalesce(deadline_time, first_kickoff)) over (
            partition by season order by gameweek
        ),
        coalesce(deadline_time, first_kickoff)
    ) as days_since_prev_gameweek

from combined
where gameweek is not null
