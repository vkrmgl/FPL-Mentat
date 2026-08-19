-- Grain: season x player_id x fixture_id
--
-- The core observation table: one row per player per match they were involved
-- in. Double gameweeks are two rows, not one collapsed row, which is the whole
-- point of the rebuild.
--
-- Two lineages, unioned deliberately and tagged with `source`:
--
--   vaastav  2016-17..2025-26. Already per-fixture, and carries things the API
--            does not expose historically - per-gameweek price and ownership,
--            transfer flow, Understat xG, and FPL's own xP as a benchmark.
--
--   api      2026-27 onward. Assembled from `explain`, which is per-fixture for
--            every scoring component. Non-scoring stats (bps, xG, ICT, defensive
--            actions) only exist at gameweek grain, so on a double gameweek they
--            are split across the two fixtures by minutes share. On the ~99% of
--            gameweeks with a single fixture that split is exact.

{% set explain_components = [
    'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
    'own_goals', 'penalties_saved', 'penalties_missed', 'yellow_cards',
    'red_cards', 'saves', 'bonus', 'defensive_contribution'
] %}

with

-- ---------------------------------------------------------------- historical
historical as (
    select
        season,
        element as player_id,
        fixture as fixture_id,
        gameweek,
        cast(kickoff_time as timestamp) as kickoff_time,
        team as team_name_raw,
        opponent_team as opponent_team_id,
        cast(was_home as boolean) as is_home,

        -- Position as at that gameweek. FPL reclassifies players between
        -- seasons and occasionally within one, so this is point-in-time rather
        -- than a current-state lookup. 'AM' is the Assistant Manager slot that
        -- existed in 2024-25 and is not a squad position.
        case position
            when 'GK'  then 1 when 'GKP' then 1
            when 'DEF' then 2
            when 'MID' then 3
            when 'FWD' then 4
            when 'AM'  then 5
        end as position,

        cast(minutes as double) as minutes,
        cast(starts as double) as starts,
        cast(goals_scored as double) as goals_scored,
        cast(assists as double) as assists,
        cast(clean_sheets as double) as clean_sheets,
        cast(goals_conceded as double) as goals_conceded,
        cast(own_goals as double) as own_goals,
        cast(penalties_saved as double) as penalties_saved,
        cast(penalties_missed as double) as penalties_missed,
        cast(yellow_cards as double) as yellow_cards,
        cast(red_cards as double) as red_cards,
        cast(saves as double) as saves,
        cast(bonus as double) as bonus,
        cast(bps as double) as bps,
        cast(defensive_contribution as double) as defensive_contribution,
        cast(tackles as double) as tackles,
        cast(recoveries as double) as recoveries,
        cast(clearances_blocks_interceptions as double) as clearances_blocks_interceptions,

        cast(influence as double) as influence,
        cast(creativity as double) as creativity,
        cast(threat as double) as threat,
        cast(ict_index as double) as ict_index,
        cast(expected_goals as double) as expected_goals,
        cast(expected_assists as double) as expected_assists,
        cast(expected_goal_involvements as double) as expected_goal_involvements,
        cast(expected_goals_conceded as double) as expected_goals_conceded,

        cast(value as double) / 10.0 as price,
        cast(selected as double) as selected_count,
        cast(transfers_in as double) as transfers_in,
        cast(transfers_out as double) as transfers_out,
        cast(xP as double) as fpl_expected_points,

        cast(total_points as double) as total_points,
        'vaastav' as source,
        ingested_at as effective_at
    from {{ source('raw_layer', 'raw_vaastav_player_gameweek') }}
    where fixture is not null
),

-- ---------------------------------------------------------------------- api
-- Per-fixture scoring components, pivoted out of the `explain` block. The
-- block is sparse - it only lists events that occurred - so absent identifiers
-- become 0 rather than null.
explain_latest as (
    select season, player_id, gameweek, fixture_id, identifier, value
    from {{ source('raw_layer', 'raw_player_fixture_points') }}
    qualify row_number() over (
        partition by season, player_id, fixture_id, identifier
        order by ingested_at desc
    ) = 1
),

explain_pivot as (
    select
        season,
        player_id,
        gameweek,
        fixture_id
        {% for component in explain_components %}
        , coalesce(max(case when identifier = '{{ component }}' then value end), 0)
            as {{ component }}
        {% endfor %}
    from explain_latest
    group by season, player_id, gameweek, fixture_id
),

-- Gameweek-grain stats that `explain` does not carry.
gameweek_extras as (
    select
        season,
        player_id,
        gameweek,
        cast(bps as double) as bps,
        cast(starts as double) as starts,
        cast(tackles as double) as tackles,
        cast(recoveries as double) as recoveries,
        cast(clearances_blocks_interceptions as double) as clearances_blocks_interceptions,
        cast(influence as double) as influence,
        cast(creativity as double) as creativity,
        cast(threat as double) as threat,
        cast(ict_index as double) as ict_index,
        cast(expected_goals as double) as expected_goals,
        cast(expected_assists as double) as expected_assists,
        cast(expected_goal_involvements as double) as expected_goal_involvements,
        cast(expected_goals_conceded as double) as expected_goals_conceded,
        cast(total_points as double) as total_points
    from {{ source('raw_layer', 'raw_player_gameweek') }}
    qualify row_number() over (
        partition by season, player_id, gameweek order by ingested_at desc
    ) = 1
),

-- Share of the gameweek's minutes played in each fixture. Used to split the
-- gameweek-grain stats above. Falls back to an even split when a player
-- recorded no minutes at all, so the shares always sum to 1.
allocation as (
    select
        p.*,
        case
            when sum(p.minutes) over w = 0
                then 1.0 / count(*) over w
            else p.minutes / sum(p.minutes) over w
        end as minutes_share
    from explain_pivot p
    window w as (partition by p.season, p.player_id, p.gameweek)
),

api as (
    select
        a.season,
        a.player_id,
        a.fixture_id,
        a.gameweek,
        f.kickoff_time,
        cast(null as varchar) as team_name_raw,
        case when f.home_team_id = s.team_id then f.away_team_id else f.home_team_id end
            as opponent_team_id,
        f.home_team_id = s.team_id as is_home,
        s.position,

        a.minutes,
        e.starts * a.minutes_share as starts,
        a.goals_scored,
        a.assists,
        a.clean_sheets,
        a.goals_conceded,
        a.own_goals,
        a.penalties_saved,
        a.penalties_missed,
        a.yellow_cards,
        a.red_cards,
        a.saves,
        a.bonus,
        e.bps * a.minutes_share as bps,
        a.defensive_contribution,
        e.tackles * a.minutes_share as tackles,
        e.recoveries * a.minutes_share as recoveries,
        e.clearances_blocks_interceptions * a.minutes_share
            as clearances_blocks_interceptions,

        e.influence * a.minutes_share as influence,
        e.creativity * a.minutes_share as creativity,
        e.threat * a.minutes_share as threat,
        e.ict_index * a.minutes_share as ict_index,
        e.expected_goals * a.minutes_share as expected_goals,
        e.expected_assists * a.minutes_share as expected_assists,
        e.expected_goal_involvements * a.minutes_share as expected_goal_involvements,
        e.expected_goals_conceded * a.minutes_share as expected_goals_conceded,

        s.price,
        -- Ownership arrives as a percentage here and as a manager count in the
        -- historical lineage; both are reduced to a within-gameweek rank in the
        -- mart, so the raw scales never meet.
        s.ownership_percent as selected_count,
        cast(s.transfers_in_event as double) as transfers_in,
        cast(s.transfers_out_event as double) as transfers_out,
        cast(null as double) as fpl_expected_points,

        e.total_points * a.minutes_share as total_points,
        'api' as source,
        current_timestamp as effective_at
    from allocation a
    left join gameweek_extras e
        on  e.season = a.season
        and e.player_id = a.player_id
        and e.gameweek = a.gameweek
    left join {{ ref('stg_fixtures') }} f
        on  f.season = a.season
        and f.fixture_id = a.fixture_id
    -- The player's team as at this fixture's kickoff, not their current team.
    left join {{ ref('stg_player_snapshot') }} s
        on  s.season = a.season
        and s.player_id = a.player_id
        and f.kickoff_time >= s.valid_from
        and (s.valid_to is null or f.kickoff_time < s.valid_to)
),

unioned as (
    select * from historical
    union all by name
    select * from api
),

deduped as (
    select * from unioned
    -- vaastav wins for seasons both lineages cover: it runs to gameweek 38,
    -- while the recovered API archive stops at 35, and it carries per-fixture xG.
    qualify row_number() over (
        partition by season, player_id, fixture_id
        order by case when source = 'vaastav' then 0 else 1 end, effective_at desc
    ) = 1
),

-- Fallback position for 2016-17..2019-20, whose gameweek files carry no
-- position at all. Without this those four seasons are unusable for the
-- per-position component models. Sourced from the end-of-season player file, so
-- it is a season-level attribute rather than a point-in-time one - fine for
-- position, which almost never changes mid-season, and explicitly not extended
-- to price or set-piece order.
fallback_position as (
    select season, id as player_id, element_type as position
    from {{ source('raw_layer', 'raw_vaastav_players') }}
    qualify row_number() over (
        partition by season, id order by ingested_at desc
    ) = 1
)

-- Two things are resolved against the fixture rather than taken from the source
-- row:
--
--  team_id   vaastav records the team as a display name, and the API lineage
--            would otherwise depend on a snapshot that may not cover the date.
--            `is_home` plus the fixture gives it exactly, for both sources.
--
--  gameweek  the fixture is authoritative for *when a match was actually
--            played*. In 2019-20 the COVID suspension postponed matches that
--            vaastav still labels with their originally scheduled gameweek -
--            fixture 275 is tagged gameweek 29 on the player row but was played
--            on 17 June as gameweek 39. Taking the label at face value would put
--            a June match inside a March rolling window. 31 rows are affected,
--            all in that season; `source_gameweek` keeps the original for
--            traceability.
select
    d.* exclude (effective_at, team_name_raw, gameweek, position),
    coalesce(f.gameweek, d.gameweek) as gameweek,
    d.gameweek as source_gameweek,
    coalesce(d.position, fp.position) as position,
    case when d.is_home then f.home_team_id else f.away_team_id end as team_id
from deduped d
left join {{ ref('stg_fixtures') }} f
    on  f.season = d.season
    and f.fixture_id = d.fixture_id
left join fallback_position fp
    on  fp.season = d.season
    and fp.player_id = d.player_id
