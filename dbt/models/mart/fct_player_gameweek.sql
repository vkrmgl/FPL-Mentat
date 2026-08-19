-- Grain: season x player_id x gameweek
--
-- The deliberate aggregate. Double gameweeks are summed here, once, at the end
-- of the pipeline - rather than arriving pre-collapsed at the start and being
-- approximated afterwards with a 1.8x multiplier.
--
-- Exists for reporting and evaluation, not for training. Models train on
-- fct_player_fixture; scoring and squad decisions happen per gameweek because
-- that is the unit FPL settles points in.

select
    season,
    player_id,
    gameweek,
    any_value(team_id) as team_id,
    any_value(position) as position,
    any_value(price) as price,

    count(*) as fixture_cnt,
    count(*) filter (where target_minutes > 0) as fixtures_appeared,
    max(is_double_gameweek) as is_double_gameweek,
    min(kickoff_time) as first_kickoff,

    sum(target_minutes) as minutes,
    sum(target_goals_scored) as goals_scored,
    sum(target_assists) as assists,
    sum(target_clean_sheet) as clean_sheets,
    sum(target_goals_conceded) as goals_conceded,
    sum(target_saves) as saves,
    sum(target_bonus) as bonus,
    sum(target_bps) as bps,
    sum(target_defensive_contribution) as defensive_contribution,
    sum(target_yellow_cards) as yellow_cards,
    sum(target_red_cards) as red_cards,
    sum(target_own_goals) as own_goals,
    sum(target_penalties_saved) as penalties_saved,
    sum(target_penalties_missed) as penalties_missed,

    sum(target_total_points) as total_points,
    sum(benchmark_fpl_xp) as benchmark_fpl_xp,

    bool_and(finished_fixture) as all_fixtures_finished

from {{ ref('fct_player_fixture') }}
group by season, player_id, gameweek
