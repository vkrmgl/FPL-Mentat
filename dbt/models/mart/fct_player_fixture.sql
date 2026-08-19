-- Grain: season x player_id x fixture_id
--
-- The training table. One row per player per match, carrying leak-free features
-- alongside the component outcomes each model predicts.
--
-- Replaces `fact_ml_features`, which had three problems this design removes:
-- it was at gameweek grain with a fan-out that duplicated 327 rows, its
-- `is_prediction_row` compared against `current_timestamp` so the table was
-- non-deterministic on rebuild, and it carried a single `target_total_points`
-- that is only meaningful under one season's scoring rules.
--
-- Targets are components, not points. `target_total_points` is retained for
-- evaluation and benchmarking, but nothing should train on it directly across
-- seasons - `defensive_contribution` only became a scoring event in 2025-26.
-- See docs/target-spec.html.

select
    -- ---------------------------------------------------------------- keys
    f.season,
    f.player_id,
    f.fixture_id,
    f.gameweek,
    f.kickoff_time,
    f.team_id,
    f.opponent_team_id,
    f.is_home,
    f.position,
    f.source,

    -- ------------------------------------------------------------ context
    f.price,

    -- Market signals, ranked within gameweek rather than used raw.
    --
    -- The two lineages report these on incompatible scales - vaastav gives raw
    -- manager counts, the API gives an ownership percentage and a per-gameweek
    -- transfer delta - and the raw magnitudes drift anyway as the player base
    -- grows season on season. A percentile within the gameweek is comparable
    -- across both, and "third most transferred-in player this week" is the
    -- signal being reached for regardless.
    --
    -- These matter: they are the three strongest features in the appearance
    -- model. Left on raw scales they would be silently null for the live season.
    percent_rank() over (
        partition by f.season, f.gameweek order by f.selected_count
    ) as ownership_rank_pct,
    percent_rank() over (
        partition by f.season, f.gameweek order by f.transfers_in
    ) as transfers_in_rank_pct,
    percent_rank() over (
        partition by f.season, f.gameweek order by f.transfers_out
    ) as transfers_out_rank_pct,

    f.selected_count as selected_raw,
    f.transfers_in as transfers_in_raw,
    f.transfers_out as transfers_out_raw,
    a.gameweek_fixture_cnt,
    a.is_double_gameweek,
    a.is_blank_gameweek,
    a.next_gw_fixture_cnt,

    -- Schedule pressure. This is how non-Premier-League football enters the
    -- model: cup and European minutes are invisible to the FPL API, so their
    -- effects are carried by rest and congestion instead.
    a.team_days_rest,
    a.days_since_last_appearance,
    a.fixtures_last_3_gameweeks,
    a.fixtures_last_5_gameweeks,
    a.follows_international_break,
    a.days_since_prev_gameweek,

    -- ------------------------------------------------- availability features
    a.appearance_rate_last3,
    a.appearance_rate_last5,
    a.appearance_rate_last10,
    a.start_rate_last3,
    a.start_rate_last5,
    a.start_rate_last10,
    a.played_60_rate_last3,
    a.played_60_rate_last5,
    a.played_60_rate_last10,
    a.avg_minutes_incl_absent_last3,
    a.avg_minutes_incl_absent_last5,
    a.avg_minutes_incl_absent_last10,
    a.minutes_volatility_last5,

    -- --------------------------------------------------------- form features
    -- Per-90 rates over the player's last N appearances.
    pf.goals_scored_p90_last3,
    pf.goals_scored_p90_last5,
    pf.goals_scored_p90_last10,
    pf.assists_p90_last3,
    pf.assists_p90_last5,
    pf.assists_p90_last10,
    pf.expected_goals_p90_last3,
    pf.expected_goals_p90_last5,
    pf.expected_goals_p90_last10,
    pf.expected_assists_p90_last3,
    pf.expected_assists_p90_last5,
    pf.expected_assists_p90_last10,
    pf.expected_goal_involvements_p90_last5,
    pf.bps_p90_last3,
    pf.bps_p90_last5,
    pf.bps_p90_last10,
    pf.bonus_p90_last5,
    pf.saves_p90_last3,
    pf.saves_p90_last5,
    pf.defensive_contribution_p90_last3,
    pf.defensive_contribution_p90_last5,
    pf.clearances_blocks_interceptions_p90_last5,
    pf.recoveries_p90_last5,
    pf.tackles_p90_last5,
    pf.influence_p90_last5,
    pf.creativity_p90_last5,
    pf.threat_p90_last5,
    pf.avg_points_last3,
    pf.avg_points_last5,
    pf.avg_points_last10,
    pf.clean_sheet_rate_last5 as player_clean_sheet_rate_last5,
    pf.avg_goals_conceded_last5 as player_avg_goals_conceded_last5,
    pf.appearances_in_window5,

    -- ---------------------------------------------------------- team context
    tf.difficulty as fixture_difficulty,
    tf.opponent_difficulty,
    tm.avg_goals_for_last3 as team_avg_goals_for_last3,
    tm.avg_goals_for_last5 as team_avg_goals_for_last5,
    tm.avg_goals_for_last10 as team_avg_goals_for_last10,
    tm.avg_goals_against_last3 as team_avg_goals_against_last3,
    tm.avg_goals_against_last5 as team_avg_goals_against_last5,
    tm.avg_goals_against_last10 as team_avg_goals_against_last10,
    tm.clean_sheet_rate_last5 as team_clean_sheet_rate_last5,
    tm.avg_xg_last5 as team_avg_xg_last5,
    tm.avg_xa_last5 as team_avg_xa_last5,
    tm.win_rate_last5 as team_win_rate_last5,

    -- Opponent form joins through int_team_form on the *same fixture*, so there
    -- is no second self-join on the fixtures table to fan out.
    opp.avg_goals_for_last3 as opp_avg_goals_for_last3,
    opp.avg_goals_for_last5 as opp_avg_goals_for_last5,
    opp.avg_goals_against_last3 as opp_avg_goals_against_last3,
    opp.avg_goals_against_last5 as opp_avg_goals_against_last5,
    opp.clean_sheet_rate_last5 as opp_clean_sheet_rate_last5,
    opp.avg_xg_last5 as opp_avg_xg_last5,
    opp.win_rate_last5 as opp_win_rate_last5,

    -- ------------------------------------------------------------- targets
    -- What actually happened. Each is the target of one component model.
    f.minutes as target_minutes,
    case when f.minutes > 0 then 1 else 0 end as target_appeared,
    case when f.minutes >= 60 then 1 else 0 end as target_played_60,
    f.goals_scored as target_goals_scored,
    f.assists as target_assists,
    f.clean_sheets as target_clean_sheet,
    f.goals_conceded as target_goals_conceded,
    f.saves as target_saves,
    f.bonus as target_bonus,
    f.bps as target_bps,
    f.defensive_contribution as target_defensive_contribution,
    f.yellow_cards as target_yellow_cards,
    f.red_cards as target_red_cards,
    f.own_goals as target_own_goals,
    f.penalties_saved as target_penalties_saved,
    f.penalties_missed as target_penalties_missed,

    -- Retained for evaluation only. Not comparable across seasons.
    f.total_points as target_total_points,

    -- FPL's own expected points, as a public benchmark. An exact 0.0 is a
    -- missing-value sentinel rather than a real forecast: the upstream archive
    -- stopped capturing xP for whole gameweeks (71% of 2025-26 rows, including
    -- every row in gameweeks 10-23), and a continuous forecast for a player who
    -- went on to appear cannot legitimately be zero when appearing alone earns a
    -- point. Left as-is it silently inflates the benchmark's error and makes FPL
    -- look far worse than it is.
    nullif(f.fpl_expected_points, 0) as benchmark_fpl_xp,

    f.finished_fixture

from (
    select
        pfx.*,
        fx.finished as finished_fixture
    from {{ ref('stg_player_fixture') }} pfx
    left join {{ ref('stg_fixtures') }} fx
        on  fx.season = pfx.season
        and fx.fixture_id = pfx.fixture_id
) f

left join {{ ref('int_player_availability') }} a
    on  a.season = f.season
    and a.player_id = f.player_id
    and a.fixture_id = f.fixture_id

left join {{ ref('int_player_form') }} pf
    on  pf.season = f.season
    and pf.player_id = f.player_id
    and pf.fixture_id = f.fixture_id

left join {{ ref('int_team_fixture') }} tf
    on  tf.season = f.season
    and tf.team_id = f.team_id
    and tf.fixture_id = f.fixture_id

left join {{ ref('int_team_form') }} tm
    on  tm.season = f.season
    and tm.team_id = f.team_id
    and tm.fixture_id = f.fixture_id

left join {{ ref('int_team_form') }} opp
    on  opp.season = f.season
    and opp.team_id = f.opponent_team_id
    and opp.fixture_id = f.fixture_id
