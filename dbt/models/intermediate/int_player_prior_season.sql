-- Grain: season x player_code
--
-- What a player did in the season *before* the one named, expressed as per-90
-- rates. Keyed on the career-stable `player_code` rather than the per-season
-- `player_id`.
--
-- This exists for the cold start. In gameweek 1 no match has been played, so
-- every rolling form feature is null and the models have nothing to condition
-- on. Last season is the best available prior, and 78% of a new season's squad
-- list has one.
--
-- Deliberately not a substitute for in-season form: it is a separate set of
-- columns, prefixed `prior_`, so a model can weigh it against current form
-- rather than having the two silently averaged together.

with prior as (
    select
        f.season,
        s.player_code,
        sum(f.target_minutes) as prior_minutes,
        count(*) as prior_fixtures,
        count(*) filter (where f.target_minutes > 0) as prior_appearances,
        count(*) filter (where f.target_minutes >= 60) as prior_starts,
        sum(f.target_goals_scored) as prior_goals,
        sum(f.target_assists) as prior_assists,
        sum(f.target_bonus) as prior_bonus,
        sum(f.target_bps) as prior_bps,
        sum(f.target_saves) as prior_saves,
        sum(f.target_clean_sheet) as prior_clean_sheets,
        sum(f.target_total_points) as prior_points
    from {{ ref('fct_player_fixture') }} f
    join {{ ref('stg_player_snapshot') }} s
        on  s.season = f.season
        and s.player_id = f.player_id
        and s.is_current
    where s.player_code is not null
    group by f.season, s.player_code
)

select
    -- Labelled with the season this feeds, not the season it describes.
    cast(
        cast(substr(season, 1, 4) as integer) + 1 as varchar
    ) || '-' || lpad(cast(
        (cast(substr(season, 6, 2) as integer) + 1) % 100 as varchar
    ), 2, '0') as season,
    season as prior_season,
    player_code,
    prior_minutes,
    prior_fixtures,
    prior_appearances,
    prior_starts,
    prior_points,
    prior_appearances::double / nullif(prior_fixtures, 0) as prior_appearance_rate,
    prior_starts::double / nullif(prior_fixtures, 0) as prior_start_rate,
    prior_minutes::double / nullif(prior_appearances, 0) as prior_minutes_per_appearance,
    prior_goals / nullif(prior_minutes / 90.0, 0) as prior_goals_p90,
    prior_assists / nullif(prior_minutes / 90.0, 0) as prior_assists_p90,
    prior_bonus / nullif(prior_minutes / 90.0, 0) as prior_bonus_p90,
    prior_bps / nullif(prior_minutes / 90.0, 0) as prior_bps_p90,
    prior_saves / nullif(prior_minutes / 90.0, 0) as prior_saves_p90,
    prior_clean_sheets::double / nullif(prior_appearances, 0) as prior_clean_sheet_rate,
    prior_points / nullif(prior_appearances, 0) as prior_points_per_appearance
from prior
