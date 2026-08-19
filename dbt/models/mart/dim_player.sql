-- Grain: season x player_id x valid_from  (slowly-changing, type 2)
--
-- Point-in-time player attributes. Joining this without a validity predicate
-- will fan out, which is deliberate: the old `dim_players` was one row per
-- player holding *current* state, and joining it across history is precisely
-- how a January transfer came to rewrite a player's team for every earlier
-- gameweek. The `player_team_corrections` seed exists to patch that damage and
-- is retired by this model.
--
-- Two lineages. The API snapshot carries full attributes but only for seasons
-- ingested live; historical seasons get a single end-of-season row from
-- vaastav, valid for the whole season and flagged as such via `attribute_grain`.

with api_snapshots as (
    select
        season,
        player_id,
        valid_from,
        valid_to,
        is_current,
        first_name,
        second_name,
        web_name,
        team_id,
        position,
        price,
        ownership_percent,
        status,
        chance_of_playing_this_round,
        chance_of_playing_next_round,
        news,
        penalties_order,
        fk_order,
        corners_order,
        'snapshot' as attribute_grain
    from {{ ref('stg_player_snapshot') }}
),

historical as (
    select
        season,
        id as player_id,
        cast('1970-01-01' as timestamp) as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        first_name,
        second_name,
        web_name,
        team as team_id,
        element_type as position,
        now_cost / 10.0 as price,
        cast(selected_by_percent as double) as ownership_percent,
        status,
        cast(chance_of_playing_this_round as double) as chance_of_playing_this_round,
        cast(chance_of_playing_next_round as double) as chance_of_playing_next_round,
        news,
        penalties_order,
        direct_freekicks_order as fk_order,
        corners_and_indirect_freekicks_order as corners_order,
        -- End-of-season capture, not a time series. Set-piece order in
        -- particular must not be read as point-in-time for these seasons.
        'season_end' as attribute_grain
    from {{ source('raw_layer', 'raw_vaastav_players') }}
    where season not in (select distinct season from api_snapshots)
    qualify row_number() over (
        partition by season, id order by ingested_at desc
    ) = 1
)

select * from api_snapshots
union all by name
select * from historical
