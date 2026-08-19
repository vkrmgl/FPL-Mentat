-- Grain: season x player_id x valid_from
--
-- Point-in-time player attributes, with validity ranges.
--
-- This model replaces the old `stg_players`, which collapsed the append-only
-- raw layer to current state with `qualify row_number() ... = 1`. That was the
-- root cause of the `player_team_corrections` seed: a January transfer rewrote
-- the player's team for every historical gameweek, so someone had to patch it
-- back by hand. Keeping the snapshots means "what team / price / set-piece
-- order did this player have in gameweek N" is answerable by joining on the
-- validity range instead.
--
-- Coverage caveat: the 2025-26 snapshots span 16 Apr - 8 May only, because
-- ingestion started in April. Point-in-time attributes for 2025-26 gameweeks 1
-- to 30 do not exist in any source and must not be claimed as such. From
-- 2026-27 the weekly run gives full-season coverage.

with snapshots as (
    select
        season,
        id as player_id,
        -- FPL reassigns `id` every season but `code` follows the player for
        -- their whole career - Raya carries one code across six seasons and five
        -- different ids. It is the only way to link a player to their own
        -- history, which is what makes a gameweek-1 prediction possible at all.
        code as player_code,
        coalesce(snapshot_at, ingested_at) as valid_from,

        first_name,
        second_name,
        web_name,
        team as team_id,
        element_type as position,

        now_cost / 10.0 as price,
        cast(selected_by_percent as double) as ownership_percent,

        -- Market signals. Managers transfer a player in when they expect him to
        -- start, so these are among the strongest available predictors of
        -- appearance - and they are known before the deadline, so using them is
        -- not leakage.
        --
        -- Units differ from the historical lineage: the API reports ownership as
        -- a percentage and transfers as a per-gameweek delta, while vaastav
        -- reports raw manager counts. They are reconciled downstream by ranking
        -- within gameweek rather than by unit conversion, which also removes the
        -- drift from a player base that grows every season.
        transfers_in_event,
        transfers_out_event,

        -- Availability. `status` is a single letter: a available, d doubtful,
        -- i injured, s suspended, u unavailable, n on loan.
        status,
        cast(chance_of_playing_this_round as double) as chance_of_playing_this_round,
        cast(chance_of_playing_next_round as double) as chance_of_playing_next_round,
        news,
        news_added,

        -- Set-piece order. Only trustworthy from 2026-27, see caveat above.
        penalties_order,
        direct_freekicks_order as fk_order,
        corners_and_indirect_freekicks_order as corners_order

    from {{ source('raw_layer', 'raw_players') }}
    -- Collapse repeat ingests that landed the same instant.
    qualify row_number() over (
        partition by season, id, coalesce(snapshot_at, ingested_at)
        order by ingested_at desc
    ) = 1
)

select
    *,
    lead(valid_from) over (
        partition by season, player_id order by valid_from
    ) as valid_to,
    row_number() over (
        partition by season, player_id order by valid_from desc
    ) = 1 as is_current
from snapshots
