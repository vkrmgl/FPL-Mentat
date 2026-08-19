-- Grain: season x chip_name x season_half
--
-- The season splits in two, each half carrying its own wildcard, free hit,
-- bench boost and triple captain. `start_event`/`stop_event` bound when each
-- can be played, which is what turns chip timing into a constrained choice
-- rather than a guess.

select
    season,
    name as chip_name,
    chip_type,
    start_event as first_gameweek,
    stop_event as last_gameweek,
    case when start_event <= 19 then 1 else 2 end as season_half
from {{ source('raw_layer', 'raw_chips') }}
qualify row_number() over (
    partition by season, name, start_event order by ingested_at desc
) = 1
