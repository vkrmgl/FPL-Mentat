-- Grain: season x gameweek
--
-- The gameweek spine for the whole project. Two things downstream depend on it
-- being complete rather than derived from fixtures: blank gameweeks have no
-- fixture rows at all, and the gaps between deadlines are how international
-- breaks are detected.

select
    season,
    id as gameweek,
    cast(deadline_time as timestamp) as deadline_time,
    finished,
    data_checked,
    average_entry_score,
    highest_score,

    -- Days since the previous gameweek. Ordinary weeks sit at 7; a domestic
    -- congestion week drops to 3-4; an international break runs 12 or more.
    date_diff(
        'day',
        lag(cast(deadline_time as timestamp)) over (partition by season order by id),
        cast(deadline_time as timestamp)
    ) as days_since_prev_gameweek

from {{ source('raw_layer', 'raw_events') }}
qualify row_number() over (
    partition by season, id
    order by coalesce(snapshot_at, ingested_at) desc
) = 1
