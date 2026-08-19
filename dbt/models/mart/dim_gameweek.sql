-- Grain: season x gameweek
--
-- Calendar dimension. `follows_international_break` and `is_congested` are
-- derived from deadline spacing alone, which is the only view of non-Premier
-- League football the FPL API makes available.

select
    season,
    gameweek,
    deadline_time,
    gameweek_start,
    first_kickoff,
    last_kickoff,
    fixture_cnt,
    has_deadline,
    days_since_prev_gameweek,
    coalesce(days_since_prev_gameweek >= 12, false) as follows_international_break,
    coalesce(days_since_prev_gameweek <= 4, false) as is_congested,
    fixture_cnt = 0 as is_empty_gameweek
from {{ ref('int_gameweek_spine') }}
