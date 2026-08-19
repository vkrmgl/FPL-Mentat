-- Grain: season x identifier x position x min_value
--
-- Passthrough of the validated seed. Scoring is applied from this table rather
-- than learned, which is what lets component models train across seasons whose
-- rules differ - `defensive_contribution` only exists from 2025-26, so a
-- points-target model trained on older data would be fitting a scoring system
-- that no longer exists.

select
    season,
    identifier,
    position,
    rule_type,
    unit_size,
    points,
    min_value,
    verified,
    note
from {{ ref('scoring_rules') }}
