-- Grain: season x player_id x fixture_id x identifier
--
-- Long-form component points: what each scoring event contributed, per fixture.
-- API lineage only, so 2025-26 onward - vaastav carries component *stats* but
-- not the points attribution.
--
-- Two jobs. It is the direct training target for the decomposition models, and
-- it is the reconciliation check: summing `points` per player-gameweek must
-- equal the recorded total, which currently holds for 27,228 of 27,228 rows.

select
    season,
    player_id,
    gameweek,
    fixture_id,
    identifier,
    value,
    points,
    points_modification
from {{ source('raw_layer', 'raw_player_fixture_points') }}
qualify row_number() over (
    partition by season, player_id, fixture_id, identifier
    order by ingested_at desc
) = 1
