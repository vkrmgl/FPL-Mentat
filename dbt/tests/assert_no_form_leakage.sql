-- No rolling feature may draw on the match it is attached to, or any later one.
--
-- The spec calls for leakage to be asserted rather than assumed. Both form
-- models attach their windows with an ASOF join on a strict inequality, so this
-- should hold by construction - but "by construction" is exactly the kind of
-- claim that quietly stops being true when someone widens a window frame or
-- swaps the join for a convenient left join later.
--
-- Returns offending rows, so an empty result is a pass.

select
    'int_player_form' as model,
    season,
    player_id as entity_id,
    fixture_id,
    kickoff_time,
    form_as_of
from {{ ref('int_player_form') }}
where form_as_of is not null
  and form_as_of >= kickoff_time
