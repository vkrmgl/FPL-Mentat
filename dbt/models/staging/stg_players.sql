select
	id as player_id,
	first_name,
	second_name,
	known_name,
	team as team_id,
	element_type as position,
	penalties_order,
	corners_and_indirect_freekicks_order,
	direct_freekicks_order,
	now_cost/10.0 as price,
	selected_by_percent as ownership_percent,
	ingested_at
from {{ source('raw_layer','raw_players') }}
qualify row_number() over (partition by id order by ingested_at desc) = 1
