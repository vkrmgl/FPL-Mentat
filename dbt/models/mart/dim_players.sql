select
	player_id,
	first_name,
	second_name,
	known_name,
	team_id,
	corners_and_indirect_freekicks_order as corners_order,
	direct_freekicks_order as fk_order,
	penalties_order,
	ownership_percent,
	price as curr_price
from {{ ref('stg_players') }}
