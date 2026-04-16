select
	event as gameweek,
	id as fixture_id,
	kickoff_time,
	team_a,
	team_h,
	team_a_score,
	team_h_score,
	team_a_difficulty,
	team_h_difficulty,
	ingested_at
from {{ source('raw_layer','raw_fixtures') }}
qualify row_number() over (partition by id order by ingested_at desc) = 1
