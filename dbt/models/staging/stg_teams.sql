select
	id as team_id,
	name as team_name,
	ingested_at
from {{ source('raw_layer','raw_teams') }}
qualify row_number() over (partition by id order by ingested_at desc) = 1
