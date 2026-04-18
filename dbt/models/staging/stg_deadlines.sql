select
	id as gameweek,
	cast(deadline_time as timestamp) as deadline_time,
	ingested_at
from {{ source('raw_layer','raw_events') }}
qualify row_number() over (partition by id order by ingested_at desc) = 1
