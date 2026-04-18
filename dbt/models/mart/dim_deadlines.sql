select
	gameweek,
	deadline_time
from {{ ref('stg_deadlines') }}
