select
	team_id,
	team_name
from {{ ref('stg_teams') }}
