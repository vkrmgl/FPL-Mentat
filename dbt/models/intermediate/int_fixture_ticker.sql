with
joined as (
	select
		team_a as team_id,
		gameweek,
	count(fixture_id) as fixture_cnt
	from {{ ref('stg_fixtures') }} f
	group by team_a, gameweek

	union all

	select
		team_h as team_id,
		gameweek,
	count(fixture_id) as fixture_cnt
	from {{ ref('stg_fixtures') }} f
	group by team_h, gameweek
),

aggregated AS (
	select
		team_id,
		gameweek,
		sum(fixture_cnt) as fixture_cnt
	from joined
	group by team_id, gameweek
),

next_gw as (
	select
		team_id,
		gameweek,
		fixture_cnt,
		lead(fixture_cnt, 1) over (
			partition by team_id order by gameweek
		) as next_gw_fixture_cnt
	from aggregated
)

select *
from next_gw
