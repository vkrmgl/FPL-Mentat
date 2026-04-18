{% set windows = [3,5] %}
{% set avg_stats = [
	'goals_scored', 'goals_conceded'
] %}

with 
base as (
	select
		f.team_a as team_id,
		f.gameweek,
		f.team_a_score as goals_scored,
		f.team_h_score as goals_conceded		
	from {{ ref('stg_fixtures') }} f

	union all

	select
		f.team_h as team_id,
		f.gameweek,
		f.team_h_score as goals_scored,
		f.team_a_score as goals_conceded		
	from {{ ref('stg_fixtures') }} f
)
select
	team_id,
	{% for stat in avg_stats %}
		{% for window in windows %}
			{{ rolling_avg(stat, window, 'team_id') }},
		{% endfor %}
	{% endfor %}
	gameweek
from base
