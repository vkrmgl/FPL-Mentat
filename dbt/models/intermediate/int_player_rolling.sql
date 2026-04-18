{% set windows = [3,5] %}
{% set avg_stats = [
	'goals_scored', 'assists', 'minutes', 'xG', 'xA', 
        'bonus', 'bps', 'clean_sheets', 'goals_conceded',
        'clearances_blocks_interceptions', 'recoveries', 'tackles', 'defensive_contribution',
	'yellow_cards', 'red_cards',
	'saves', 'threat', 'creativity', 'influence',
	'expected_goal_involvements', 'expected_goals_conceded',
	'total_points'
] %}

with base as(
	select
	pg.player_id,
	pg.gameweek,
	ft.fixture_cnt,
	case
	    when ft.fixture_cnt = 0 then 1
	    else 0
	end as had_blank_gw,
	ft.next_gw_fixture_cnt,
	p.position,
	p.team_id,
	case 
		when p.team_id = f.team_a then f.team_h
		when p.team_id = f.team_h then f.team_a 
	end as opponent_id,
	case 
		when p.team_id = f.team_a then 'away'
		when p.team_id = f.team_h then 'home' 
	end as home_or_away,
	case 
		when p.team_id = f.team_a then f.team_a_difficulty
		when p.team_id = f.team_h then f.team_h_difficulty
	end as fixture_difficulty,
	pg.minutes,
	pg.goals_scored,
	pg.assists,
	pg.clean_sheets,
	pg.goals_conceded,
	pg.penalties_saved,
	pg.penalties_missed,
	pg.yellow_cards,
	pg.red_cards,
	pg.saves,
	pg.bonus,
	pg.creativity,
	pg.influence,
	pg.threat,
	pg.clearances_blocks_interceptions,
	pg.recoveries,
	pg.tackles,
	pg.defensive_contribution,
	pg.starts,
	pg.bps,
	pg.xG,
	pg.xA,
	pg.expected_goal_involvements,
	pg.expected_goals_conceded,	
	p.corners_order,
	p.fk_order,
	p.penalty_order,
	pg.total_points
	from {{	ref('stg_player_gameweek') }} pg
	left join {{ ref('int_player_team_corrections') }} p
	    on p.player_id = pg.player_id
	    and p.gameweek = pg.gameweek
	left join {{ ref('stg_fixtures') }} f 
		on pg.gameweek = f.gameweek and (p.team_id = f.team_a or p.team_id = f.team_h)
	left join {{ ref('int_fixture_ticker') }} ft 
		on pg.gameweek = ft.gameweek and p.team_id = ft.team_id
)

select
	player_id,
	gameweek,
	position,
	team_id,
	opponent_id,
	home_or_away,
	fixture_difficulty,
	fixture_cnt,
	had_blank_gw,
	next_gw_fixture_cnt,
	penalty_order,
	fk_order,
	corners_order,
	total_points,
	stddev(minutes) over (
	    partition by player_id
	    order by gameweek
	    rows between 5 preceding and 1 preceding
	) as minutes_variability,
	{% for stat in avg_stats %}
		{% for window in windows %}
			{{ rolling_avg(stat, window, 'player_id') }},
		{% endfor %}
	{% endfor %}
	{{ rolling_sum('fixture_cnt', 3, 'player_id') }},
	{{ rolling_sum('fixture_cnt', 5, 'player_id') }},
	{{ rolling_sum_unbounded('penalties_missed', 'player_id') }},
	{{ rolling_sum_unbounded('penalties_saved', 'player_id') }},
	{{ rolling_sum('starts', 3, 'player_id') }},
	{{ rolling_sum('starts', 5, 'player_id') }}
from base
