{% macro rolling_avg(column_name, window, partition_by) %}
	avg({{ column_name }}) over(
		partition by {{ partition_by }}
		order by gameweek
		rows between {{	window }} preceding and 1 preceding
	) as avg_{{ column_name }}_gw{{ window }}
{% endmacro %}
