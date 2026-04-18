{% macro rolling_sum(column_name, window, partition_by) %}
sum({{ column_name }}) over (
	partition by {{ partition_by }}
	order by gameweek
	rows between {{ window }} preceding and 1 preceding
) as {{ column_name }}_gw{{ window }}
{% endmacro %}
