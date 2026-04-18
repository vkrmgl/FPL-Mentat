{% macro rolling_sum_unbounded(column_name, partition_by='player_id') %}
sum({{ column_name }}) over(
    partition by {{ partition_by }}
    order by gameweek
    rows between unbounded preceding and 1 preceding
) as {{ column_name }}_season
{% endmacro %}
