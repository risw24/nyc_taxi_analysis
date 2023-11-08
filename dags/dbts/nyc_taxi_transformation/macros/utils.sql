{% macro format_to_two_decimal_places(decimal_value) %}
  ROUND({{ decimal_value }}, 2)
{% endmacro %}

{% macro get_month(column_name) %}
    case
        when extract(month from {{column_name}}) = 1 then "January"
        when extract(month from {{column_name}}) = 2 then "February"
        when extract(month from {{column_name}}) = 3 then "March"
        when extract(month from {{column_name}}) = 4 then "April"
        when extract(month from {{column_name}}) = 5 then "May"
        when extract(month from {{column_name}}) = 6 then "June"
        when extract(month from {{column_name}}) = 7 then "July"
        when extract(month from {{column_name}}) = 8 then "August"
        when extract(month from {{column_name}}) = 9 then "September"
        when extract(month from {{column_name}}) = 10 then "October"
        when extract(month from {{column_name}}) = 11 then "November"
        when extract(month from {{column_name}}) = 12 then "December"
    end as name_month
{% endmacro %}


{% macro monthly_total_column(source, column_target) %}
    select {{ get_month('lpep_pickup_datetime') }}, sum({{column_target}}) as total_{{column_target }}
    from source
    group by name_month

{% endmacro %}

{% macro monthly_total_column_with_indicator(source, column_target, column_indicator) %}

    select {{ get_month('lpep_pickup_datetime') }}, sum({{column_target}}) as total_{{column_target}}, {{column_indicator}}
    from source
    group by name_month, {{column_indicator}}
    order by name_month

{% endmacro %}
