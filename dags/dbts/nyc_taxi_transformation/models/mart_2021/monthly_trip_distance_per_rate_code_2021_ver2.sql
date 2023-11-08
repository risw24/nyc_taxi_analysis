with source as (

    select * from {{ source('nyc_taxi_int', 'nyc_taxi_real_trip_2021') }}

),

version_1 as (
  {{ monthly_total_column_with_indicator(source, 'trip_distance', 'RatecodeID') }}
),

final as (
  SELECT
    name_month,
    SUM(CASE WHEN RatecodeID = 1 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_1,
    SUM(CASE WHEN RatecodeID = 2 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_2,
    SUM(CASE WHEN RatecodeID = 3 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_3,
    SUM(CASE WHEN RatecodeID = 4 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_4,
    SUM(CASE WHEN RatecodeID = 5 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_5,
    SUM(CASE WHEN RatecodeID = 6 THEN {{ format_to_two_decimal_places('total_trip_distance') }} ELSE 0 END) AS rate_code_6,
FROM version_1
GROUP BY name_month
)

select * from final