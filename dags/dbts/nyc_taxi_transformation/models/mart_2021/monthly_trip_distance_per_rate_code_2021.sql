with source as (

    select * from {{ source('nyc_taxi_int', 'nyc_taxi_real_trip_2021') }}

),

with_decimals as (
  {{ monthly_total_column_with_indicator(source, 'trip_distance', 'RatecodeID') }}
),

final as (
  select name_month, {{ format_to_two_decimal_places('total_trip_distance') }} as total_trip_distance, RatecodeID from with_decimals
)

select * from final