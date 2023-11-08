with source as (

    select * from {{ source('nyc_taxi_int', 'nyc_taxi_real_trip_2021') }}

),

final as (
  {{ monthly_total_column_with_indicator(source,'payment_type','payment_type') }}
)

select * from final