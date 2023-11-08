with source as (

    select * from {{ source('nyc_taxi_int', 'nyc_taxi_real_trip_2021') }}

),

final as (
  {{ monthly_total_column(source, 'passenger_count') }}
)

select * from final