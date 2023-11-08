with taxi_real_trip as (

    select * from {{ ref('int_nyc_taxi_real_trip') }}

),

taxi_real_trip_2021 as (
  select *
  from taxi_real_trip
  where extract(year from lpep_pickup_datetime) = 2021
)

select * from taxi_real_trip_2021