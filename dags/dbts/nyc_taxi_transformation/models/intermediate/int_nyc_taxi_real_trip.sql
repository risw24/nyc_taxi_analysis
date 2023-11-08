with source as (

    select * from {{ source('nyc_taxi', 'raw_nyc_taxi_trip') }}

),

cleaned_taxi_record as (

  select
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID,	
    DOLocationID,
    RatecodeID,
    trip_type,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount
  from source
  where
    passenger_count is not null 
    and payment_type is not null

),

taxi_with_real_trip as (
    select *
    from cleaned_taxi_record
    where
        trip_distance > 0
        and fare_amount > 0
        and passenger_count > 0
),

base as (
    select *
    from taxi_with_real_trip
    where
        total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge
)

select * from base

