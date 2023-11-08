with source as (

    select * from {{ source('nyc_taxi_int', 'nyc_taxi_real_trip_2021') }}

),

version_1 as (
  {{ monthly_total_column_with_indicator(source,'payment_type','payment_type') }}
),

final as (
  SELECT
    name_month,
    SUM(CASE WHEN payment_type = 1 THEN total_payment_type ELSE 0 END) AS transaction_type_1,
    SUM(CASE WHEN payment_type = 2 THEN total_payment_type ELSE 0 END) AS transaction_type_2,
    SUM(CASE WHEN payment_type = 3 THEN total_payment_type ELSE 0 END) AS transaction_type_3,
    SUM(CASE WHEN payment_type = 4 THEN total_payment_type ELSE 0 END) AS transaction_type_4,
    SUM(CASE WHEN payment_type = 5 THEN total_payment_type ELSE 0 END) AS transaction_type_5,
    SUM(CASE WHEN payment_type = 6 THEN total_payment_type ELSE 0 END) AS transaction_type_6,
FROM version_1
GROUP BY name_month
)

select * from final