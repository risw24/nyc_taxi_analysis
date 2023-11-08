{% test total_amount(model, column_name) %}

select *
    from {{ model }}
    where
        {{ column_name }} != fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge
{% endtest %}