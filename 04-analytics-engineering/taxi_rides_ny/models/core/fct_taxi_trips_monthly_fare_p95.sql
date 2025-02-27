{{ config(materialized="table") }}

with refined_table as (
    select
        'green' as service_type,
        fare_amount,
        trip_distance,
        payment_type,
        {{ get_payment_type_description("payment_type") }}
        as payment_type_description
    from {{ source("staging", "green_tripdata_2020_04") }}

    union all

    select
        'yellow' as service_type,
        fare_amount,
        trip_distance,
        payment_type,
        {{ get_payment_type_description("payment_type") }}
        as payment_type_description
    from {{ source("staging", "yellow_tripdata_2020_04") }}
),

filtered_table as (
    select 
    service_type,
    fare_amount 
    from refined_table
    where    
    fare_amount > 0
    AND trip_distance > 0
    AND payment_type_description IN ('Cash', 'Credit card')
)

select
    DISTINCT
    service_type, 
    percentile_cont(fare_amount, 0.97) over (
        partition by service_type
    ) as fare_amount_p97,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type
    ) as fare_amount_p95,
    percentile_cont(fare_amount, 0.90) over (
        partition by service_type
    ) as fare_amount_p90
from filtered_table
group by service_type, fare_amount
order by service_type
