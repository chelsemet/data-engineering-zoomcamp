{{
    config(
        materialized='table'
    )
}}

with trips_unioned as (
    select *, 
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.service_type,
    EXTRACT(YEAR FROM trips_unioned.pickup_datetime) AS year,
    EXTRACT(MONTH FROM trips_unioned.pickup_datetime) AS month,
    trips_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.sr_flag, 
    trips_unioned.dispatching_base_num, 
    trips_unioned.affiliated_base_number, 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
where trips_unioned.pickup_locationid is not null and trips_unioned.dropoff_locationid is not null