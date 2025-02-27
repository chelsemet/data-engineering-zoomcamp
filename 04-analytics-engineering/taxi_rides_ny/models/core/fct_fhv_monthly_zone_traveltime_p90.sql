
WITH trips_with_duration AS (
    SELECT
        year,
        month,
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone,
        -- Calculate trip duration in seconds
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE dropoff_datetime > pickup_datetime
),

p90 AS (
    SELECT
        year,
        month,
        pickup_locationid,
        pickup_zone,
        dropoff_locationid,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.9) OVER (
            PARTITION BY year, month, pickup_locationid, dropoff_locationid
        ) AS trip_duration_p90
    FROM trips_with_duration
),

distinct_table AS(
SELECT DISTINCT
    year,
    month,
    pickup_locationid,
    pickup_zone,
    dropoff_locationid,
    dropoff_zone,
    trip_duration_p90
FROM p90
WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
AND year = 2019
AND month = 11
),

ranked_routes AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        trip_duration_p90,
        DENSE_RANK() OVER (
            PARTITION BY pickup_zone 
            ORDER BY trip_duration_p90 DESC
        ) AS duration_rank
    FROM distinct_table
)

SELECT
    pickup_zone,
    dropoff_zone,
    trip_duration_p90
FROM ranked_routes
WHERE duration_rank = 2
ORDER BY pickup_zone