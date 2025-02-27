{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM lpep_pickup_datetime) AS year,
        EXTRACT(QUARTER FROM lpep_pickup_datetime) AS quarter,
        'green' AS taxi_type,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ source('staging','green_taxi_2019_2020') }}
    GROUP BY 1, 2, 3
    union all
    SELECT
        EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
        EXTRACT(QUARTER FROM tpep_pickup_datetime) AS quarter,
        'yellow' AS taxi_type,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ source('staging','yellow_taxi_2019_2020') }}
    GROUP BY 1, 2, 3
),

quarterly_yoy AS (
    SELECT
        current_year.year,
        current_year.quarter,
        current_year.taxi_type,
        current_year.quarterly_revenue,
        previous_year.quarterly_revenue AS prev_year_revenue,
        CASE
            WHEN previous_year.quarterly_revenue IS NULL OR previous_year.quarterly_revenue = 0 THEN NULL
            ELSE (current_year.quarterly_revenue - previous_year.quarterly_revenue) / previous_year.quarterly_revenue * 100
        END AS yoy_growth_pct
    FROM quarterly_revenue current_year
    LEFT JOIN quarterly_revenue previous_year ON
        current_year.year = previous_year.year + 1
        AND current_year.quarter = previous_year.quarter
        AND current_year.taxi_type = previous_year.taxi_type
)

SELECT
    year,
    quarter,
    taxi_type,
    quarterly_revenue,
    prev_year_revenue,
    ROUND(yoy_growth_pct, 2) AS yoy_growth_pct,
FROM quarterly_yoy
ORDER BY taxi_type, year, quarter