{{ config(materialized='table') }}

SELECT
    passenger_class,

    COUNT(*) AS total_passengers,

    SUM(survived) AS total_survivors,

    ROUND(
        100.0 * SUM(survived) / NULLIF(COUNT(*), 0),
        2
    ) AS survival_rate

FROM {{ ref('fct_titanic') }}

GROUP BY passenger_class

ORDER BY passenger_class