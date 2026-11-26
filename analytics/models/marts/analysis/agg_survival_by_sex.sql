{{ config(materialized='table') }}

SELECT
    sex,

    COUNT(*) AS total_passengers,

    SUM(survived) AS total_survivors,

    COUNT(*) - SUM(survived) AS total_deaths,

    ROUND(
        100.0 * SUM(survived) / NULLIF(COUNT(*), 0),
        2
    ) AS survival_rate

FROM {{ ref('fct_titanic') }}

GROUP BY sex