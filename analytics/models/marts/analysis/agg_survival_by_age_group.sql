{{ config(materialized='table') }}

SELECT
    CASE
        WHEN age < 18 THEN 'Child'
        WHEN age BETWEEN 18 AND 35 THEN 'Young Adult'
        WHEN age BETWEEN 36 AND 60 THEN 'Adult'
        WHEN age > 60 THEN 'Senior'
        ELSE 'Unknown'
    END AS age_group,

    COUNT(*) AS total_passengers,

    SUM(survived) AS total_survivors,

    ROUND(
        100.0 * SUM(survived) / NULLIF(COUNT(*), 0),
        2
    ) AS survival_rate

FROM {{ ref('fct_titanic') }}

GROUP BY 1

ORDER BY 1