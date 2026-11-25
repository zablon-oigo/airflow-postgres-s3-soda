{{ config(materialized='table') }}

SELECT
    passenger_id,
    passenger_name,
    sex,
    age,
    passenger_class,
    ticket,
    fare,
    cabin,
    embarked,

    siblings_spouses,
    parents_children,

    siblings_spouses + parents_children AS family_size,

    CASE
        WHEN siblings_spouses + parents_children > 0
        THEN TRUE
        ELSE FALSE
    END AS travelled_with_family,

    survived,

    CASE
        WHEN survived = 1 THEN 'Survived'
        ELSE 'Did Not Survive'
    END AS survival_status

FROM {{ ref('stg_titanic') }}