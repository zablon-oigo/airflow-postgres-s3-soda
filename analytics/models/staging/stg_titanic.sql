{{ config(materialized='view') }}

SELECT
    CAST("PassengerId" AS INTEGER) AS passenger_id,
    CAST("Survived" AS INTEGER) AS survived,
    CAST("Pclass" AS INTEGER) AS passenger_class,

    TRIM("Name") AS passenger_name,
    LOWER(TRIM("Sex")) AS sex,

    CAST("Age" AS NUMERIC) AS age,
    CAST("SibSp" AS INTEGER) AS siblings_spouses,
    CAST("Parch" AS INTEGER) AS parents_children,

    TRIM("Ticket") AS ticket,
    CAST("Fare" AS NUMERIC) AS fare,

    NULLIF(TRIM("Cabin"), '') AS cabin,
    UPPER(TRIM("Embarked")) AS embarked

FROM {{ source('titanic', 'titanic') }}