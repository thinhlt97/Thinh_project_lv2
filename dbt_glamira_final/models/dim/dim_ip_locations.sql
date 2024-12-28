{{ config(materialized='table') }}

WITH REPLACE_TABLE AS (
    SELECT 
        ip
        ,REPLACE(country_short,'-','Not Found') AS country_short
        ,REPLACE(country_name,'-','Not Found') AS country_name
        ,REPLACE(region,'-','Not Found') AS region
        ,REPLACE(city,'-','Not Found') AS city
    FROM `de-project-440715.thinh_glamira_bq.ip_locations`

)

SELECT *
FROM REPLACE_TABLE