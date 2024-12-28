-- models/dim_product.sql
{{ config(materialized='table') }}

SELECT DISTINCT
    product_id
    ,product_name
    ,current_url
FROM `de-project-440715.thinh_glamira_bq.product_name`