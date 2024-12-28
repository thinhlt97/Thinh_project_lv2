{{ config(materialized='view') }}

SELECT 
       dmp.product_name
       ,SUM(f.item_price * f.quantity) AS revenue
FROM {{ ref('fact_order') }} f 
LEFT JOIN {{ ref('dim_products') }} dmp
       ON f.product_id = dmp.product_id
WHERE dmp.product_name NOT IN ("link 404")
GROUP BY dmp.product_name
ORDER BY revenue DESC