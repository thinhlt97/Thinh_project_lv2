SELECT 
    dmdl.country_name
    ,SUM(fo.item_price * fo.quantity) as revenue
FROM {{ ref("fact_order") }} fo
INNER JOIN {{ ref("dim_ip_locations") }} dmdl
    ON fo.ip = dmdl.ip
GROUP BY country_name