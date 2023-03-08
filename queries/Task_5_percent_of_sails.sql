select 	dim_store_details.store_type, 
sum (orders_table.product_quantity*dim_products.product_price) as revenue,
sum(100.0*orders_table.product_quantity*dim_products.product_price)/(sum(sum(orders_table.product_quantity*dim_products.product_price)) over ()) AS percentage_total
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by dim_store_details.store_type
ORDER BY percentage_total DESC;