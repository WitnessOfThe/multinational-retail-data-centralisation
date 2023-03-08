select dim_date_times.year,dim_date_times.month, round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by 	dim_date_times.month,dim_date_times.year
ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;