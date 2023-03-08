select round(count(orders_table.date_uuid)) as sales	, 
dim_store_details.store_type, 
dim_store_details.country_code
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
where dim_store_details.country_code = 'DE'
group by 	dim_store_details.store_type,dim_store_details.country_code
--ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;