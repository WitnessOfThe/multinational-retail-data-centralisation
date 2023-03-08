select 	count (orders_table.product_quantity) as numbers_of_sales,sum(orders_table.product_quantity) as product_quantity_count,
	case 
		when dim_store_details.store_code = 'WEB-1388012W' then 'Web'
		else 'Offline'
		end as product_location
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by product_location
ORDER BY sum(orders_table.product_quantity) aSC;
	
