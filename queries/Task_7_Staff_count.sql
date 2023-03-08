select sum(dim_store_details.staff_numbers) as total_staff_numbers, dim_store_details.country_code
from dim_store_details
group by 	dim_store_details.country_code
--ORDER BY    sum(orders_table.product_quantity*dim_products.product_price)  DESC;