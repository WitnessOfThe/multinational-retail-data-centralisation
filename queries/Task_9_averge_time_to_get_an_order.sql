select  dim_date_times.year, 		  
    concat('"hours": ',EXTRACT(hours FROM  avg(dim_date_times.time_diff)),' ',
		   '"minutes": ',EXTRACT(minutes FROM  avg(dim_date_times.time_diff)),' ',		  
		   '"seconds": ',round(EXTRACT(seconds FROM  avg(dim_date_times.time_diff)),2),' '		  
--		   '"milliseconds": ',round(mod(EXTRACT(seconds FROM  avg(dim_date_times.time_diff))*1000,1000)),' '		  
		  ) as actual_time_taken
		 		  
 from dim_date_times
group by dim_date_times.year
order by avg(dim_date_times.time_diff) desc
