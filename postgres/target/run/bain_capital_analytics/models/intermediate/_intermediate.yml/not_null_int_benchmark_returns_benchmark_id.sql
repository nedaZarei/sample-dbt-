
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_id
from "bain_analytics"."public"."int_benchmark_returns"
where benchmark_id is null



  
  
      
    ) dbt_internal_test