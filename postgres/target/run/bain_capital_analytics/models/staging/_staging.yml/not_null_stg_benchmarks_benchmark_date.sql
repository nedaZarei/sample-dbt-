
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_date
from "bain_analytics"."public"."stg_benchmarks"
where benchmark_date is null



  
  
      
    ) dbt_internal_test