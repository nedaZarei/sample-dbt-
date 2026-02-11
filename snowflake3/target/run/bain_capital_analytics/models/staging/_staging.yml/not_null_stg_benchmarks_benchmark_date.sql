
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_date
from DBT_DEMO.DEV.stg_benchmarks
where benchmark_date is null



  
  
      
    ) dbt_internal_test