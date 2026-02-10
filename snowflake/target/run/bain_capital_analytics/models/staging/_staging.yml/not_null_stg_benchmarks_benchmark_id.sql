
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_id
from DBT_DEMO.DEV.stg_benchmarks
where benchmark_id is null



  
  
      
    ) dbt_internal_test