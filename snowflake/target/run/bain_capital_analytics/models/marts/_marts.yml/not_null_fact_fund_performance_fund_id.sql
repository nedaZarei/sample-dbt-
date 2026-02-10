
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fund_id
from DBT_DEMO.DEV.fact_fund_performance
where fund_id is null



  
  
      
    ) dbt_internal_test