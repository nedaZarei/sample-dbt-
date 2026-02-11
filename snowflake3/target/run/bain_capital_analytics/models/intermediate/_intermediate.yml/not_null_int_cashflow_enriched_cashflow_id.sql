
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cashflow_id
from DBT_DEMO.DEV.int_cashflow_enriched
where cashflow_id is null



  
  
      
    ) dbt_internal_test