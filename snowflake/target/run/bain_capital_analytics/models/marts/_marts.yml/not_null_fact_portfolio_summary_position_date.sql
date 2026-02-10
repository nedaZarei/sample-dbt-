
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select position_date
from DBT_DEMO.DEV.fact_portfolio_summary
where position_date is null



  
  
      
    ) dbt_internal_test