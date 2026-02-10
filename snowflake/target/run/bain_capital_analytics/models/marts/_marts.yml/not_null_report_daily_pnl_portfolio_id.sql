
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select portfolio_id
from DBT_DEMO.DEV.report_daily_pnl
where portfolio_id is null



  
  
      
    ) dbt_internal_test