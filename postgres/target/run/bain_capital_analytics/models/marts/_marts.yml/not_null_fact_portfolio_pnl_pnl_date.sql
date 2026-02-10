
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pnl_date
from "bain_analytics"."public"."fact_portfolio_pnl"
where pnl_date is null



  
  
      
    ) dbt_internal_test