
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select position_date
from "bain_analytics"."public"."fact_portfolio_summary"
where position_date is null



  
  
      
    ) dbt_internal_test