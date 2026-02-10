
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select portfolio_id
from "bain_analytics"."public"."int_portfolio_attribution"
where portfolio_id is null



  
  
      
    ) dbt_internal_test